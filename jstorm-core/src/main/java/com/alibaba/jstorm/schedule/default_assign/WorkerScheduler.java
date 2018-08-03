/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.jstorm.schedule.default_assign;

import backtype.storm.Config;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.client.WorkerAssignment;
import com.alibaba.jstorm.cluster.Common;
import com.alibaba.jstorm.daemon.supervisor.SupervisorInfo;
import com.alibaba.jstorm.schedule.TopologyAssignContext;
import com.alibaba.jstorm.utils.FailedAssignTopologyException;
import com.alibaba.jstorm.utils.NetWorkUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class WorkerScheduler {

    public static Logger LOG = LoggerFactory.getLogger(WorkerScheduler.class);

    private static WorkerScheduler instance;

    private WorkerScheduler() {
    }

    public static WorkerScheduler getInstance() {
        if (instance == null) {
            instance = new WorkerScheduler();
        }
        return instance;
    }

    /**
     * 获取可用的 worker 列表，并为其设置分配对应的 supervisor 信息
     *
     * @param context
     * @param needAssign 需要分配的 task id 集合
     * @param allocWorkerNum 需要分配的 worker 的数目
     * @return
     */
    public List<ResourceWorkerSlot> getAvailableWorkers(
            DefaultTopologyAssignContext context, Set<Integer> needAssign, int allocWorkerNum) {
        int reserveWorkers = context.getReserveWorkerNum(); // 保留的 worker 数目
        int workersNum = this.getAvailableWorkersNum(context); // 可用的 worker 的数目
        if ((workersNum - reserveWorkers) < allocWorkerNum) {
            // 没有足够的 worker 可以分配：可用 worker 数目 - 保留的 worker 数目 < 需要分配的数目
            throw new FailedAssignTopologyException("there's no enough worker. allocWorkerNum="
                    + allocWorkerNum + ", availableWorkerNum=" + workersNum + ",reserveWorkerNum=" + reserveWorkers);
        }
        workersNum = allocWorkerNum;
        List<ResourceWorkerSlot> assignedWorkers = new ArrayList<>(); // 记录分配到的 worker
        // user define assignments, but dont't try to use custom scheduling for TM bolts now.
        // 从 needAssign 中移除已经分配的 task，并记录分配的 worker 到 assignedWorkers 中
        this.getRightWorkers(context, needAssign, assignedWorkers, workersNum,
                // 获取用户自定义分配 worker slot 信息，去除状态为 unstopped 的 worker
                this.getUserDefineWorkers(context, ConfigExtension.getUserDefineAssignment(context.getStormConf())));

        // 如果配置指定要复用旧的分配，则优先从旧的分配中选出合适的 worker
        if (ConfigExtension.isUseOldAssignment(context.getStormConf())) {
            this.getRightWorkers(context, needAssign, assignedWorkers, workersNum, context.getOldWorkers());
        } else if (context.getAssignType() == TopologyAssignContext.ASSIGN_TYPE_REBALANCE && !context.isReassign()) {
            // 如果是 rebalance，且可以使用原来的worker，将原来使用的worker存储起来。
            int cnt = 0;
            for (ResourceWorkerSlot worker : context.getOldWorkers()) {
                if (cnt < workersNum) {
                    ResourceWorkerSlot resFreeWorker = new ResourceWorkerSlot();
                    resFreeWorker.setPort(worker.getPort());
                    resFreeWorker.setHostname(worker.getHostname());
                    resFreeWorker.setNodeId(worker.getNodeId());
                    assignedWorkers.add(resFreeWorker);
                    cnt++;
                } else {
                    break;
                }
            }
        }

        // calculate rest TM bolts
        int workersForSingleTM = 0;
        if (context.getAssignSingleWorkerForTM()) {
            for (Integer taskId : needAssign) {
                String componentName = context.getTaskToComponent().get(taskId);
                if (componentName.equals(Common.TOPOLOGY_MASTER_COMPONENT_ID)) {
                    workersForSingleTM++;
                }
            }
        }

        LOG.info("Get workers from user define and old assignments: " + assignedWorkers);

        int restWorkerNum = workersNum - assignedWorkers.size(); // 还需要分配的 worker 数目
        if (restWorkerNum < 0) {
            throw new FailedAssignTopologyException(
                    "Too many workers are required for user define or old assignments. workersNum="
                            + workersNum + ", assignedWorkersNum=" + assignedWorkers.size());
        }

        // 对于剩下需要的 worker，直接添加 ResourceWorkerSlot 实例对象
        for (int i = 0; i < restWorkerNum; i++) {
            assignedWorkers.add(new ResourceWorkerSlot());
        }
        // 获取那些专门指定运行拓扑的 supervisor 节点
        List<SupervisorInfo> isolationSupervisors = this.getIsolationSupervisors(context);
        if (isolationSupervisors.size() != 0) {
            this.putAllWorkerToSupervisor(assignedWorkers, this.getResAvailSupervisors(isolationSupervisors));
        } else {
            // 为 worker 分配对应的 supervisor
            this.putAllWorkerToSupervisor(assignedWorkers, this.getResAvailSupervisors(context.getCluster()));
        }
        this.setAllWorkerMemAndCpu(context.getStormConf(), assignedWorkers);
        LOG.info("Assigned workers=" + assignedWorkers);
        return assignedWorkers;
    }

    private void setAllWorkerMemAndCpu(Map conf, List<ResourceWorkerSlot> assignedWorkers) {
        long defaultSize = ConfigExtension.getMemSizePerWorker(conf);
        int defaultCpu = ConfigExtension.getCpuSlotPerWorker(conf);
        for (ResourceWorkerSlot worker : assignedWorkers) {
            if (worker.getMemSize() <= 0) {
                worker.setMemSize(defaultSize);
            }
            if (worker.getCpu() <= 0) {
                worker.setCpu(defaultCpu);
            }
        }
    }

    /**
     * 为 worker 分配对应的 supervisor
     *
     * @param assignedWorkers
     * @param supervisors
     */
    private void putAllWorkerToSupervisor(List<ResourceWorkerSlot> assignedWorkers, List<SupervisorInfo> supervisors) {
        for (ResourceWorkerSlot worker : assignedWorkers) {
            if (worker.getHostname() != null) {
                for (SupervisorInfo supervisor : supervisors) {
                    // 如果当前 worker 对应的 hostname 是该 supervisor，且 supervisor 存在空闲的 worker
                    if (NetWorkUtils.equals(supervisor.getHostName(), worker.getHostname())
                            && supervisor.getAvailableWorkerPorts().size() > 0) {
                        /*
                         * 基于当前 supervisor 信息更新对应的 worker 信息：
                         *  1. 保证 worker 对应的端口号是当前 supervisor 空闲的，否则选一个 supervisor 空闲的给 worker
                         *  2. 设置 worker 对应的 node_id 为当前 supervisor 的 ID
                         */
                        this.putWorkerToSupervisor(supervisor, worker);
                        break;
                    }
                }
            }
        }

        // 更新 supervisor 列表，移除没有空闲端口的 supervisor
        supervisors = this.getResAvailSupervisors(supervisors);
        Collections.sort(supervisors, new Comparator<SupervisorInfo>() {

            @Override
            public int compare(SupervisorInfo o1, SupervisorInfo o2) {
                return -NumberUtils.compare(o1.getAvailableWorkerPorts().size(), o2.getAvailableWorkerPorts().size());
            }

        });
        // 防止过载的 supervisor
        this.putWorkerToSupervisor(assignedWorkers, supervisors);
    }

    /**
     * 基于当前 supervisor 信息更新对应的 worker 信息：
     * 1. 保证 worker 对应的端口号是当前 supervisor 空闲的，否则选一个 supervisor 空闲的给 worker
     * 2. 设置 worker 对应的 node_id 为当前 supervisor 的 ID
     *
     * @param supervisor
     * @param worker
     */
    private void putWorkerToSupervisor(SupervisorInfo supervisor, ResourceWorkerSlot worker) {
        // 如果 worker 对应的端口号不在 supervisor 空闲端口号列表中，则选择一个 supervisor 空闲的端口号重新设置 worker 的端口号
        int port = worker.getPort();
        if (!supervisor.getAvailableWorkerPorts().contains(worker.getPort())) {
            port = supervisor.getAvailableWorkerPorts().iterator().next();
        }
        worker.setPort(port);
        supervisor.getAvailableWorkerPorts().remove(port); // 移除已使用的端口号
        worker.setNodeId(supervisor.getSupervisorId()); // 设置当前 worker 的 nodeId
    }

    /**
     * @param assignedWorkers
     * @param supervisors
     */
    private void putWorkerToSupervisor(List<ResourceWorkerSlot> assignedWorkers, List<SupervisorInfo> supervisors) {
        int allUsedPorts = 0; // 记录所有 supervisor 已经使用的端口数目
        for (SupervisorInfo supervisor : supervisors) {
            int supervisorUsedPorts = supervisor.getWorkerPorts().size() - supervisor.getAvailableWorkerPorts().size();
            allUsedPorts = allUsedPorts + supervisorUsedPorts;
        }
        // per supervisor should be allocated ports in theory
        // 计算 supervisor 的平均端口数使用量
        int theoryAveragePorts = (allUsedPorts + assignedWorkers.size()) / supervisors.size() + 1;
        // supervisor which use more than theoryAveragePorts ports will be pushed overLoadSupervisors
        List<SupervisorInfo> overLoadSupervisors = new ArrayList<>(); // 记录过载（端口数使用量大于平均值）的 supervisor
        int key = 0;
        Iterator<ResourceWorkerSlot> iterator = assignedWorkers.iterator();
        while (iterator.hasNext()) {
            if (supervisors.size() == 0) {
                break;
            }
            if (key >= supervisors.size()) {
                key = 0;
            }
            SupervisorInfo supervisor = supervisors.get(key);
            // 计算当前 supervisor 对应的端口使用数量
            int supervisorUsedPorts = supervisor.getWorkerPorts().size() - supervisor.getAvailableWorkerPorts().size();
            if (supervisorUsedPorts < theoryAveragePorts) {
                ResourceWorkerSlot worker = iterator.next();
                if (worker.getNodeId() != null) {
                    // 已经分配了 supervisor
                    continue;
                }
                worker.setHostname(supervisor.getHostName());
                worker.setNodeId(supervisor.getSupervisorId());
                worker.setPort(supervisor.getAvailableWorkerPorts().iterator().next());
                supervisor.getAvailableWorkerPorts().remove(worker.getPort());
                if (supervisor.getAvailableWorkerPorts().size() == 0) {
                    supervisors.remove(supervisor);
                }
                key++;
            } else {
                // 标记当前 supervisor 过载
                overLoadSupervisors.add(supervisor);
                supervisors.remove(supervisor);
            }
        }
        // rest assignedWorkers will be allocate supervisor by deal
        Collections.sort(overLoadSupervisors, new Comparator<SupervisorInfo>() {

            @Override
            public int compare(SupervisorInfo o1, SupervisorInfo o2) {
                return -NumberUtils.compare(o1.getAvailableWorkerPorts().size(), o2.getAvailableWorkerPorts().size());
            }

        });
        key = 0;
        while (iterator.hasNext()) {
            if (overLoadSupervisors.size() == 0) {
                break;
            }
            if (key >= overLoadSupervisors.size()) {
                key = 0;
            }
            ResourceWorkerSlot worker = iterator.next();
            if (worker.getNodeId() != null) {
                continue;
            }
            SupervisorInfo supervisor = overLoadSupervisors.get(key);
            worker.setHostname(supervisor.getHostName());
            worker.setNodeId(supervisor.getSupervisorId());
            worker.setPort(supervisor.getAvailableWorkerPorts().iterator().next());
            supervisor.getAvailableWorkerPorts().remove(worker.getPort());
            if (supervisor.getAvailableWorkerPorts().size() == 0) {
                overLoadSupervisors.remove(supervisor);
            }
            key++;
        }
    }

    /**
     * 从 needAssign 中移除已经分配的 task，并记录分配的 worker 到 assignedWorkers 中
     *
     * @param context 之前准备的拓扑上下文信息
     * @param needAssign 该拓扑需要分配的 task_id 集合
     * @param assignedWorkers 存储那些在这个方法内分配到的 worker 资源，用于返回值
     * @param workersNum 拓扑需要分配的 worker 数目
     * @param workers 用户自定义分配 worker slot 信息，已经去除状态为 unstopped 的 worker
     */
    private void getRightWorkers(DefaultTopologyAssignContext context,
                                 Set<Integer> needAssign, List<ResourceWorkerSlot> assignedWorkers,
                                 int workersNum, Collection<ResourceWorkerSlot> workers) {
        Set<Integer> assigned = new HashSet<>(); // 记录已经分配的 taskId
        List<ResourceWorkerSlot> users = new ArrayList<>();
        if (workers == null) {
            return;
        }
        for (ResourceWorkerSlot worker : workers) {
            boolean right = true;
            Set<Integer> tasks = worker.getTasks();
            if (tasks == null) {
                // worker 没有 task 信息
                continue;
            }
            for (Integer task : tasks) {
                // 不需要分配或已经分配
                if (!needAssign.contains(task) || assigned.contains(task)) {
                    right = false;
                    break;
                }
            }
            if (right) {
                assigned.addAll(tasks);
                users.add(worker);
            }
        }

        if (users.size() + assignedWorkers.size() > workersNum) {
            LOG.warn("There are no enough workers for user define scheduler / keeping old assignment, " +
                    "userDefineWorkers={}, assignedWorkers={}, workerNum={}", users, assignedWorkers, workersNum);
            return;
        }

        assignedWorkers.addAll(users);
        needAssign.removeAll(assigned);
    }

    private int getAvailableWorkersNum(DefaultTopologyAssignContext context) {
        Map<String, SupervisorInfo> supervisors = context.getCluster();
        List<SupervisorInfo> isolationSupervisors = this.getIsolationSupervisors(context);
        int slotNum = 0;

        if (isolationSupervisors.size() != 0) {
            for (SupervisorInfo supervisor : isolationSupervisors) {
                slotNum = slotNum + supervisor.getAvailableWorkerPorts().size();
            }
        } else {
            for (Entry<String, SupervisorInfo> entry : supervisors.entrySet()) {
                slotNum = slotNum + entry.getValue().getAvailableWorkerPorts().size();
            }
        }
        return slotNum;
    }

    /**
     * 获取用户自定义分配 worker slot 信息，去除状态为 unstopped 的 worker
     *
     * @param context
     * @param workers 获取用户定义的分配信息
     * @return
     */
    @SuppressWarnings("unchecked")
    private List<ResourceWorkerSlot> getUserDefineWorkers(
            DefaultTopologyAssignContext context, List<WorkerAssignment> workers) {
        List<ResourceWorkerSlot> ret = new ArrayList<>();
        if (workers == null) {
            // 用户没有自定义分配信息
            return ret;
        }

        Map<String, List<Integer>> componentToTask =
                (HashMap<String, List<Integer>>) ((HashMap<String, List<Integer>>) context.getComponentTasks()).clone();
        if (context.getAssignType() != TopologyAssignContext.ASSIGN_TYPE_NEW) {
            // 如果用户指定的某些 worker 资源属于 unstopped worker，则从 workers 中移除
            this.checkUserDefineWorkers(context, workers, context.getTaskToComponent());
        }

        // 遍历用户定义的 worker，去除那些没有分配 task 的 worker
        // 用户定义的 worker 中已经指定哪些 task 该分配到哪个 worker 中
        for (WorkerAssignment worker : workers) {
            ResourceWorkerSlot workerSlot = new ResourceWorkerSlot(worker, componentToTask);
            if (workerSlot.getTasks().size() != 0) {
                ret.add(workerSlot);
            }
        }
        return ret;
    }

    /**
     * 如果用户指定的某些 worker 资源属于 unstopped worker，则移除
     *
     * @param context
     * @param workers
     * @param taskToComponent
     */
    private void checkUserDefineWorkers(
            DefaultTopologyAssignContext context, List<WorkerAssignment> workers, Map<Integer, String> taskToComponent) {
        Set<ResourceWorkerSlot> unstoppedWorkers = context.getUnstoppedWorkers();
        List<WorkerAssignment> re = new ArrayList<>();
        for (WorkerAssignment worker : workers) {
            for (ResourceWorkerSlot unstopped : unstoppedWorkers) {
                if (unstopped.compareToUserDefineWorker(worker, taskToComponent)) {
                    re.add(worker);
                }
            }
        }
        workers.removeAll(re);

    }

    /**
     * 移除没有空闲端口的 supervisor
     *
     * @param supervisors
     * @return
     */
    private List<SupervisorInfo> getResAvailSupervisors(Map<String, SupervisorInfo> supervisors) {
        List<SupervisorInfo> availableSupervisors = new ArrayList<>();
        for (Entry<String, SupervisorInfo> entry : supervisors.entrySet()) {
            SupervisorInfo supervisor = entry.getValue();
            if (supervisor.getAvailableWorkerPorts().size() > 0) {
                availableSupervisors.add(entry.getValue());
            }
        }
        return availableSupervisors;
    }

    /**
     * 移除没有空闲端口的 supervisor
     *
     * @param supervisors
     * @return
     */
    private List<SupervisorInfo> getResAvailSupervisors(List<SupervisorInfo> supervisors) {
        List<SupervisorInfo> availableSupervisors = new ArrayList<>();
        for (SupervisorInfo supervisor : supervisors) {
            if (supervisor.getAvailableWorkerPorts().size() > 0) {
                availableSupervisors.add(supervisor);
            }
        }
        return availableSupervisors;
    }

    @SuppressWarnings("unchecked")
    private List<SupervisorInfo> getIsolationSupervisors(DefaultTopologyAssignContext context) {
        List<String> isolationHosts = (List<String>) context.getStormConf().get(Config.ISOLATION_SCHEDULER_MACHINES); // isolation.scheduler.machines
        LOG.info("Isolation machines: " + isolationHosts);
        if (isolationHosts == null) {
            return new ArrayList<>();
        }
        List<SupervisorInfo> isolationSupervisors = new ArrayList<>();
        for (Entry<String, SupervisorInfo> entry : context.getCluster().entrySet()) {
            if (this.containTargetHost(isolationHosts, entry.getValue().getHostName())) {
                isolationSupervisors.add(entry.getValue());
            }
        }
        return isolationSupervisors;
    }

    private boolean containTargetHost(Collection<String> hosts, String target) {
        for (String host : hosts) {
            if (NetWorkUtils.equals(host, target)) {
                return true;
            }
        }
        return false;
    }
}
