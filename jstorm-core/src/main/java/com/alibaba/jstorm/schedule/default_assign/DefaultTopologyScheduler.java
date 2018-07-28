/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.jstorm.schedule.default_assign;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.daemon.supervisor.SupervisorInfo;
import com.alibaba.jstorm.schedule.Assignment;
import com.alibaba.jstorm.schedule.ITopologyScheduler;
import com.alibaba.jstorm.schedule.TopologyAssignContext;
import com.alibaba.jstorm.utils.FailedAssignTopologyException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 目前对于 {@link ITopologyScheduler} 的唯一实现
 */
public class DefaultTopologyScheduler implements ITopologyScheduler {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultTopologyScheduler.class);

    @SuppressWarnings("unused")
    private Map nimbusConf;

    @Override
    public void prepare(Map conf) {
        nimbusConf = conf;
    }

    /**
     * TODO: problematic: some dead slots have been freed
     * 如果 task 对应 worker 所属的 supervisor 运行正常，
     * 则将对应的 worker port 加入到 supervisor 的 availableWorkerPorts 中
     *
     * @param context topology assign context
     */
    protected void freeUsed(TopologyAssignContext context) {
        // 所有的 taskId 列表（不包含 unstopped）
        Set<Integer> canFree = new HashSet<>(context.getAllTaskIds());
        canFree.removeAll(context.getUnstoppedTaskIds());

        Map<String, SupervisorInfo> cluster = context.getCluster();
        Assignment oldAssigns = context.getOldAssignment();
        // 如果 task 对应 worker 所属的 supervisor 运行正常，则将对应的 worker port 加入 availableWorkerPorts
        for (Integer task : canFree) {
            // 获取 task 对应的 worker 信息
            ResourceWorkerSlot worker = oldAssigns.getWorkerByTaskId(task);
            if (worker == null) {
                LOG.warn("No ResourceWorkerSlot of task " + task + " is found when freeing resource");
                continue;
            }

            // 获取 worker 对应的 supervisor 信息
            SupervisorInfo supervisorInfo = cluster.get(worker.getNodeId());
            if (supervisorInfo == null) {
                continue;
            }
            supervisorInfo.getAvailableWorkerPorts().add(worker.getPort());
        }
    }

    /**
     * 基于当前的分配类型获取需要分配的 task id 列表
     *
     * @param context
     * @return
     */
    private Set<Integer> getNeedAssignTasks(DefaultTopologyAssignContext context) {
        Set<Integer> needAssign = new HashSet<>();

        int assignType = context.getAssignType();
        if (assignType == TopologyAssignContext.ASSIGN_TYPE_NEW) {
            needAssign.addAll(context.getAllTaskIds());
        } else if (assignType == TopologyAssignContext.ASSIGN_TYPE_REBALANCE) {
            needAssign.addAll(context.getAllTaskIds());
            needAssign.removeAll(context.getUnstoppedTaskIds());
        } else { // ASSIGN_TYPE_MONITOR
            Set<Integer> deadTasks = context.getDeadTaskIds();
            needAssign.addAll(deadTasks);
        }

        return needAssign;
    }

    /**
     * Get the task Map which the task is alive and will be kept only when type is ASSIGN_TYPE_MONITOR, it is valid
     *
     * @param defaultContext default topology context
     * @param needAssigns a set of tasks to be assigned
     * @return a set of assigned slots
     */
    public Set<ResourceWorkerSlot> getKeepAssign(DefaultTopologyAssignContext defaultContext, Set<Integer> needAssigns) {
        Set<Integer> keepAssignIds = new HashSet<>(defaultContext.getAllTaskIds());
        keepAssignIds.removeAll(defaultContext.getUnstoppedTaskIds()); // 移除 unstopped
        keepAssignIds.removeAll(needAssigns); // 移除所有 needAssigns
        Set<ResourceWorkerSlot> keeps = new HashSet<>();
        if (keepAssignIds.isEmpty()) {
            return keeps;
        }

        Assignment oldAssignment = defaultContext.getOldAssignment();
        if (oldAssignment == null) {
            return keeps;
        }

        // 如果 oldWorker 的某个 task 不在 keepAssignIds 列表中，则将该 worker 从 keeps 中移除
        keeps.addAll(defaultContext.getOldWorkers());
        for (ResourceWorkerSlot worker : defaultContext.getOldWorkers()) {
            for (Integer task : worker.getTasks()) {
                if (!keepAssignIds.contains(task)) {
                    keeps.remove(worker);
                    break;
                }
            }
        }
        return keeps;
    }

    /**
     * 获取当前拓扑
     *
     * @param context
     * @return
     * @throws FailedAssignTopologyException
     */
    @Override
    public Set<ResourceWorkerSlot> assignTasks(TopologyAssignContext context) throws FailedAssignTopologyException {
        int assignType = context.getAssignType();
        if (!TopologyAssignContext.isAssignTypeValid(assignType)) {
            // 检查 assignType 的有效性
            throw new FailedAssignTopologyException("Invalid assign type " + assignType);
        }

        DefaultTopologyAssignContext defaultContext = new DefaultTopologyAssignContext(context);
        if (assignType == TopologyAssignContext.ASSIGN_TYPE_REBALANCE) {
            // Mark all current assigned worker as available.
            // Current assignment will be restored in task scheduler.
            /*
             * 如果 task 对应 worker 所属的 supervisor 运行正常，
             * 则将对应的 worker port 加入到 supervisor 的 availableWorkerPorts 中
             */
            this.freeUsed(defaultContext);
        }
        LOG.info("Dead tasks:" + defaultContext.getDeadTaskIds());
        LOG.info("Unstopped tasks:" + defaultContext.getUnstoppedTaskIds());

        // 基于当前的分配类型获取需要分配的 task id 列表
        Set<Integer> needAssignTasks = this.getNeedAssignTasks(defaultContext);
        // 获取那些所有的 task 都在 needAssignTasks 之外的 worker 集合，这些 worker 是不需要 assign 的
        Set<ResourceWorkerSlot> keepAssigns = this.getKeepAssign(defaultContext, needAssignTasks);

        Set<ResourceWorkerSlot> ret = new HashSet<>();
        ret.addAll(keepAssigns);
        ret.addAll(defaultContext.getUnstoppedWorkers());

        // 计算需要分配的 worker 的数目
        int allocWorkerNum = defaultContext.getTotalWorkerNum() - defaultContext.getUnstoppedWorkerNum() - keepAssigns.size();
        LOG.info("allocWorkerNum=" + allocWorkerNum + ", totalWorkerNum=" + defaultContext.getTotalWorkerNum() + ", keepWorkerNum=" + keepAssigns.size());

        if (allocWorkerNum <= 0) {
            LOG.warn("Don't need assign workers, all workers are fine " + defaultContext.toDetailString());
            throw new FailedAssignTopologyException("Don't need assign worker, all workers are fine ");
        }

        List<ResourceWorkerSlot> availableWorkers =
                WorkerScheduler.getInstance().getAvailableWorkers(defaultContext, needAssignTasks, allocWorkerNum);
        TaskScheduler taskScheduler = new TaskScheduler(defaultContext, needAssignTasks, availableWorkers);
        Set<ResourceWorkerSlot> assignment = new HashSet<>(taskScheduler.assign());

        //setting worker's memory for TM
        int topologyMasterId = defaultContext.getTopologyMasterTaskId();
        // ${topology.master.worker.memory.size}
        Long tmWorkerMem = ConfigExtension.getMemSizePerTopologyMasterWorker(defaultContext.getStormConf());
        if (tmWorkerMem != null) {
            for (ResourceWorkerSlot resourceWorkerSlot : assignment) {
                if (resourceWorkerSlot.getTasks().contains(topologyMasterId)) {
                    resourceWorkerSlot.setMemSize(tmWorkerMem);
                }
            }
        }
        ret.addAll(assignment);

        LOG.info("Keep Alive slots:" + keepAssigns);
        LOG.info("Unstopped slots:" + defaultContext.getUnstoppedWorkers());
        LOG.info("New assign slots:" + assignment);

        return ret;
    }

}
