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
import com.alibaba.jstorm.cluster.Common;
import com.alibaba.jstorm.schedule.TopologyAssignContext;
import com.alibaba.jstorm.schedule.default_assign.Selector.ComponentNumSelector;
import com.alibaba.jstorm.schedule.default_assign.Selector.InputComponentNumSelector;
import com.alibaba.jstorm.schedule.default_assign.Selector.Selector;
import com.alibaba.jstorm.schedule.default_assign.Selector.TotalTaskNumSelector;
import com.alibaba.jstorm.utils.FailedAssignTopologyException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class TaskScheduler {

    public static Logger LOG = LoggerFactory.getLogger(TaskScheduler.class);

    private final TaskAssignContext taskContext;
    private List<ResourceWorkerSlot> assignments = new ArrayList<>();
    private int workerNum;

    /**
     * For balance purpose, default scheduler is trying to assign the same number of tasks to a worker.
     * e.g. There are 4 tasks and 3 available workers.
     * Each worker will be assigned one task first. And then one worker is chosen for the last one.
     */
    private int avgTaskNum;
    private int leftTaskNum;

    private Set<Integer> tasks;

    private DefaultTopologyAssignContext context;
    private Selector componentSelector;
    private Selector inputComponentSelector;
    private Selector totalTaskNumSelector;

    /**
     * @param context topology 任务分配上下文
     * @param tasks 需要分配的 task
     * @param workers 可用的 worker
     */
    public TaskScheduler(DefaultTopologyAssignContext context, Set<Integer> tasks, List<ResourceWorkerSlot> workers) {
        this.tasks = tasks;
        LOG.info("Tasks " + tasks + " is going to be assigned in workers " + workers);
        this.context = context;
        this.taskContext = new TaskAssignContext(
                this.buildSupervisorToWorker(workers), // 建立 supervisorId 到隶属于该 supervisor 的 worker 列表的映射关系
                Common.buildSpoutOutputAndBoltInputMap(context), context.getTaskToComponent());
        this.componentSelector = new ComponentNumSelector(taskContext);
        this.inputComponentSelector = new InputComponentNumSelector(taskContext);
        this.totalTaskNumSelector = new TotalTaskNumSelector(taskContext);
        if (tasks.size() == 0) {
            return;
        }
        if (context.getAssignType() != TopologyAssignContext.ASSIGN_TYPE_REBALANCE || context.isReassign()) {
            // warning ! it doesn't consider HA TM now!!
            if (context.getAssignSingleWorkerForTM() && tasks.contains(context.getTopologyMasterTaskId())) {
                // 为 TM 分配 worker
                this.assignForTopologyMaster();
            }
        }

        int taskNum = tasks.size();
        // 获取 worker 及其分配的 task 数目
        Map<ResourceWorkerSlot, Integer> workerSlotIntegerMap = taskContext.getWorkerToTaskNum();
        Set<ResourceWorkerSlot> preAssignWorkers = new HashSet<>();
        for (Entry<ResourceWorkerSlot, Integer> worker : workerSlotIntegerMap.entrySet()) {
            if (worker.getValue() > 0) {
                taskNum += worker.getValue();
                preAssignWorkers.add(worker.getKey());
            }
        }
        this.setTaskNum(taskNum, workerNum);

        // Check the worker assignment status of pre-assigned workers, e.g user defined or old assignment workers.
        // Remove the workers which have been assigned with enough workers.
        for (ResourceWorkerSlot worker : preAssignWorkers) {
            if (taskContext.getWorkerToTaskNum().keySet().contains(worker)) {

                Set<ResourceWorkerSlot> doneWorkers = this.removeWorkerFromSrcPool(taskContext.getWorkerToTaskNum().get(worker), worker);
                if (doneWorkers != null) {
                    for (ResourceWorkerSlot doneWorker : doneWorkers) {
                        taskNum -= doneWorker.getTasks().size();
                        workerNum--;
                    }
                }
            }
        }
        this.setTaskNum(taskNum, workerNum);

        // For Scale-out case, the old assignment should be kept.
        if (context.getAssignType() == TopologyAssignContext.ASSIGN_TYPE_REBALANCE && !context.isReassign()) {
            this.keepAssignment(taskNum, context.getOldAssignment().getWorkers());
        }
    }

    private void keepAssignment(int taskNum, Set<ResourceWorkerSlot> keepAssignments) {
        Set<Integer> keepTasks = new HashSet<>();
        ResourceWorkerSlot tmWorker = null;
        for (ResourceWorkerSlot worker : keepAssignments) {
            if (worker.getTasks().contains(context.getTopologyMasterTaskId())) {
                tmWorker = worker;
            }
            for (Integer taskId : worker.getTasks()) {
                if (tasks.contains(taskId)) {
                    ResourceWorkerSlot contextWorker = taskContext.getWorker(worker);
                    if (contextWorker != null) {
                        if (tmWorker != null && tmWorker.getTasks().contains(taskId) && context.getAssignSingleWorkerForTM()) {
                            if (context.getTopologyMasterTaskId() == taskId) {
                                this.updateAssignedTasksOfWorker(taskId, contextWorker);
                                taskContext.getWorkerToTaskNum().remove(contextWorker);
                                contextWorker.getTasks().clear();
                                contextWorker.getTasks().add(taskId);
                                assignments.add(contextWorker);
                                tasks.remove(taskId);
                                taskNum--;
                                workerNum--;
                                LOG.info("assign for TopologyMaster: " + contextWorker);
                            }
                        } else {
                            String componentName = context.getTaskToComponent().get(taskId);
                            this.updateAssignedTasksOfWorker(taskId, contextWorker);
                            this.updateComponentsNumOfWorker(componentName, contextWorker);
                            keepTasks.add(taskId);
                        }
                    }
                }
            }
        }
        if (tmWorker != null) {
            this.setTaskNum(taskNum, workerNum);
            keepAssignments.remove(tmWorker);
        }

        // Try to find the workers which have been assigned too much tasks
        // If found, remove the workers from worker resource pool and update
        // the avgNum and leftNum
        int doneAssignedTaskNum = 0;
        while (true) {
            boolean found = false;
            Set<ResourceWorkerSlot> doneAssignedWorkers = new HashSet<>();
            for (ResourceWorkerSlot worker : keepAssignments) {
                ResourceWorkerSlot contextWorker = taskContext.getWorker(worker);
                if (contextWorker != null && this.isTaskFullForWorker(contextWorker)) {
                    found = true;
                    workerNum--;
                    taskContext.getWorkerToTaskNum().remove(contextWorker);
                    assignments.add(contextWorker);

                    doneAssignedWorkers.add(worker);
                    doneAssignedTaskNum += contextWorker.getTasks().size();
                }
            }

            if (found) {
                taskNum -= doneAssignedTaskNum;
                this.setTaskNum(taskNum, workerNum);
                keepAssignments.removeAll(doneAssignedWorkers);
            } else {
                break;
            }
        }
        tasks.removeAll(keepTasks);
        LOG.info("keep following assignment, " + assignments);
    }

    private boolean isTaskFullForWorker(ResourceWorkerSlot worker) {
        boolean ret = false;
        Set<Integer> tasks = worker.getTasks();

        if (tasks != null) {
            if ((leftTaskNum <= 0 && tasks.size() >= avgTaskNum) ||
                    (leftTaskNum > 0 && tasks.size() >= (avgTaskNum + 1))) {
                ret = true;
            }
        }
        return ret;
    }

    /**
     * 获取分配了 task 的 worker
     *
     * @return
     */
    private Set<ResourceWorkerSlot> getRestAssignedWorkers() {
        Set<ResourceWorkerSlot> ret = new HashSet<>();
        for (ResourceWorkerSlot worker : taskContext.getWorkerToTaskNum().keySet()) {
            if (worker.getTasks() != null && worker.getTasks().size() > 0) {
                // 当前 worker 有分配 task
                ret.add(worker);
            }
        }
        return ret;
    }

    /**
     * 为当前 topology 范围内的 task 分配 worker
     *
     * @return
     */
    public List<ResourceWorkerSlot> assign() {
        if (tasks.size() == 0) { // 没有需要再分配的任务
            assignments.addAll(this.getRestAssignedWorkers());
            return assignments;
        }

        // 1. 处理设置了 task.on.differ.node=true 的组件，为其在不同 supervisor 节点上分配 worker
        Set<Integer> assignedTasks = this.assignForDifferNodeTask();

        // 2. 为剩余 task 分配 worker，不包含系统组件
        tasks.removeAll(assignedTasks);
        Map<Integer, String> systemTasks = new HashMap<>();
        for (Integer task : tasks) {
            String name = context.getTaskToComponent().get(task);
            if (Common.isSystemComponent(name)) {
                systemTasks.put(task, name);
                continue;
            }
            this.assignForTask(name, task);
        }

        // 3. 为系统组件 task 分配 worker, e.g. acker, topology master...
        for (Entry<Integer, String> entry : systemTasks.entrySet()) {
            this.assignForTask(entry.getValue(), entry.getKey());
        }

        // 记录所有分配了 task 的 worker 集合
        assignments.addAll(this.getRestAssignedWorkers());
        return assignments;
    }

    /**
     * 为 TM 分配 worker
     */
    private void assignForTopologyMaster() {
        int taskId = context.getTopologyMasterTaskId();

        // Try to find a worker which is in a supervisor with most workers,
        // to avoid the balance problem when the assignment for other workers.
        // 从 supervisor 列表中寻找一个 worker 分配给 TM，该 worker 隶属的 supervisor 具备最多的 worker 数目
        ResourceWorkerSlot workerAssigned = null;
        int workerNumOfSuperv = 0;
        for (ResourceWorkerSlot workerSlot : taskContext.getWorkerToTaskNum().keySet()) {
            List<ResourceWorkerSlot> workers = taskContext.getSupervisorToWorker().get(workerSlot.getNodeId());
            if (workers != null && workers.size() > workerNumOfSuperv) {
                for (ResourceWorkerSlot worker : workers) {
                    Set<Integer> tasks = worker.getTasks();
                    if (tasks == null || tasks.size() == 0) {
                        // TM 独占一个 worker
                        workerAssigned = worker;
                        workerNumOfSuperv = workers.size();
                        break;
                    }
                }
            }
        }
        if (workerAssigned == null) {
            throw new FailedAssignTopologyException("there's no enough workers for the assignment of topology master");
        }
        // 将 taskId 加入到 worker 的 task 列表，并更新 worker 持有的 task 数目
        this.updateAssignedTasksOfWorker(taskId, workerAssigned);
        taskContext.getWorkerToTaskNum().remove(workerAssigned);
        assignments.add(workerAssigned);
        tasks.remove(taskId);
        workerNum--;
        LOG.info("assignForTopologyMaster, assignments=" + assignments);
    }

    private void assignForTask(String name, Integer task) {
        // 基于多重选择器为当前 task 选择最优 worker
        ResourceWorkerSlot worker = this.chooseWorker(name, new ArrayList<>(taskContext.getWorkerToTaskNum().keySet()));
        /*
         * 1. 将 task 加入到 worker 的 task 列表，并更新 worker 持有的 task 数目
         * 2. 检查当前 worker 分配的 task 数目，如果足够多则将其从资源池移除，不再分配新的 task
         * 3. 更新指定组件分配在指定 worker 上的 task 数目
         */
        this.pushTaskToWorker(task, name, worker);
    }

    /**
     * 处理设置了 task.on.differ.node=true 的组件，为其在不同 supervisor 节点上分配 worker
     *
     * @return
     */
    private Set<Integer> assignForDifferNodeTask() {
        Set<Integer> ret = new HashSet<>();
        for (Integer task : tasks) {
            // 获取当前 task 对应组件的配置信息
            Map conf = Common.getComponentMap(context, task);
            // 配置了 task.on.differ.node=true 的组件，其 task 必须允许在不同的节点上面
            if (ConfigExtension.isTaskOnDifferentNode(conf)) {
                ret.add(task);
            }
        }
        for (Integer task : ret) {
            String name = context.getTaskToComponent().get(task);
            // 基于多重选择器从不同 supervisor 节点上选择最优 worker
            ResourceWorkerSlot worker = this.chooseWorker(name, this.getDifferNodeTaskWorkers(name));
            LOG.info("Due to task.on.differ.node, push task-{} to {}:{}", task, worker.getHostname(), worker.getPort());
            /*
             * 1. 将 task 加入到 worker 的 task 列表，并更新 worker 持有的 task 数目
             * 2. 检查当前 worker 分配的 task 数目，如果足够多则将其从资源池移除，不再分配新的 task
             * 3. 更新指定组件分配在指定 worker 上的 task 数目
             */
            this.pushTaskToWorker(task, name, worker);
        }
        return ret;
    }

    /**
     * 建立 supervisorId 到隶属于该 supervisor 的 worker 列表的映射关系
     *
     * @param workers
     * @return
     */
    private Map<String, List<ResourceWorkerSlot>> buildSupervisorToWorker(List<ResourceWorkerSlot> workers) {
        Map<String, List<ResourceWorkerSlot>> supervisorToWorker = new HashMap<>();
        for (ResourceWorkerSlot worker : workers) {
            List<ResourceWorkerSlot> supervisor = supervisorToWorker.get(worker.getNodeId());
            if (supervisor == null) {
                supervisor = new ArrayList<>();
                supervisorToWorker.put(worker.getNodeId(), supervisor);
            }
            supervisor.add(worker);
        }
        this.workerNum = workers.size();
        return supervisorToWorker;
    }

    /**
     * 基于多重选择器选择最优 worker
     *
     * @param name
     * @param workers
     * @return
     */
    private ResourceWorkerSlot chooseWorker(String name, List<ResourceWorkerSlot> workers) {
        List<ResourceWorkerSlot> result = componentSelector.select(workers, name);
        result = totalTaskNumSelector.select(result, name);
        if (Common.isSystemComponent(name)) {
            return result.iterator().next();
        }
        result = inputComponentSelector.select(result, name);
        return result.iterator().next();
    }

    /**
     * @param task
     * @param name 组件名称
     * @param worker task 隶属的 worker
     */
    private void pushTaskToWorker(Integer task, String name, ResourceWorkerSlot worker) {
        LOG.debug("Push task-" + task + " to worker-" + worker.getPort());
        // 将 task 加入到 worker 的 task 列表，并更新 worker 持有的 task 数目
        int taskNum = this.updateAssignedTasksOfWorker(task, worker);
        // 检查当前 worker 分配的 task 数目，如果足够多则将其从资源池移除，不再分配新的 task
        this.removeWorkerFromSrcPool(taskNum, worker);
        // 更新指定组件分配在指定 worker 上的 task 数目
        this.updateComponentsNumOfWorker(name, worker);
    }

    /**
     * 将 task 加入到 worker 的 task 列表，并更新 worker 持有的 task 数目
     *
     * @param task
     * @param worker
     * @return
     */
    private int updateAssignedTasksOfWorker(Integer task, ResourceWorkerSlot worker) {
        int ret;
        Set<Integer> tasks = worker.getTasks();
        if (tasks == null) {
            tasks = new HashSet<>();
            worker.setTasks(tasks);
        }
        tasks.add(task);

        ret = taskContext.getWorkerToTaskNum().get(worker);
        taskContext.getWorkerToTaskNum().put(worker, ++ret);
        return ret;
    }

    /**
     * Remove the worker from source worker pool, if the worker is assigned with enough tasks
     *
     * @param taskNum
     * @param worker
     * @return
     */
    private Set<ResourceWorkerSlot> removeWorkerFromSrcPool(int taskNum, ResourceWorkerSlot worker) {
        Set<ResourceWorkerSlot> ret = new HashSet<>();

        if (leftTaskNum <= 0) {
            if (taskNum >= avgTaskNum) {
                taskContext.getWorkerToTaskNum().remove(worker);
                assignments.add(worker);
                ret.add(worker);
            }
        } else {
            if (taskNum > avgTaskNum) {
                taskContext.getWorkerToTaskNum().remove(worker);
                leftTaskNum = leftTaskNum - (taskNum - avgTaskNum);
                assignments.add(worker);
                ret.add(worker);
            }
            if (leftTaskNum <= 0) {
                List<ResourceWorkerSlot> needDelete = new ArrayList<>();
                for (Entry<ResourceWorkerSlot, Integer> entry : taskContext.getWorkerToTaskNum().entrySet()) {
                    if (avgTaskNum != 0 && entry.getValue() == avgTaskNum) {
                        needDelete.add(entry.getKey());
                    }
                }
                for (ResourceWorkerSlot workerToDelete : needDelete) {
                    taskContext.getWorkerToTaskNum().remove(workerToDelete);
                    assignments.add(workerToDelete);
                    ret.add(workerToDelete);
                }
            }
        }

        return ret;
    }

    /**
     * 更新指定组件分配在指定 worker 上的 task 数目
     *
     * @param name
     * @param worker
     */
    private void updateComponentsNumOfWorker(String name, ResourceWorkerSlot worker) {
        // Map<worker, Map<component_name, assigned task num in this worker>
        Map<String, Integer> components = taskContext.getWorkerToComponentNum().get(worker);
        if (components == null) {
            components = new HashMap<>();
            taskContext.getWorkerToComponentNum().put(worker, components);
        }
        Integer componentNum = components.get(name);
        if (componentNum == null) {
            componentNum = 0;
        }
        components.put(name, ++componentNum);
    }

    private void setTaskNum(int taskNum, int workerNum) {
        if (taskNum >= 0 && workerNum > 0) {
            this.avgTaskNum = taskNum / workerNum;
            this.leftTaskNum = taskNum % workerNum;
            LOG.debug("avgTaskNum=" + avgTaskNum + ", leftTaskNum=" + leftTaskNum);
        } else {
            LOG.warn("Illegal parameters, taskNum=" + taskNum + ", workerNum=" + workerNum);
        }
    }

    /**
     * 获取指定组件可以分配的 worker 列表，保证这些 worker 运行在不同的 supervisor 节点上
     *
     * @param name 组件名称
     * @return
     */
    private List<ResourceWorkerSlot> getDifferNodeTaskWorkers(String name) {
        // 获取所有的 worker 候选列表
        List<ResourceWorkerSlot> workers = new ArrayList<>(taskContext.getWorkerToTaskNum().keySet());

        // <supervisor_id, List<worker>>
        for (Entry<String, List<ResourceWorkerSlot>> entry : taskContext.getSupervisorToWorker().entrySet()) {
            // 如果当前组件在当前 supervisor 已经分配了 task，则从候选列表中移除当前 supervisor 上的所有可用的 worker
            if (taskContext.getComponentNumOnSupervisor(entry.getKey(), name) != 0) {
                workers.removeAll(entry.getValue());
            }
        }
        if (workers.size() == 0) {
            throw new FailedAssignTopologyException(
                    "there's no enough supervisor for making component: " + name + " 's tasks on different node");
        }
        return workers;
    }
}
