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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class TaskAssignContext {
    private final Map<Integer, String> taskToComponent;

    private final Map<String, List<ResourceWorkerSlot>> supervisorToWorker;

    private final Map<String, Set<String>> relationship;

    // Map<worker, Map<component_name, assigned task num in this worker>
    private final Map<ResourceWorkerSlot, Map<String, Integer>> workerToComponentNum = new HashMap<>();

    /** 记录 worker 及其分配的 task 数目 */
    // Map<available worker, assigned task num in this worker>
    private final Map<ResourceWorkerSlot, Integer> workerToTaskNum = new HashMap<>();

    /** (host:port) -> worker */
    private final Map<String, ResourceWorkerSlot> hostPortToWorkerMap = new HashMap<>();

    /**
     * @param supervisorToWorker 建立 supervisorId 到隶属于该 supervisor 的 worker 列表的映射关系
     * @param relationship
     * @param taskToComponent
     */
    public TaskAssignContext(Map<String, List<ResourceWorkerSlot>> supervisorToWorker,
                             Map<String, Set<String>> relationship, Map<Integer, String> taskToComponent) {
        this.supervisorToWorker = supervisorToWorker;
        this.relationship = relationship;
        this.taskToComponent = taskToComponent;

        for (Entry<String, List<ResourceWorkerSlot>> entry : supervisorToWorker.entrySet()) {
            for (ResourceWorkerSlot worker : entry.getValue()) {
                workerToTaskNum.put(worker, (worker.getTasks() != null ? worker.getTasks().size() : 0));
                hostPortToWorkerMap.put(worker.getHostPort(), worker);

                if (worker.getTasks() != null) {
                    // 计算 work 范围内组件对应的 task 数目
                    Map<String, Integer> componentToNum = new HashMap<>();
                    for (Integer taskId : worker.getTasks()) {
                        String componentId = taskToComponent.get(taskId);
                        int num = componentToNum.get(componentId) == null ? 0 : componentToNum.get(componentId);
                        componentToNum.put(componentId, ++num);
                    }
                    workerToComponentNum.put(worker, componentToNum);
                }
            }
        }
    }

    public Map<ResourceWorkerSlot, Integer> getWorkerToTaskNum() {
        return workerToTaskNum;
    }

    public Map<String, List<ResourceWorkerSlot>> getSupervisorToWorker() {
        return supervisorToWorker;
    }

    public Map<ResourceWorkerSlot, Map<String, Integer>> getWorkerToComponentNum() {
        return workerToComponentNum;
    }

    public Map<String, Set<String>> getRelationship() {
        return relationship;
    }

    /**
     * 获取指定组件在指定 supervisor 节点上分配的 task 数目
     *
     * @param supervisor
     * @param name 组件名称
     * @return
     */
    public int getComponentNumOnSupervisor(String supervisor, String name) {
        // 获取当前 supervisor 上的组件列表
        List<ResourceWorkerSlot> workers = supervisorToWorker.get(supervisor);
        if (workers == null) {
            return 0;
        }
        int result = 0;
        for (ResourceWorkerSlot worker : workers) {
            // 获取指定组件在指定 worker 上分配的 task 的数目
            result = result + this.getComponentNumOnWorker(worker, name);
        }
        return result;
    }

    /**
     * 获取指定组件在指定 worker 上分配的 task 的数目
     *
     * @param worker
     * @param name 组件名称
     * @return
     */
    public int getComponentNumOnWorker(ResourceWorkerSlot worker, String name) {
        int result = 0;
        // 获取组件分配给当前 worker 的 task 数目
        // Map<worker, Map<component_name, assigned task num in this worker>
        Map<String, Integer> componentNum = workerToComponentNum.get(worker); // <组件名称， task 数目>
        if (componentNum != null && componentNum.get(name) != null) {
            result = componentNum.get(name);
        }
        return result;
    }

    public int getTaskNumOnSupervisor(String supervisor) {
        List<ResourceWorkerSlot> workers = supervisorToWorker.get(supervisor);
        if (workers == null) {
            return 0;
        }
        int result = 0;
        for (ResourceWorkerSlot worker : workers) {
            result = result + this.getTaskNumOnWorker(worker);
        }
        return result;
    }

    public int getTaskNumOnWorker(ResourceWorkerSlot worker) {
        return worker.getTasks() == null ? 0 : worker.getTasks().size();
    }

    public int getInputComponentNumOnSupervisor(String supervisor, String name) {
        int result = 0;
        List<ResourceWorkerSlot> workers = supervisorToWorker.get(supervisor);
        if (workers == null) {
            return 0;
        }
        for (ResourceWorkerSlot worker : workers)
            result = result + this.getInputComponentNumOnWorker(worker, name);
        return result;
    }

    public int getInputComponentNumOnWorker(ResourceWorkerSlot worker, String name) {
        int result = 0;
        for (String component : relationship.get(name))
            result = result + this.getComponentNumOnWorker(worker, component);
        return result;
    }

    public Map<String, ResourceWorkerSlot> getHostPortToWorkerMap() {
        return hostPortToWorkerMap;
    }

    public ResourceWorkerSlot getWorker(ResourceWorkerSlot worker) {
        return hostPortToWorkerMap.get(worker.getHostPort());
    }
}
