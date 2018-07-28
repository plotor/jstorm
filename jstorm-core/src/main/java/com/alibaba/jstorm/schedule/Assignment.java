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

package com.alibaba.jstorm.schedule;

import com.alibaba.jstorm.schedule.default_assign.ResourceWorkerSlot;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Assignment of one topology, stored in /ZK-DIR/assignments/{topologyId}
 * nodeHost {supervisorId: hostname} -- assigned supervisor
 * Map taskStartTimeSecs: {taskId, taskStartSeconds}
 * masterCodeDir: topology source code's dir in Nimbus
 * taskToResource: {taskId, ResourceAssignment}
 *
 * 定义了当前 topology 的任务分配情况
 *
 * @author Lixin/Longda
 */
public class Assignment implements Serializable {

    /**
     * 任务分配类型
     */
    public enum AssignmentType {
        // 新分配的任务
        Assign,
        // 更新的任务
        UpdateTopology,
        // 扩展？
        ScaleTopology
    }

    private static final long serialVersionUID = 6087667851333314069L;

    /**
     * nimbus 在本地保存该 topology 信息的路径，
     * 主要包含三个文件：stormjar.jar、stormcode.ser、stormconf.ser
     *
     * eg. /home/work/data/jstorm/nimbus/stormdist/zhenchao-demo-topology-1-1530348138
     */
    private final String masterCodeDir;

    /**
     * nodeHost which will be stored in zk
     *
     * 定义了当前 topology 被分配到的 <supervisor_id, hostname>:
     *
     * "nodeHost": {
     * "980bbcfd-5438-4e25-aee9-bf411304a446": "10.38.164.192"
     * }
     */
    private final Map<String, String> nodeHost;

    /**
     * 定义当前 topology 对应的 task 启动时间:
     *
     * "taskStartTimeSecs": {
     * "1": 1530348139,
     * "2": 1530348139,
     * "3": 1530348139,
     * "4": 1530348139,
     * "5": 1530348139,
     * "6": 1530348139,
     * "7": 1530348139,
     * "8": 1530348139,
     * "9": 1530348139,
     * "10": 1530348139,
     * "11": 1530348139,
     * "12": 1530348139,
     * "13": 1530348139,
     * "14": 1530348139,
     * "15": 1530348139,
     * "16": 1530348139,
     * "17": 1530348139,
     * "18": 1530348139
     * }
     */
    private final Map<Integer, Integer> taskStartTimeSecs;

    /**
     * worker 基本信息，以及被分配的 task 列表：
     *
     * "workers": [
     * {
     * "hostname": "10.38.164.192",
     * "memSize": 2147483648,
     * "cpu": 1,
     * "tasks": [
     * 12,
     * 2,
     * 10
     * ],
     * "nodeId": "980bbcfd-5438-4e25-aee9-bf411304a446",
     * "port": 6903
     * },
     * {
     * "hostname": "10.38.164.192",
     * "memSize": 2147483648,
     * "cpu": 1,
     * "tasks": [
     * 13,
     * 3,
     * 11
     * ],
     * "nodeId": "980bbcfd-5438-4e25-aee9-bf411304a446",
     * "port": 6904
     * },
     * {
     * "hostname": "10.38.164.192",
     * "memSize": 2147483648,
     * "cpu": 1,
     * "tasks": [
     * 4,
     * 17
     * ],
     * "nodeId": "980bbcfd-5438-4e25-aee9-bf411304a446",
     * "port": 6905
     * },
     * {
     * "hostname": "10.38.164.192",
     * "memSize": 2147483648,
     * "cpu": 1,
     * "tasks": [
     * 5,
     * 18
     * ],
     * "nodeId": "980bbcfd-5438-4e25-aee9-bf411304a446",
     * "port": 6906
     * },
     * {
     * "hostname": "10.38.164.192",
     * "memSize": 2147483648,
     * "cpu": 1,
     * "tasks": [
     * 1,
     * 6
     * ],
     * "nodeId": "980bbcfd-5438-4e25-aee9-bf411304a446",
     * "port": 6900
     * },
     * {
     * "hostname": "10.38.164.192",
     * "memSize": 2147483648,
     * "cpu": 1,
     * "tasks": [
     * 14,
     * 7
     * ],
     * "nodeId": "980bbcfd-5438-4e25-aee9-bf411304a446",
     * "port": 6901
     * },
     * {
     * "hostname": "10.38.164.192",
     * "memSize": 2147483648,
     * "cpu": 1,
     * "tasks": [
     * 8,
     * 15
     * ],
     * "nodeId": "980bbcfd-5438-4e25-aee9-bf411304a446",
     * "port": 6902
     * },
     * {
     * "hostname": "10.38.164.192",
     * "memSize": 2147483648,
     * "cpu": 1,
     * "tasks": [
     * 16,
     * 9
     * ],
     * "nodeId": "980bbcfd-5438-4e25-aee9-bf411304a446",
     * "port": 6912
     * }
     * ]
     */
    private final Set<ResourceWorkerSlot> workers;

    private long timeStamp;

    private AssignmentType type;

    public Assignment() {
        this.masterCodeDir = null;
        this.nodeHost = new HashMap<>();
        this.taskStartTimeSecs = new HashMap<>();
        this.workers = new HashSet<>();
        this.timeStamp = System.currentTimeMillis();
        this.type = AssignmentType.Assign;
    }

    public Assignment(String masterCodeDir, Set<ResourceWorkerSlot> workers,
                      Map<String, String> nodeHost, Map<Integer, Integer> taskStartTimeSecs) {
        this.workers = workers;
        this.nodeHost = nodeHost;
        this.taskStartTimeSecs = taskStartTimeSecs;
        this.masterCodeDir = masterCodeDir;
        this.timeStamp = System.currentTimeMillis();
        this.type = AssignmentType.Assign;
    }

    public void setAssignmentType(AssignmentType type) {
        this.type = type;
    }

    public AssignmentType getAssignmentType() {
        return type;
    }

    public Map<String, String> getNodeHost() {
        return nodeHost;
    }

    public Map<Integer, Integer> getTaskStartTimeSecs() {
        return taskStartTimeSecs;
    }

    public String getMasterCodeDir() {
        return masterCodeDir;
    }

    public Set<ResourceWorkerSlot> getWorkers() {
        return workers;
    }

    /**
     * get workers for every supervisorId (node)
     *
     * @param supervisorId supervisor
     * @return Map[Integer, WorkerSlot]
     */
    public Map<Integer, ResourceWorkerSlot> getTaskToNodePortbyNode(String supervisorId) {
        Map<Integer, ResourceWorkerSlot> result = new HashMap<>();
        for (ResourceWorkerSlot worker : workers) {
            if (worker.getNodeId().equals(supervisorId)) {
                result.put(worker.getPort(), worker);
            }
        }
        return result;
    }

    public Set<Integer> getCurrentSuperviosrTasks(String supervisorId) {
        Set<Integer> Tasks = new HashSet<>();
        for (ResourceWorkerSlot worker : workers) {
            if (worker.getNodeId().equals(supervisorId)) {
                Tasks.addAll(worker.getTasks());
            }
        }
        return Tasks;
    }

    public Set<Integer> getCurrentSuperviosrWorkers(String supervisorId) {
        Set<Integer> workerSet = new HashSet<>();
        for (ResourceWorkerSlot worker : workers) {
            if (worker.getNodeId().equals(supervisorId)) {
                workerSet.add(worker.getPort());
            }
        }
        return workerSet;
    }

    public Set<Integer> getCurrentWorkerTasks(String supervisorId, int port) {
        for (ResourceWorkerSlot worker : workers) {
            if (worker.getNodeId().equals(supervisorId) && worker.getPort() == port) {
                return worker.getTasks();
            }
        }
        return new HashSet<>();
    }

    /**
     * 获取 task 对应的 worker 信息
     *
     * @param taskId
     * @return
     */
    public ResourceWorkerSlot getWorkerByTaskId(Integer taskId) {
        for (ResourceWorkerSlot worker : workers) {
            if (worker.getTasks().contains(taskId)) {
                return worker;
            }
        }
        return null;
    }

    public long getTimeStamp() {
        return this.timeStamp;
    }

    public boolean isTopologyChange(long oldTimeStamp) {
        boolean isChange = false;
        if (timeStamp > oldTimeStamp && (type.equals(AssignmentType.UpdateTopology) || type.equals(AssignmentType.ScaleTopology))) {
            isChange = true;
        }
        return isChange;
    }

    public void updateTimeStamp() {
        timeStamp = System.currentTimeMillis();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((masterCodeDir == null) ? 0 : masterCodeDir.hashCode());
        result = prime * result + ((nodeHost == null) ? 0 : nodeHost.hashCode());
        result = prime * result + ((taskStartTimeSecs == null) ? 0 : taskStartTimeSecs.hashCode());
        result = prime * result + ((workers == null) ? 0 : workers.hashCode());
        result = prime * result + (int) (timeStamp & 0xFFFFFFFF);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (this.getClass() != obj.getClass()) {
            return false;
        }
        Assignment other = (Assignment) obj;
        if (masterCodeDir == null) {
            if (other.masterCodeDir != null) {
                return false;
            }
        } else if (!masterCodeDir.equals(other.masterCodeDir)) {
            return false;
        }
        if (nodeHost == null) {
            if (other.nodeHost != null) {
                return false;
            }
        } else if (!nodeHost.equals(other.nodeHost)) {
            return false;
        }
        if (taskStartTimeSecs == null) {
            if (other.taskStartTimeSecs != null) {
                return false;
            }
        } else if (!taskStartTimeSecs.equals(other.taskStartTimeSecs)) {
            return false;
        }
        if (workers == null) {
            if (other.workers != null) {
                return false;
            }
        } else if (!workers.equals(other.workers)) {
            return false;
        }
        if (timeStamp != other.timeStamp) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }

}
