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

import backtype.storm.Config;
import backtype.storm.generated.Bolt;
import backtype.storm.generated.ComponentCommon;
import backtype.storm.generated.SpoutSpec;
import backtype.storm.generated.StateSpoutSpec;
import backtype.storm.generated.StormTopology;
import backtype.storm.utils.ThriftTopologyUtils;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.Common;
import com.alibaba.jstorm.daemon.supervisor.SupervisorInfo;
import com.alibaba.jstorm.schedule.TopologyAssignContext;
import com.alibaba.jstorm.utils.FailedAssignTopologyException;
import com.alibaba.jstorm.utils.JStormUtils;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class DefaultTopologyAssignContext extends TopologyAssignContext {

    private final StormTopology sysTopology;
    /** supervisor_id 与 supervisor 所属 hostname 的映射关系 */
    private final Map<String, String> sidToHostname;
    /** supervisor 所属 hostname 与 supervisor_id 的映射关系 */
    private final Map<String, List<String>> hostToSid;
    /** oldAssignment 对应的 worker 列表 */
    private final Set<ResourceWorkerSlot> oldWorkers;
    /** 组件 ID 与其 task id 列表的映射关系 */
    private final Map<String, List<Integer>> componentTasks;
    private final Set<ResourceWorkerSlot> unstoppedWorkers = new HashSet<>();
    private final int totalWorkerNum;
    private final int unstoppedWorkerNum;
    /** 保留的 worker 数 */
    private final int reserveWorkerNum;

    /**
     * 计算 worker 的数目（实际组件的 worker 数目 + topology master 对应的 worker 数目）
     * 最终返回的 worker 数目会基于 topology.workers 配置和各个组件的 worker 数目之和取最小值
     *
     * @return
     */
    private int computeWorkerNum() {
        // 获取拓扑设置的 worker 数目：topology.workers
        Integer settingNum = JStormUtils.parseInt(stormConf.get(Config.TOPOLOGY_WORKERS));

        /*
         * ret：实际组件的 worker 数目之和
         * hintSum：所有组件的配置 worker 数目之和
         * tmCount：topologyMaster 的 worker 数目
         */
        int ret, hintSum = 0, tmCount = 0;

        // 获取当前 topology 的所有组件信息
        Map<String, Object> components = ThriftTopologyUtils.getComponents(sysTopology);
        for (Entry<String, Object> entry : components.entrySet()) {
            String componentName = entry.getKey(); // 组件 ID
            Object component = entry.getValue(); // 组件对象

            ComponentCommon common = null;
            if (component instanceof Bolt) {
                common = ((Bolt) component).get_common();
            }
            if (component instanceof SpoutSpec) {
                common = ((SpoutSpec) component).get_common();
            }
            if (component instanceof StateSpoutSpec) {
                common = ((StateSpoutSpec) component).get_common();
            }

            // 获取组件并行度
            int hint = common.get_parallelism_hint();
            if (componentName.equals(Common.TOPOLOGY_MASTER_COMPONENT_ID)) { // topology master
                tmCount += hint;
                continue;
            }
            // 计算所有组件并行度之和
            hintSum += hint;
        }

        if (settingNum == null) {
            ret = hintSum;
        } else {
            // 取 ${topology.workers} 配置的与实际各个组件设置总数的较小值
            ret = Math.min(settingNum, hintSum);
        }

        // 是否为 topology master 设置 single worker
        Boolean isTmSingleWorker = ConfigExtension.getTopologyMasterSingleWorker(stormConf);
        if (isTmSingleWorker != null) {
            if (isTmSingleWorker) {
                // Assign a single worker for topology master
                ret += tmCount;
                this.setAssignSingleWorkerForTM(true);
                ConfigExtension.setTopologyMasterSingleWorker(stormConf, true);
            }
        } else {
            // If not configured, judge this config by worker number
            if (ret >= 10) {
                ret += tmCount;
                this.setAssignSingleWorkerForTM(true);
                ConfigExtension.setTopologyMasterSingleWorker(stormConf, true);
            }
        }

        return ret;
    }

    public int computeUnstoppedAssignments() {
        for (Integer task : unstoppedTaskIds) {
            // if unstoppedTasksIds isn't empty, it should be REASSIGN/Monitor
            ResourceWorkerSlot worker = oldAssignment.getWorkerByTaskId(task);
            unstoppedWorkers.add(worker);
        }
        return unstoppedWorkers.size();
    }

    private void refineDeadTasks() {
        Set<Integer> rawDeadTasks = this.getDeadTaskIds();
        Set<Integer> refineDeadTasks = new HashSet<>(rawDeadTasks);

        Set<Integer> unstoppedTasks = this.getUnstoppedTaskIds();

        // if type is ASSIGN_NEW, rawDeadTasks is empty
        // then the oldWorkerTasks should be existingAssignment
        for (Integer task : rawDeadTasks) {
            if (unstoppedTasks.contains(task)) {
                continue;
            }
            // 存在于 rawDeadTasks 而不在 unstoppedTasks 中的
            for (ResourceWorkerSlot worker : oldWorkers) {
                if (worker.getTasks().contains(task)) {
                    refineDeadTasks.addAll(worker.getTasks());
                }
            }
        }
        this.setDeadTaskIds(refineDeadTasks);
    }

    /**
     * 获取 supervisor_id 与 supervisor 所属 hostname 的映射关系
     *
     * @return
     */
    private Map<String, String> generateSidToHost() {
        Map<String, String> sidToHostname = new HashMap<>();
        if (oldAssignment != null) {
            sidToHostname.putAll(oldAssignment.getNodeHost());
        }

        for (Entry<String, SupervisorInfo> entry : cluster.entrySet()) {
            String supervisorId = entry.getKey();
            SupervisorInfo supervisorInfo = entry.getValue();
            sidToHostname.put(supervisorId, supervisorInfo.getHostName());
        }

        return sidToHostname;
    }

    /**
     * 基于 TopologyAssignContext 构建对象
     *
     * @param context
     */
    public DefaultTopologyAssignContext(TopologyAssignContext context) {
        super(context);

        try {
            // 添加系统组件
            sysTopology = Common.system_topology(stormConf, rawTopology);
        } catch (Exception e) {
            throw new FailedAssignTopologyException("Failed to generate system topology");
        }

        // 获取 supervisorId 与 supervisor 所属 hostname 的映射关系： <supervisor_id, hostname>
        sidToHostname = this.generateSidToHost();
        // <hostname, supervisorId>
        hostToSid = JStormUtils.reverse_map(sidToHostname);

        if (oldAssignment != null && oldAssignment.getWorkers() != null) {
            oldWorkers = oldAssignment.getWorkers();
        } else {
            oldWorkers = new HashSet<>();
        }

        // 更新 deadTask 列表
        this.refineDeadTasks();

        // 组件 ID 与其 task id 列表的映射关系
        componentTasks = JStormUtils.reverse_map(context.getTaskToComponent());
        for (Entry<String, List<Integer>> entry : componentTasks.entrySet()) {
            List<Integer> componentTaskList = entry.getValue();
            // 对于 taskId 按照由小到大排序
            Collections.sort(componentTaskList);
        }

        /*
         * 计算 worker 的数目（实际组件的 worker 数目 + topologyMaster 的 worker 数目）
         * 最终的 worker 数目会基于 topology.workers 配置和各个组件的 worker 数目之和取最小值
         */
        totalWorkerNum = this.computeWorkerNum();
        // 计算 unstopped worker 数目
        unstoppedWorkerNum = this.computeUnstoppedAssignments();
        // 获取保留的 worker 数目
        reserveWorkerNum = ConfigExtension.getReserveWorkers(stormConf);
    }

    public StormTopology getSysTopology() {
        return sysTopology;
    }

    public Map<String, String> getSidToHostname() {
        return sidToHostname;
    }

    public Map<String, List<String>> getHostToSid() {
        return hostToSid;
    }

    public Map<String, List<Integer>> getComponentTasks() {
        return componentTasks;
    }

    public int getTotalWorkerNum() {
        return totalWorkerNum;
    }

    public int getUnstoppedWorkerNum() {
        return unstoppedWorkerNum;
    }

    public Set<ResourceWorkerSlot> getOldWorkers() {
        return oldWorkers;
    }

    @Override
    public Set<ResourceWorkerSlot> getUnstoppedWorkers() {
        return unstoppedWorkers;
    }

    @Override
    public String toString() {
        return (String) stormConf.get(Config.TOPOLOGY_NAME);
    }

    public String toDetailString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }

    public int getReserveWorkerNum() {
        return reserveWorkerNum;
    }
}
