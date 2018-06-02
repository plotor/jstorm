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

package com.alibaba.jstorm.daemon.nimbus;

import backtype.storm.scheduler.INimbus;
import backtype.storm.scheduler.IScheduler;
import backtype.storm.scheduler.SupervisorDetails;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.WorkerSlot;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * 目前对于 {@link INimbus} 默认且唯一的实现
 */
public class DefaultINimbus implements INimbus {

    @Override
    public void prepare(Map stormConf, String schedulerLocalDir) {
    }

    /**
     * 返回所有可以在下一次调度中被分配的 slot 集合，包含：
     * 1. 空闲且可以被分配的 slot
     * 2. 已被使用且可以被再分配的 slot
     *
     * @param existingSupervisors
     * @param topologies
     * @param topologiesMissingAssignments
     * @return
     */
    @Override
    public Collection<WorkerSlot> allSlotsAvailableForScheduling(
            Collection<SupervisorDetails> existingSupervisors, Topologies topologies, Set<String> topologiesMissingAssignments) {
        Collection<WorkerSlot> result = new HashSet<>();
        // 将所有的 supervisor 封装成 WorkerSlot
        for (SupervisorDetails detail : existingSupervisors) {
            for (Integer port : detail.getAllPorts())
                result.add(new WorkerSlot(detail.getId(), port));
        }
        return result;
    }

    @Override
    public void assignSlots(Topologies topologies, Map<String, Collection<WorkerSlot>> newSlotsByTopologyId) {
    }

    @Override
    public String getHostName(Map<String, SupervisorDetails> existingSupervisors, String nodeId) {
        return null;
    }

    @Override
    public IScheduler getForcedScheduler() {
        return null;
    }

}
