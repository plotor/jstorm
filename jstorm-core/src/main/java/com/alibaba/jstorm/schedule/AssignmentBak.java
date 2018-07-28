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

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class AssignmentBak implements Serializable {

    private static final long serialVersionUID = 7633746649144483965L;

    /**
     * "componentTasks": {
     * "__topology_master": [
     * 1
     * ],
     * "word_count_bolt": [
     * 2,
     * 3,
     * 4,
     * 5,
     * 6
     * ],
     * "sentence_split_bolt": [
     * 7,
     * 8,
     * 9,
     * 10,
     * 11
     * ],
     * "kafka_spout": [
     * 14,
     * 15,
     * 16,
     * 17,
     * 18
     * ],
     * "__acker": [
     * 12,
     * 13
     * ]
     * },
     */
    private final Map<String, List<Integer>> componentTasks;

    /**
     * "assignment": {
     * "masterCodeDir": "/home/work/data/jstorm/nimbus/stormdist/zhenchao-demo-topology-2-1532048664",
     * "nodeHost": {
     * "980bbcfd-5438-4e25-aee9-bf411304a446": "10.38.164.192"
     * },
     * "taskStartTimeSecs": {
     * "1": 1532048664,
     * "2": 1532048664,
     * "3": 1532048664,
     * "4": 1532048664,
     * "5": 1532048664,
     * "6": 1532048664,
     * "7": 1532048664,
     * "8": 1532048664,
     * "9": 1532048664,
     * "10": 1532048664,
     * "11": 1532048664,
     * "12": 1532048664,
     * "13": 1532048664,
     * "14": 1532048664,
     * "15": 1532048664,
     * "16": 1532048664,
     * "17": 1532048664,
     * "18": 1532048664
     * },
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
     * ],
     * "timeStamp": 1532048664907,
     * "type": "Assign"
     * }
     */
    private final Assignment assignment;

    public AssignmentBak(Map<String, List<Integer>> componentTasks, Assignment assignment) {
        super();
        this.componentTasks = componentTasks;
        this.assignment = assignment;
    }

    public Map<String, List<Integer>> getComponentTasks() {
        return componentTasks;
    }

    public Assignment getAssignment() {
        return assignment;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }
}
