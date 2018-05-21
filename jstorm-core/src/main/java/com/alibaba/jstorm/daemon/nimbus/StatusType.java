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

/**
 * topology status:
 *
 * 1. Status:
 * this status will be stored in ZK killed/inactive/active/rebalancing
 *
 * 2. action:
 * monitor -- every Config.NIMBUS_MONITOR_FREQ_SECS seconds will trigger this only valid when current status is active inactivate -- client will trigger this
 * action, only valid when current status is active activate -- client will trigger this action only valid when current status is inactive startup -- when
 * nimbus startup, it will trigger this action only valid when current status is killed/rebalancing kill -- client kill topology will trigger this action, only
 * valid when current status is active/inactive/killed remove -- 30 seconds after client submit kill command, it will do this action, only valid when current
 * status is killed rebalance -- client submit rebalance command, only valid when current status is active/deactive do_rebalance -- 30 seconds after client
 * submit rebalance command, it will do this action, only valid when current status is rebalance
 *
 * 状态说明和动作的触发：
 *
 * kill：只有当前状态为active/inactive/killed，当client kill Topology时会触发这个动作。
 * monitor：如果当前状态为active，则每Config.NIMBUS_MONITOR_FREQ_SECS seconds会触发这个动作一次，nimbus会重新分配workers。
 * inactivate：当前状态为active时，client会触发这个动作。
 * activate：当前状态为inactive时，client会触发这个动作。
 * startup：当前状态为killed/rebalancing，当nimbus启动时也会触发这个动作。
 * remove：当前状态为killed，在client提交kill命令的一段时间之后触发这个动作
 * rebalance：当前状态为active/inactive，当client提交 rebalance命令后触发这个动作。
 * do-rebalance：当前状态为rebalancing，当client提交rebalance命令一段时间后触发这个动作。
 */
public enum StatusType {

    // topology status
    active("active"),
    inactive("inactive"),
    rebalancing("rebalancing"),
    killed("killed"),

    // actions
    activate("activate"),
    inactivate("inactivate"),
    monitor("monitor"),
    startup("startup"),
    kill("kill"),
    remove("remove"),
    rebalance("rebalance"),
    do_rebalance("do-rebalance"),
    done_rebalance("done-rebalance"),
    update_topology("update-topology"),
    upgrading("upgrading"),
    rollback("rollback");

    private String status;

    StatusType(String status) {
        this.status = status;
    }

    public String getStatus() {
        return status;
    }
}
