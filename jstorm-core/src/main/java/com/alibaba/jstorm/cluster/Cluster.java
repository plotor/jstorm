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

package com.alibaba.jstorm.cluster;

import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.daemon.supervisor.SupervisorInfo;
import com.alibaba.jstorm.schedule.Assignment;
import com.alibaba.jstorm.task.TaskInfo;
import com.alibaba.jstorm.task.error.TaskError;
import com.alibaba.jstorm.utils.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

/**
 * storm operation ZK
 *
 * @author yannian/longda/zhiyuan.ls
 */
public class Cluster {

    // TODO: need to move constants to ZkConstant

    private static Logger LOG = LoggerFactory.getLogger(Cluster.class);

    public static final String ZK_SEPARATOR = "/";

    public static final String ASSIGNMENTS_ROOT = "assignments";
    public static final String ASSIGNMENTS_BAK = "assignments_bak";
    public static final String TASKS_ROOT = "tasks";
    public static final String STORMS_ROOT = "topology";
    public static final String SUPERVISORS_ROOT = "supervisors";
    public static final String TASKBEATS_ROOT = "taskbeats";
    public static final String TASKERRORS_ROOT = "taskerrors";
    public static final String MASTER_ROOT = "nimbus_master";
    public static final String NIMBUS_SLAVE_ROOT = "nimbus_slave";
    public static final String METRIC_ROOT = "metrics";
    public static final String GRAY_UPGRADE_ROOT = "gray_upgrade";

    public static final String LAST_ERROR = "last_error";
    public static final String NIMBUS_SLAVE_DETAIL_ROOT = "nimbus_slave_detail";
    public static final String BLOBSTORE_ROOT = "blobstore";
    public static final String BLACKLIST_ROOT = "blacklist";
    // stores the latest update sequence for a blob
    public static final String BLOBSTORE_MAX_KEY_SEQUENCE_NUMBER_ROOT = "blobstoremaxkeysequencenumber";
    public static final String CONF = "conf";
    public static final String UPGRADED_WORKERS = "upgraded_workers";
    public static final String UPGRADING_WORKERS = "upgrading_workers";

    public static final String ASSIGNMENTS_SUBTREE;
    public static final String ASSIGNMENTS_BAK_SUBTREE;
    public static final String TASKS_SUBTREE;
    public static final String STORMS_SUBTREE;
    public static final String SUPERVISORS_SUBTREE;
    public static final String TASKBEATS_SUBTREE;
    public static final String TASKERRORS_SUBTREE;
    public static final String MASTER_SUBTREE;
    public static final String NIMBUS_SLAVE_SUBTREE;
    public static final String METRIC_SUBTREE;
    public static final String GRAY_UPGRADE_SUBTREE;
    public static final String NIMBUS_SLAVE_DETAIL_SUBTREE;
    // Blobstore subtree /storm/blobstore
    public static final String BLOBSTORE_SUBTREE;
    public static final String BLOBSTORE_MAX_KEY_SEQUENCE_NUMBER_SUBTREE;
    public static final String BLACKLIST_SUBTREE;

    static {
        ASSIGNMENTS_SUBTREE = ZK_SEPARATOR + ASSIGNMENTS_ROOT; // assignments
        ASSIGNMENTS_BAK_SUBTREE = ZK_SEPARATOR + ASSIGNMENTS_BAK; // assignments_bak
        TASKS_SUBTREE = ZK_SEPARATOR + TASKS_ROOT; // tasks
        STORMS_SUBTREE = ZK_SEPARATOR + STORMS_ROOT; // topology
        SUPERVISORS_SUBTREE = ZK_SEPARATOR + SUPERVISORS_ROOT; // supervisors
        TASKBEATS_SUBTREE = ZK_SEPARATOR + TASKBEATS_ROOT; // taskbeats
        TASKERRORS_SUBTREE = ZK_SEPARATOR + TASKERRORS_ROOT; // taskerrors
        MASTER_SUBTREE = ZK_SEPARATOR + MASTER_ROOT; // nimbus_master
        NIMBUS_SLAVE_SUBTREE = ZK_SEPARATOR + NIMBUS_SLAVE_ROOT;
        METRIC_SUBTREE = ZK_SEPARATOR + METRIC_ROOT; // metrics
        GRAY_UPGRADE_SUBTREE = ZK_SEPARATOR + GRAY_UPGRADE_ROOT; // gray_upgrade
        NIMBUS_SLAVE_DETAIL_SUBTREE = ZK_SEPARATOR + NIMBUS_SLAVE_DETAIL_ROOT; // nimbus_slave_detail
        BLOBSTORE_SUBTREE = ZK_SEPARATOR + BLOBSTORE_ROOT; // blobstore
        BLOBSTORE_MAX_KEY_SEQUENCE_NUMBER_SUBTREE = ZK_SEPARATOR + BLOBSTORE_MAX_KEY_SEQUENCE_NUMBER_ROOT; // blobstoremaxkeysequencenumber
        BLACKLIST_SUBTREE = ZK_SEPARATOR + BLACKLIST_ROOT; // blacklist
    }

    /**
     * supervisors/${supervisor_id}
     *
     * @param id
     * @return
     */
    public static String supervisor_path(String id) {
        return SUPERVISORS_SUBTREE + ZK_SEPARATOR + id;
    }

    /**
     * 获取 topology 对应的任务分配信息路径
     *
     * @param id
     * @return
     */
    public static String assignment_path(String id) {
        // assignments/{topology_id}
        return ASSIGNMENTS_SUBTREE + ZK_SEPARATOR + id;
    }

    /**
     * topology/${topology_id}
     *
     * @param id
     * @return
     */
    public static String storm_path(String id) {
        return STORMS_SUBTREE + ZK_SEPARATOR + id;
    }

    public static String storm_task_root(String topology_id) {
        // tasks/{topology_id}
        return TASKS_SUBTREE + ZK_SEPARATOR + topology_id;
    }

    public static String taskbeat_storm_root(String topology_id) {
        return TASKBEATS_SUBTREE + ZK_SEPARATOR + topology_id;
    }

    /**
     * taskerrors/${topology_id}
     *
     * @param topology_id
     * @return
     */
    public static String taskerror_storm_root(String topology_id) {
        return TASKERRORS_SUBTREE + ZK_SEPARATOR + topology_id;
    }

    public static String lasterror_path(String topology_id) {
        return taskerror_storm_root(topology_id) + ZK_SEPARATOR + LAST_ERROR;
    }

    public static String taskerror_path(String topology_id, int task_id) {
        return taskerror_storm_root(topology_id) + ZK_SEPARATOR + task_id;
    }

    public static String metric_path(String topology_id) {
        return METRIC_SUBTREE + ZK_SEPARATOR + topology_id;
    }

    /**
     * gray_upgrade/${topology_id}
     *
     * @param topology_id
     * @return
     */
    public static String gray_upgrade_base_path(String topology_id) {
        return GRAY_UPGRADE_SUBTREE + ZK_SEPARATOR + topology_id;
    }

    public static String gray_upgrade_conf_path(String topologyId) {
        return gray_upgrade_base_path(topologyId) + ZK_SEPARATOR + CONF;
    }

    /**
     * gray_upgrade/${topology_id}/upgrading_workers
     *
     * @param topologyId
     * @return
     */
    public static String gray_upgrade_upgrading_workers_path(String topologyId) {
        return Cluster.gray_upgrade_base_path(topologyId) + ZK_SEPARATOR + UPGRADING_WORKERS;
    }

    /**
     * gray_upgrade/${topology_id}/upgraded_workers
     *
     * @param topologyId
     * @return
     */
    public static String gray_upgrade_upgraded_workers_path(String topologyId) {
        return Cluster.gray_upgrade_base_path(topologyId) + ZK_SEPARATOR + UPGRADED_WORKERS;
    }

    public static String gray_upgrade_upgrading_worker_path(String topologyId, String hostPort) {
        return Cluster.gray_upgrade_upgrading_workers_path(topologyId) + ZK_SEPARATOR + hostPort;
    }

    public static String gray_upgrade_upgraded_worker_path(String topologyId, String hostPort) {
        return Cluster.gray_upgrade_upgraded_workers_path(topologyId) + ZK_SEPARATOR + hostPort;
    }

    /**
     * 获取 assignments_bak/{topology_name}
     *
     * @param id
     * @return
     */
    public static String assignment_bak_path(String id) {
        return ASSIGNMENTS_BAK_SUBTREE + ZK_SEPARATOR + id; // assignments_bak/{topology_name}
    }

    /**
     * 获取 blobstore/${key}
     *
     * @param key
     * @return
     */
    public static String blobstore_path(String key) {
        return BLOBSTORE_SUBTREE + ZK_SEPARATOR + key;
    }

    public static String blob_max_key_sequence_number_path(String key) {
        return BLOBSTORE_MAX_KEY_SEQUENCE_NUMBER_SUBTREE + ZK_SEPARATOR + key;
    }

    public static String blacklist_path(String key) {
        return BLACKLIST_SUBTREE + ZK_SEPARATOR + key;
    }

    @SuppressWarnings("rawtypes")
    public static StormClusterState mk_storm_cluster_state(Map cluster_state_spec) throws Exception {
        return new StormZkClusterState(cluster_state_spec);
    }

    public static StormClusterState mk_storm_cluster_state(ClusterState cluster_state_spec) throws Exception {
        return new StormZkClusterState(cluster_state_spec);
    }

    /**
     * 从 ZK 上获取当前 topology 对应的所有 {@link TaskInfo}
     *
     * @param zkCluster
     * @param topologyId
     * @return
     * @throws Exception
     */
    public static Map<Integer, TaskInfo> get_all_taskInfo(StormClusterState zkCluster, String topologyId) throws Exception {
        return zkCluster.task_all_info(topologyId);
    }

    /**
     * 获取指定 topology 下对应的 <task_id, component_id> 映射列表
     *
     * @param zkCluster
     * @param topologyId
     * @param taskInfoMap
     * @return
     * @throws Exception
     */
    public static Map<Integer, String> get_all_task_component(
            StormClusterState zkCluster, String topologyId, Map<Integer, TaskInfo> taskInfoMap) throws Exception {
        if (taskInfoMap == null) {
            // 从 ZK 上获取当前 topology 对应的所有 {@link TaskInfo}
            taskInfoMap = get_all_taskInfo(zkCluster, topologyId);
        }
        if (taskInfoMap == null) {
            return null;
        }
        return Common.getTaskToComponent(taskInfoMap);
    }

    public static Map<Integer, String> get_all_task_type(StormClusterState zkCluster, String topologyId,
                                                         Map<Integer, TaskInfo> taskInfoMap) throws Exception {
        if (taskInfoMap == null) {
            taskInfoMap = get_all_taskInfo(zkCluster, topologyId);
        }

        if (taskInfoMap == null) {
            return null;
        }

        return Common.getTaskToType(taskInfoMap);
    }

    /**
     * if a topology's name is equal to the input storm_name, then return the topology id, otherwise return null
     */
    public static String get_topology_id(StormClusterState zkCluster, String storm_name) throws Exception {
        List<String> active_storms = zkCluster.active_storms();
        String rtn = null;
        if (active_storms != null) {
            for (String topology_id : active_storms) {
                if (!topology_id.contains(storm_name)) {
                    continue;
                }
                StormBase base = zkCluster.storm_base(topology_id, null);
                if (base != null && storm_name.equals(Common.getTopologyNameById(topology_id))) {
                    rtn = topology_id;
                    break;
                }
            }
        }
        return rtn;
    }

    /**
     * get all topology's StormBase
     *
     * @param zkCluster zk cluster state
     * @return map[topology_id, StormBase]
     */
    public static HashMap<String, StormBase> get_all_StormBase(StormClusterState zkCluster) throws Exception {
        HashMap<String, StormBase> rtn = new HashMap<>();
        List<String> active_storms = zkCluster.active_storms();
        if (active_storms != null) {
            for (String topology_id : active_storms) {
                StormBase base = zkCluster.storm_base(topology_id, null);
                if (base != null) {
                    rtn.put(topology_id, base);
                }
            }
        }
        return rtn;
    }

    /**
     * 从 ZK 上获取所有的 supervisor 信息：<id, supervisorInfo>
     *
     * @param stormClusterState storm cluster state
     * @param callback watcher callback
     * @return Map[supervisorId, SupervisorInfo]
     */
    public static Map<String, SupervisorInfo> get_all_SupervisorInfo(
            StormClusterState stormClusterState, RunnableCallback callback) throws Exception {

        Map<String, SupervisorInfo> rtn = new TreeMap<>();

        // 获取所有的 supervisor_id, 位于 /zk/supervisors 下面
        List<String> supervisorIds = stormClusterState.supervisors(callback);

        // ignore any supervisors in blacklist
        List<String> blacklist = stormClusterState.get_blacklist();

        if (supervisorIds != null) {
            for (Iterator<String> iter = supervisorIds.iterator(); iter.hasNext(); ) {
                String supervisorId = iter.next();
                // 获取指定 id 的 supervisor 信息
                SupervisorInfo supervisorInfo = stormClusterState.supervisor_info(supervisorId);
                if (supervisorInfo == null) {
                    LOG.warn("Failed to get SupervisorInfo of " + supervisorId);
                } else if (blacklist.contains(supervisorInfo.getHostName())) {
                    // 跳过黑名单 supervisor
                    LOG.warn(" hostname:" + supervisorInfo.getHostName() + " is in blacklist");
                } else {
                    rtn.put(supervisorId, supervisorInfo);
                }
            }
        } else {
            LOG.info("No alive supervisor");
        }

        return rtn;
    }

    public static Map<String, Assignment> get_all_assignment(
            StormClusterState stormClusterState, RunnableCallback callback) throws Exception {
        Map<String, Assignment> ret = new HashMap<>();

        // get /assignments {topology_id}
        List<String> assignments = stormClusterState.assignments(callback);
        if (assignments == null) {
            LOG.debug("No assignment of ZK");
            return ret;
        }

        for (String topology_id : assignments) {
            Assignment assignment = stormClusterState.assignment_info(topology_id, callback);
            if (assignment == null) {
                LOG.error("Failed to get Assignment of " + topology_id + " from ZK");
                continue;
            }

            ret.put(topology_id, assignment);
        }

        return ret;
    }

    public static Map<String, String> get_all_nimbus_slave(StormClusterState stormClusterState) throws Exception {
        List<String> hosts = stormClusterState.get_nimbus_slaves();
        if (hosts == null || hosts.size() == 0) {
            return null;
        }

        Map<String, String> ret = new HashMap<>();
        for (String host : hosts) {
            String time = stormClusterState.get_nimbus_slave_time(host);
            ret.put(host, time);
        }

        return ret;
    }

    public static String get_supervisor_hostname(StormClusterState stormClusterState, String supervisorId) throws Exception {
        SupervisorInfo supervisorInfo = stormClusterState.supervisor_info(supervisorId);
        if (supervisorInfo == null) {
            return null;
        } else {
            return supervisorInfo.getHostName();
        }
    }

    public static boolean is_topology_exist_error(StormClusterState stormClusterState, String topologyId) throws Exception {
        Map<Integer, String> lastErrMap = stormClusterState.topo_lastErr_time(topologyId);
        if (lastErrMap == null || lastErrMap.size() == 0) {
            return false;
        }

        int now = TimeUtils.current_time_secs();
        for (Entry<Integer, String> entry : lastErrMap.entrySet()) {
            Integer timeout = entry.getKey();
            Integer timestamp = Integer.valueOf(entry.getValue());

            if (now - timestamp <= timeout) {
                return true;
            }
        }

        return false;
    }

    public static Map<Integer, List<TaskError>> get_all_task_errors(StormClusterState stormClusterState, String topologyId) {
        Map<Integer, List<TaskError>> ret = new HashMap<>();
        try {
            List<String> errorTasks = stormClusterState.task_error_ids(topologyId);
            if (errorTasks == null || errorTasks.size() == 0) {
                return ret;
            }

            for (String taskIdStr : errorTasks) {
                Integer taskId;
                try {
                    taskId = Integer.valueOf(taskIdStr);
                } catch (Exception e) {
                    // skip last_error
                    continue;
                }

                List<TaskError> taskErrorList = stormClusterState.task_errors(topologyId, taskId);
                ret.put(taskId, taskErrorList);
            }
        } catch (Exception ignored) {
        }
        return ret;
    }

}
