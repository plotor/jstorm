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

package com.alibaba.jstorm.daemon.supervisor;

import backtype.storm.utils.LocalState;
import com.alibaba.jstorm.blobstore.BlobStoreUtils;
import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.Common;
import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.daemon.worker.LocalAssignment;
import com.alibaba.jstorm.event.EventManager;
import com.alibaba.jstorm.event.EventManagerZkPusher;
import com.alibaba.jstorm.schedule.Assignment;
import com.alibaba.jstorm.schedule.default_assign.ResourceWorkerSlot;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.Pair;
import com.alibaba.jstorm.utils.PathUtils;
import com.alibaba.jstorm.utils.TimeUtils;
import com.google.common.collect.Sets;
import org.apache.commons.io.FileUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * supervisor SynchronizeSupervisor workflow
 * 1. writer local assignment to LocalState
 * 2. download new assignments of topologies
 * 3. remove useless topologies
 * 4. push a SyncProcessEvent to SyncProcessEvent's EventManager
 *
 * @author Johnfang (xiaojian.fxj@alibaba-inc.com)
 */
class SyncSupervisorEvent extends RunnableCallback {
    private static final Logger LOG = LoggerFactory.getLogger(SyncSupervisorEvent.class);

    private String supervisorId; // 本地 supervisor 的 ID
    private EventManager syncSupEventManager;
    private StormClusterState stormClusterState; // ZK 客户端实例
    private LocalState localState; // 本地 KV 数据库
    private Map<Object, Object> conf;
    private SyncProcessEvent syncProcesses;
    private int lastTime; // 最近一次扫描的时间
    private Heartbeat heartbeat;

    @SuppressWarnings("unchecked")
    public SyncSupervisorEvent(String supervisorId, Map conf, EventManager syncSupEventManager,
                               StormClusterState stormClusterState, LocalState localState,
                               SyncProcessEvent syncProcesses, Heartbeat heartbeat) {
        this.syncProcesses = syncProcesses;
        this.syncSupEventManager = syncSupEventManager;
        this.stormClusterState = stormClusterState;
        this.conf = conf;
        this.supervisorId = supervisorId;
        this.localState = localState;
        this.heartbeat = heartbeat;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void run() {
        LOG.debug("Synchronizing supervisor, interval (sec): " + TimeUtils.time_delta(lastTime));
        lastTime = TimeUtils.current_time_secs();
        // make sure that the status is the same for each execution of syncsupervisor
        HealthStatus healthStatus = heartbeat.getHealthStatus(); // 获取当前 supervisor 心跳状态
        try {
            RunnableCallback syncCallback = new EventManagerZkPusher(this, syncSupEventManager);

            // 从本地获取任务分配的版本信息
            Map<String, Integer> assignmentVersion = (Map<String, Integer>) localState.get(Common.LS_LOCAL_ZK_ASSIGNMENT_VERSION);
            if (assignmentVersion == null) {
                assignmentVersion = new HashMap<>();
            }
            // 从本地获取任务分配信息
            Map<String, Assignment> assignments = (Map<String, Assignment>) localState.get(Common.LS_LOCAl_ZK_ASSIGNMENTS);
            if (assignments == null) {
                assignments = new HashMap<>();
            }
            LOG.debug("get local assignments  " + assignments);
            LOG.debug("get local assignments version " + assignmentVersion);

            /*
             * 1: 获取所有 topology 的版本信息和任务分配信息，并更新到本地，同时为 /jstorm/assignment 添加监听回调
             */
            if (healthStatus.isMoreSeriousThan(HealthStatus.ERROR)) {
                // 检查当前 supervisor 的状态信息，如果是 PANIC 或 ERROR，则清除所有本地的任务分配相关信息
                // if status is panic or error, clear all assignments and kill all workers
                assignmentVersion.clear();
                assignments.clear();
                LOG.warn("Supervisor machine check status: " + healthStatus + ", killing all workers.");
            } else {
                // 获取所有 topology 的版本和任务分配信息，更新到本地（即更新 assignmentVersion 和 assignments 参数）
                this.getAllAssignments(assignmentVersion, assignments, syncCallback);
            }
            LOG.debug("Get all assignments " + assignments);

            /*
             * 2: 从当前 supervisor 本地（STORM-LOCAL-DIR/supervisor/stormdist/）获取所有的 topologyId
             */
            List<String> downloadedTopologyIds = StormConfig.get_supervisor_toplogy_list(conf);
            LOG.debug("Downloaded storm ids: " + downloadedTopologyIds);

            /*
             * 3: 获取当前 supervisor 的任务分配信息，<port, LocalAssignments>
             */
            Map<Integer, LocalAssignment> zkAssignment = this.getLocalAssign(stormClusterState, supervisorId, assignments);

            /*
             * 4: 将当前 supervisor 的任务分配信息记录到 LocalState 中
             */
            Map<Integer, LocalAssignment> localAssignment;
            try {
                LOG.debug("Writing local assignment " + zkAssignment);
                localAssignment = (Map<Integer, LocalAssignment>) localState.get(Common.LS_LOCAL_ASSIGNMENTS); // local-assignments
                if (localAssignment == null) {
                    localAssignment = new HashMap<>();
                }
                localState.put(Common.LS_LOCAL_ASSIGNMENTS, zkAssignment);
            } catch (IOException e) {
                LOG.error("put LS_LOCAL_ASSIGNMENTS " + zkAssignment + " to localState failed");
                throw e;
            }

            /*
             * 5: 获取所有需要更新的 topology 对应的 ID 集合（包括需要重新下载的 topology）
             */
            Set<String> updateTopologies = this.getUpdateTopologies(localAssignment, zkAssignment, assignments);
            Set<String> reDownloadTopologies = this.getNeedReDownloadTopologies(localAssignment);
            if (reDownloadTopologies != null) {
                updateTopologies.addAll(reDownloadTopologies);
            }

            // get upgrade topology ports
            Map<String, Set<Pair<String, Integer>>> upgradeTopologyPorts =
                    this.getUpgradeTopologies(stormClusterState, localAssignment, zkAssignment);
            if (upgradeTopologyPorts.size() > 0) {
                LOG.info("upgrade topology ports:{}", upgradeTopologyPorts);
                updateTopologies.addAll(upgradeTopologyPorts.keySet());
            }

            /*
             * 6: 从 nimbus 下载对应的 topology 任务代码
             */
            // 从 ZK 上获取当前 supervisor 上分配的 [topologyId, master-code-dir] 信息
            Map<String, String> topologyCodes = getTopologyCodeLocations(assignments, supervisorId);
            // downloadFailedTopologyIds which can't finished download binary from nimbus
            Set<String> downloadFailedTopologyIds = new HashSet<>(); // 记录所有下载失败的 topologyId
            // 下载 topology 任务代码
            this.downloadTopology(topologyCodes, downloadedTopologyIds, updateTopologies, assignments, downloadFailedTopologyIds);

            /*
             * 7: 删除无用的 topology 任务代码（之前下载过，但是本次未分配给当前 supervisor 的 topology）
             */
            this.removeUselessTopology(topologyCodes, downloadedTopologyIds);

            /*
             * 8: 运行同步进程事件
             */
            syncProcesses.run(zkAssignment, downloadFailedTopologyIds, upgradeTopologyPorts);

            // set the trigger to update heartbeat of supervisor
            // 触发心跳更新
            heartbeat.updateHbTrigger(true);

            try {
                // update localState
                localState.put(Common.LS_LOCAL_ZK_ASSIGNMENT_VERSION, assignmentVersion);
                localState.put(Common.LS_LOCAl_ZK_ASSIGNMENTS, assignments);
            } catch (IOException e) {
                LOG.error("put LS_LOCAL_ZK_ASSIGNMENT_VERSION & LS_LOCAl_ZK_ASSIGNMENTS to localState failed");
                throw e;
            }
        } catch (Exception e) {
            LOG.error("Failed to init SyncSupervisorEvent", e);
            // throw new RuntimeException(e);
        }
        if (healthStatus.isMoreSeriousThan(HealthStatus.PANIC)) {
            // if status is panic, kill supervisor;
            JStormUtils.halt_process(0, "Supervisor machine check status: Panic! !!!!shutdown!!!!");
        }

    }

    /**
     * download code with two cluster modes: local and distributed
     */
    private void downloadStormCode(Map conf, String topologyId, String masterCodeDir) throws IOException, TException {
        String clusterMode = StormConfig.cluster_mode(conf);
        if (clusterMode.endsWith("distributed")) {
            BlobStoreUtils.downloadDistributeStormCode(conf, topologyId, masterCodeDir);
        } else if (clusterMode.endsWith("local")) {
            BlobStoreUtils.downloadLocalStormCode(conf, topologyId, masterCodeDir);
        }
    }

    /**
     * a port must be assigned to a topology
     *
     * @param stormClusterState
     * @param supervisorId
     * @param assignments [topology_id, Assignment]
     * @return map: [port,LocalAssignment]
     * @throws Exception
     */
    private Map<Integer, LocalAssignment> getLocalAssign(
            StormClusterState stormClusterState, String supervisorId, Map<String, Assignment> assignments) throws Exception {
        Map<Integer, LocalAssignment> portToAssignment = new HashMap<>();
        for (Entry<String, Assignment> assignEntry : assignments.entrySet()) {
            String topologyId = assignEntry.getKey();
            Assignment assignment = assignEntry.getValue();

            // 获取当前 supervisor 分配的任务
            Map<Integer, LocalAssignment> portTasks = this.readMyTasks(stormClusterState, topologyId, supervisorId, assignment);
            if (portTasks == null) {
                continue;
            }

            // a port must be assigned to one assignment
            // 校验、保证每一个port对应一个任务
            for (Entry<Integer, LocalAssignment> entry : portTasks.entrySet()) {
                Integer port = entry.getKey();
                LocalAssignment la = entry.getValue();
                if (!portToAssignment.containsKey(port)) {
                    portToAssignment.put(port, la);
                } else {
                    // 同一个端口不允许出现多个 topology
                    throw new RuntimeException("Should not have multiple topologies assigned to one port");
                }
            }
        }
        return portToAssignment;
    }

    /**
     * get local node's tasks
     *
     * @return Map: [port, LocalAssignment]
     */
    @SuppressWarnings("unused")
    private Map<Integer, LocalAssignment> readMyTasks(
            StormClusterState stormClusterState, String topologyId, String supervisorId, Assignment assignmentInfo) throws Exception {
        Map<Integer, LocalAssignment> portTasks = new HashMap<>();

        Set<ResourceWorkerSlot> workers = assignmentInfo.getWorkers();
        if (workers == null) {
            LOG.error("No worker found for assignment {}!", assignmentInfo);
            return portTasks;
        }

        for (ResourceWorkerSlot worker : workers) {
            if (!supervisorId.equals(worker.getNodeId())) {
                continue;
            }
            portTasks.put(worker.getPort(), new LocalAssignment(
                    topologyId, worker.getTasks(), Common.topologyIdToName(topologyId), worker.getMemSize(),
                    worker.getCpu(), worker.getJvm(), assignmentInfo.getTimeStamp()));
        }

        return portTasks;
    }

    /**
     * get master code dir for each topology
     * 从 ZK 上获取当前 supervisor 上分配的 [topologyId, master-code-dir] 信息
     *
     * @return Map: [topologyId, master-code-dir] from zookeeper
     */
    public static Map<String, String> getTopologyCodeLocations(
            Map<String, Assignment> assignments, String supervisorId) throws Exception {
        Map<String, String> rtn = new HashMap<>();
        for (Entry<String, Assignment> entry : assignments.entrySet()) {
            String topologyId = entry.getKey();
            Assignment assignmentInfo = entry.getValue();

            // 获取当前 topology 分配的 worker 信息
            Set<ResourceWorkerSlot> workers = assignmentInfo.getWorkers();
            for (ResourceWorkerSlot worker : workers) {
                String node = worker.getNodeId();
                if (supervisorId.equals(node)) {
                    // 对应分配在当前 supervisor 上的 topology 信息，记录 [topologyId, master-code-dir]
                    rtn.put(topologyId, assignmentInfo.getMasterCodeDir());
                    break;
                }
            }

        }
        return rtn;
    }

    /**
     * @param topologyCodes [topologyId, master-code-dir]
     * @param downloadedTopologyIds 本地已经下载的 topology
     * @param updateTopologies
     * @param assignments
     * @param downloadFailedTopologyIds
     * @throws Exception
     */
    public void downloadTopology(Map<String, String> topologyCodes, List<String> downloadedTopologyIds,
                                 Set<String> updateTopologies, Map<String, Assignment> assignments, Set<String> downloadFailedTopologyIds) throws Exception {
        Set<String> downloadTopologies = new HashSet<>();
        for (Entry<String, String> entry : topologyCodes.entrySet()) {
            String topologyId = entry.getKey();
            String masterCodeDir = entry.getValue();
            // 对于未下载的或需要更新的 topology 执行下载
            if (!downloadedTopologyIds.contains(topologyId) || updateTopologies.contains(topologyId)) {
                LOG.info("Downloading code for storm id " + topologyId + " from " + masterCodeDir);
                int retry = 0;
                while (retry < 3) {
                    try {
                        this.downloadStormCode(conf, topologyId, masterCodeDir);
                        // update assignment timeStamp
                        StormConfig.write_supervisor_topology_timestamp(
                                conf, topologyId, assignments.get(topologyId).getTimeStamp());
                        break;
                    } catch (IOException | TException e) {
                        LOG.error(e + " downloadStormCode failed, topologyId:" + topologyId + ", masterCodeDir:" + masterCodeDir);
                    }
                    retry++;
                }
                if (retry < 3) {
                    LOG.info("Finished downloading code for storm id " + topologyId + " from " + masterCodeDir);
                    downloadTopologies.add(topologyId);
                } else {
                    LOG.error("Failed to download code for storm id " + topologyId + " from " + masterCodeDir);
                    downloadFailedTopologyIds.add(topologyId);
                }
            }
        }
        // clear directory of topologyId is dangerous,
        // so it only clear the topologyId which isn't contained by downloadedTopologyIds
        for (String topologyId : downloadFailedTopologyIds) {
            if (!downloadedTopologyIds.contains(topologyId)) {
                try {
                    String stormroot = StormConfig.supervisor_stormdist_root(conf, topologyId);
                    File destDir = new File(stormroot);
                    FileUtils.deleteQuietly(destDir);
                } catch (Exception e) {
                    LOG.error("Failed to clear directory about storm id " + topologyId + " on supervisor ");
                }
            }
        }

        this.updateTaskCleanupTimeout(downloadTopologies);
    }

    /**
     * 删除那些之前下载过，但是本次未分配给当前 supervisor 的 topology
     *
     * @param topologyCodes
     * @param downloadedTopologyIds
     */
    public void removeUselessTopology(Map<String, String> topologyCodes, List<String> downloadedTopologyIds) {
        for (String topologyId : downloadedTopologyIds) {
            if (!topologyCodes.containsKey(topologyId)) {
                LOG.info("Removing code for storm id " + topologyId);
                String path = null;
                try {
                    // ${storm.local.dir}/supervisor/stormdist/${topology_id}
                    path = StormConfig.supervisor_stormdist_root(conf, topologyId);
                    PathUtils.rmr(path);
                } catch (IOException e) {
                    String errMsg = "rmr the path:" + path + "failed\n";
                    LOG.error(errMsg, e);
                }
            }
        }
    }

    private Set<String> getUpdateTopologies(Map<Integer, LocalAssignment> localAssignments,
                                            Map<Integer, LocalAssignment> zkAssignments, Map<String, Assignment> assignments) {
        Set<String> ret = new HashSet<>();
        if (localAssignments != null && zkAssignments != null) {
            for (Entry<Integer, LocalAssignment> entry : localAssignments.entrySet()) {
                Integer port = entry.getKey();
                LocalAssignment localAssignment = entry.getValue();
                LocalAssignment zkAssignment = zkAssignments.get(port);
                if (localAssignment == null || zkAssignment == null) {
                    continue;
                }

                Assignment assignment = assignments.get(localAssignment.getTopologyId());
                if (localAssignment.getTopologyId().equals(zkAssignment.getTopologyId())
                        && assignment != null && assignment.isTopologyChange(localAssignment.getTimeStamp())) {
                    if (ret.add(localAssignment.getTopologyId())) {
                        LOG.info("Topology " + localAssignment.getTopologyId() +
                                " has been updated. LocalTs=" + localAssignment.getTimeStamp() + ", ZkTs=" + zkAssignment.getTimeStamp());
                    }
                }
            }
        }
        return ret;
    }

    private Map<String, Set<Pair<String, Integer>>> getUpgradeTopologies(
            StormClusterState stormClusterState, Map<Integer, LocalAssignment> localAssignments, Map<Integer, LocalAssignment> zkAssignments) {
        SupervisorInfo supervisorInfo = heartbeat.getSupervisorInfo();
        Map<String, Set<Pair<String, Integer>>> ret = new HashMap<>();

        try {
            Set<String> upgradingTopologies = Sets.newHashSet(stormClusterState.get_upgrading_topologies());
            for (String topologyId : upgradingTopologies) {
                List<String> upgradingWorkers = stormClusterState.get_upgrading_workers(topologyId);
                for (String worker : upgradingWorkers) {
                    String[] hostPort = worker.split(":");
                    String host = hostPort[0];
                    Integer port = Integer.valueOf(hostPort[1]);
                    if (host.equals(supervisorInfo.getHostName()) &&
                            supervisorInfo.getWorkerPorts().contains(port) &&
                            localAssignments.containsKey(port) && zkAssignments.containsKey(port)) {
                        Set<Pair<String, Integer>> ports = ret.get(topologyId);
                        if (ports == null) {
                            ports = new HashSet<>();
                        }
                        ports.add(new Pair<>(host, port));
                        ret.put(topologyId, ports);
                    }
                }
            }
        } catch (Exception ex) {
            LOG.error("Failed to get upgrading topologies", ex);
        }

        return ret;
    }

    @SuppressWarnings("unchecked")
    private Set<String> getNeedReDownloadTopologies(Map<Integer, LocalAssignment> localAssignment) {
        Set<String> reDownloadTopologies = syncProcesses.getTopologyIdNeedDownload().getAndSet(null);
        if (reDownloadTopologies == null || reDownloadTopologies.size() == 0) {
            return null;
        }
        Set<String> needRemoveTopologies = new HashSet<>();
        Map<Integer, String> portToStartWorkerId = syncProcesses.getPortToWorkerId();
        for (Entry<Integer, LocalAssignment> entry : localAssignment.entrySet()) {
            if (portToStartWorkerId.containsKey(entry.getKey())) {
                needRemoveTopologies.add(entry.getValue().getTopologyId());
            }
        }
        LOG.debug("workers are starting on these topologies, delay downloading topology binary: " + needRemoveTopologies);
        reDownloadTopologies.removeAll(needRemoveTopologies);
        if (reDownloadTopologies.size() > 0) {
            LOG.info("Following topologies are going to re-download the jars, " + reDownloadTopologies);
        }
        return reDownloadTopologies;
    }

    @SuppressWarnings("unchecked")
    private void updateTaskCleanupTimeout(Set<String> topologies) {
        Map topologyConf = null;
        Map<String, Integer> taskCleanupTimeouts = new HashMap<>();
        for (String topologyId : topologies) {
            try {
                topologyConf = StormConfig.read_supervisor_topology_conf(conf, topologyId);
            } catch (IOException e) {
                LOG.info("Failed to read conf for " + topologyId);
            }

            Integer cleanupTimeout = null;
            if (topologyConf != null) {
                cleanupTimeout = JStormUtils.parseInt(topologyConf.get(ConfigExtension.TASK_CLEANUP_TIMEOUT_SEC));
            }

            if (cleanupTimeout == null) {
                cleanupTimeout = ConfigExtension.getTaskCleanupTimeoutSec(conf);
            }

            taskCleanupTimeouts.put(topologyId, cleanupTimeout);
        }

        Map<String, Integer> localTaskCleanupTimeouts = null;
        try {
            localTaskCleanupTimeouts = (Map<String, Integer>) localState.get(Common.LS_TASK_CLEANUP_TIMEOUT);
        } catch (IOException e) {
            LOG.error("Failed to read local task cleanup timeout map", e);
        }

        if (localTaskCleanupTimeouts == null) {
            localTaskCleanupTimeouts = taskCleanupTimeouts;
        } else {
            localTaskCleanupTimeouts.putAll(taskCleanupTimeouts);
        }

        try {
            localState.put(Common.LS_TASK_CLEANUP_TIMEOUT, localTaskCleanupTimeouts);
        } catch (IOException e) {
            LOG.error("Failed to write local task cleanup timeout map", e);
        }
    }

    /**
     * 获取所有 topology 的版本和任务分配信息，更新 assignmentVersion 和 localZkAssignments 参数
     *
     * @param assignmentVersion <topology_id, assign_version>
     * @param localZkAssignments <topology_id, assignment>
     * @param callback
     * @throws Exception
     */
    private void getAllAssignments(
            Map<String, Integer> assignmentVersion,
            Map<String, Assignment> localZkAssignments,
            RunnableCallback callback) throws Exception {

        Map<String, Assignment> ret = new HashMap<>();
        // <topology_id, assign_version>
        Map<String, Integer> updateAssignmentVersion = new HashMap<>();

        // 枚举所有 /assignments/${topology_id}
        List<String> assignments = stormClusterState.assignments(callback);
        if (assignments == null) {
            assignmentVersion.clear();
            localZkAssignments.clear();
            LOG.debug("No assignment in ZK");
            return;
        }

        for (String topologyId : assignments) {
            // 获取 topology 在 ZK 上的版本号
            Integer zkVersion = stormClusterState.assignment_version(topologyId, callback);
            LOG.debug(topologyId + "'s assignment version of zk is :" + zkVersion);
            // 获取 topology 在本地的版本号
            Integer recordedVersion = assignmentVersion.get(topologyId);
            LOG.debug(topologyId + "'s assignment version of local is :" + recordedVersion);

            Assignment assignment = null;
            if (recordedVersion != null && zkVersion != null && recordedVersion.equals(zkVersion)) {
                // 版本号相同就从本地获取 topology 的任务分配信息
                assignment = localZkAssignments.get(topologyId);
            }

            // because the first version is 0
            if (assignment == null) {
                // 本地不命中，从 ZK 拉取 topology 的分配信息
                assignment = stormClusterState.assignment_info(topologyId, callback);
            }
            if (assignment == null) {
                LOG.error("Failed to get assignment of " + topologyId + " from ZK");
                continue;
            }

            // 记录 topology 对应的版本信息
            updateAssignmentVersion.put(topologyId, zkVersion);
            // 记录 topology 对应的任务分配信息
            ret.put(topologyId, assignment);
        }

        // 更新 topology 的在本地的版本信息和任务分配信息
        assignmentVersion.clear();
        assignmentVersion.putAll(updateAssignmentVersion);
        localZkAssignments.clear();
        localZkAssignments.putAll(ret);
    }

}
