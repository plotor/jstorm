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

import backtype.storm.Config;
import backtype.storm.messaging.IContext;
import backtype.storm.utils.LocalState;
import backtype.storm.utils.Utils;
import com.alibaba.jstorm.callback.AsyncLoopThread;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.Cluster;
import com.alibaba.jstorm.cluster.Common;
import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.config.SupervisorRefreshConfig;
import com.alibaba.jstorm.daemon.worker.WorkerReportError;
import com.alibaba.jstorm.daemon.worker.hearbeat.SyncContainerHb;
import com.alibaba.jstorm.event.EventManagerImp;
import com.alibaba.jstorm.event.EventManagerPusher;
import com.alibaba.jstorm.utils.DefaultUncaughtExceptionHandler;
import com.alibaba.jstorm.utils.JStormServerUtils;
import com.alibaba.jstorm.utils.JStormUtils;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Map;
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Supervisor 可以理解为单机任务调度器，它负责监听 Nimbus 的任务调度，
 * 启动相应的 Worker 对 Nimbus 分配的任务进行处理，同时监测 Worker 的
 * 运行状态，一旦发现有 Worker 有异常，就会杀死该 Worker，并将原先分配
 * 给 Worker 的任务交还给 Nimbus 重新分配
 *
 * Supervisor workflow
 *
 * 1. write SupervisorInfo to ZK
 *
 * 2. Every 10 seconds run SynchronizeSupervisor
 * 2.1 download new topology
 * 2.2 release useless worker
 * 2.3 assign new task to /local-dir/supervisor/localstate
 * 2.4 add one syncProcesses event
 *
 * 3. Every supervisor.monitor.frequency.secs run SyncProcesses
 * 3.1 kill useless worker
 * 3.2 start new worker
 *
 * 4. create heartbeat thread every supervisor.heartbeat.frequency.secs, write SupervisorInfo to ZK
 *
 * @author Johnfang (xiaojian.fxj@alibaba-inc.com)
 */

@SuppressWarnings({"unchecked", "unused"})
public class Supervisor {

    private static Logger LOG = LoggerFactory.getLogger(Supervisor.class);

    /**
     * create and start a supervisor
     *
     * @param conf : configuration (default.yaml & storm.yaml)
     * @param sharedContext : null (right now)
     * @return SupervisorManger: which is used to shutdown all workers and supervisor
     */
    @SuppressWarnings("rawtypes")
    public SupervisorManger mkSupervisor(Map conf, IContext sharedContext) throws Exception {

        LOG.info("Starting Supervisor with conf " + conf);

        /*
         * Step 1: cleanup all files in ${storm.local.dir}/supervisor/tmp
         */
        String path = StormConfig.supervisorTmpDir(conf);
        FileUtils.cleanDirectory(new File(path));

        /*
         * Step 2: create ZK operation instance StormClusterState
         */

        StormClusterState stormClusterState = Cluster.mk_storm_cluster_state(conf);

        String hostName = JStormServerUtils.getHostName(conf);
        WorkerReportError workerReportError = new WorkerReportError(stormClusterState, hostName);

        /*
         * Step 3: create LocalState (a simple KV store)
         * 3.1 create LocalState instance;
         * 3.2 get supervisorId, if there's no supervisorId, create one
         */

        LocalState localState = StormConfig.supervisorState(conf);

        String supervisorId = (String) localState.get(Common.LS_ID); // supervisor-id
        if (supervisorId == null) {
            supervisorId = UUID.randomUUID().toString();
            localState.put(Common.LS_ID, supervisorId);
        }
        //clean LocalStat's zk-assignment & versions
        localState.remove(Common.LS_LOCAl_ZK_ASSIGNMENTS);
        localState.remove(Common.LS_LOCAL_ZK_ASSIGNMENT_VERSION);

        Vector<AsyncLoopThread> threads = new Vector<>();

        /*
         * Step 4: create HeartBeat
         * every supervisor.heartbeat.frequency.secs, write SupervisorInfo to ZK sync heartbeat to nimbus
         */

        Heartbeat hb = new Heartbeat(conf, stormClusterState, supervisorId, localState);
        hb.update();

        AsyncLoopThread heartbeat = new AsyncLoopThread(hb, false, null, Thread.MIN_PRIORITY, true);
        threads.add(heartbeat);

        // Sync heartbeat to Apsara Container
        AsyncLoopThread syncContainerHbThread = SyncContainerHb.mkSupervisorInstance(conf);
        if (syncContainerHbThread != null) {
            threads.add(syncContainerHbThread);
        }

        /*
         * Step 5: create and start sync Supervisor thread every
         * supervisor.monitor.frequency.secs second run SyncSupervisor
         */

        ConcurrentHashMap<String, String> workerThreadPids = new ConcurrentHashMap<>();
        SyncProcessEvent syncProcessEvent = new SyncProcessEvent(
                supervisorId, conf, localState, workerThreadPids, sharedContext, workerReportError, stormClusterState);

        EventManagerImp syncSupEventManager = new EventManagerImp();
        AsyncLoopThread syncSupEventThread = new AsyncLoopThread(syncSupEventManager);
        threads.add(syncSupEventThread);

        SyncSupervisorEvent syncSupervisorEvent = new SyncSupervisorEvent(
                supervisorId, conf, syncSupEventManager, stormClusterState, localState, syncProcessEvent, hb);

        int syncFrequency = JStormUtils.parseInt(conf.get(Config.SUPERVISOR_MONITOR_FREQUENCY_SECS));
        EventManagerPusher syncSupervisorPusher = new EventManagerPusher(syncSupEventManager, syncSupervisorEvent, syncFrequency);
        AsyncLoopThread syncSupervisorThread = new AsyncLoopThread(syncSupervisorPusher);
        threads.add(syncSupervisorThread);

        /*
         * Step 6: start httpserver
         */
        Httpserver httpserver = null;
        if (!StormConfig.local_mode(conf)) {
            int port = ConfigExtension.getSupervisorDeamonHttpserverPort(conf);
            httpserver = new Httpserver(port, conf);
            httpserver.start();
        }

        /*
         * Step 7: check supervisor
         */
        if (!StormConfig.local_mode(conf)) {
            if (ConfigExtension.isEnableCheckSupervisor(conf)) {
                SupervisorHealth supervisorHealth = new SupervisorHealth(conf, hb, supervisorId);
                AsyncLoopThread healthThread = new AsyncLoopThread(supervisorHealth, false, null, Thread.MIN_PRIORITY, true);
                threads.add(healthThread);
            }

            // init refresh config thread
            AsyncLoopThread refreshConfigThread = new AsyncLoopThread(new SupervisorRefreshConfig(conf));
            threads.add(refreshConfigThread);
        }

        // create SupervisorManger which can shutdown all supervisor and workers
        return new SupervisorManger(
                conf, supervisorId, threads, syncSupEventManager, httpserver, stormClusterState, workerThreadPids);
    }

    /**
     * shutdown
     */
    public void killSupervisor(SupervisorManger supervisor) {
        supervisor.shutdown();
    }

    /**
     * 添加一个 hook 方法
     *
     * @param supervisor
     */
    private void initShutdownHook(SupervisorManger supervisor) {
        Runtime.getRuntime().addShutdownHook(new Thread(supervisor));
    }

    /**
     * 创建 ${storm.local.dir}/supervisor/pids/${pid}
     *
     * @param conf
     * @throws Exception
     */
    private void createPid(Map conf) throws Exception {
        // ${storm.local.dir}/supervisor/pids
        String pidDir = StormConfig.supervisorPids(conf);
        // ${storm.local.dir}/supervisor/pids/${pid}
        JStormServerUtils.createPid(pidDir);
    }

    /**
     * start supervisor
     */
    public void run() {
        SupervisorManger supervisorManager; // supervisor shutdown manager
        try {
            // 解析配置文件
            Map<Object, Object> conf = Utils.readStormConfig();

            // 确保当前为分布式模式
            StormConfig.validate_distributed_mode(conf);

            // 创建 ${storm.local.dir}/supervisor/pids/${pid}
            this.createPid(conf);

            supervisorManager = this.mkSupervisor(conf, null);

            JStormUtils.redirectOutput("/dev/null");

            this.initShutdownHook(supervisorManager);

            while (!supervisorManager.isFinishShutdown()) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ignored) {
                }
            }

        } catch (Throwable e) {
            if (e instanceof OutOfMemoryError) {
                LOG.error("Halting due to out of memory error...");
            }
            LOG.error("Fail to run supervisor ", e);
            System.exit(1);
        } finally {
            LOG.info("Shutdown supervisor!!!");
        }

    }

    /**
     * start supervisor daemon
     */
    public static void main(String[] args) {
        Thread.setDefaultUncaughtExceptionHandler(new DefaultUncaughtExceptionHandler());
        JStormServerUtils.startTaobaoJvmMonitor();
        Supervisor instance = new Supervisor();
        instance.run();
    }
}
