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
 * - 2.1 download new topology
 * - 2.2 release useless worker
 * - 2.3 assign new task to /local-dir/supervisor/localstate
 * - 2.4 add one syncProcesses event
 *
 * 3. Every {supervisor.monitor.frequency.secs} run SyncProcesses
 * - 3.1 kill useless worker
 * - 3.2 start new worker
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
         * 1. 创建并清空 ${storm.local.dir}/supervisor/tmp
         *    用于临时存放从 nimbus 下载的文件（stormcode.cer、stormconf.cer、stormjar.jar、lib），
         *    简单处理之后复制到 ${storm.local.dir}/supervisor/stormdist/${topology_id}
         */
        String path = StormConfig.supervisorTmpDir(conf);
        FileUtils.cleanDirectory(new File(path));

        /*
         * 2. 创建 ZK 操作对象和 worker 运行错误数据上报器
         */
        StormClusterState stormClusterState = Cluster.mk_storm_cluster_state(conf);
        String hostName = JStormServerUtils.getHostName(conf); // 获取主机名
        // 创建 worker 运行错误上报器，上错 worker 的运行错误数据到 ZK:taskerrors/${topology_id}/${task_id}
        WorkerReportError workerReportError = new WorkerReportError(stormClusterState, hostName);

        /*
         * 3. 创建 LocalState 对象（简单、低效的 KV 存储），同时获取当前 supervisorId（如果不存在则创建一个（基于 UUID））
         */

        // 创建 LocalState 对象（简单、低效的 KV 存储）
        LocalState localState = StormConfig.supervisorState(conf);

        // 获取 supervisorId，如果不存在则创建一个（UUID）
        String supervisorId = (String) localState.get(Common.LS_ID); // supervisor-id
        if (supervisorId == null) {
            supervisorId = UUID.randomUUID().toString();
            localState.put(Common.LS_ID, supervisorId);
        }
        // clean LocalStat's zk-assignment & versions
        localState.remove(Common.LS_LOCAl_ZK_ASSIGNMENTS); // lcoal-zk-assignment
        localState.remove(Common.LS_LOCAL_ZK_ASSIGNMENT_VERSION); // lcoal-zk-assignment.version

        /*
         * 4. 创建并启动心跳机制
         * every ${supervisor.heartbeat.frequency.secs} write SupervisorInfo to ZK sync heartbeat to nimbus
         */
        Heartbeat hb = new Heartbeat(conf, stormClusterState, supervisorId, localState);
        hb.update(); // 更新端口号列表，并写入 supervisor 信息到 ZK

        // 启动循环尝试执行 Heartbeat#update 方法
        Vector<AsyncLoopThread> threads = new Vector<>();
        AsyncLoopThread heartbeat = new AsyncLoopThread(hb, false, null, Thread.MIN_PRIORITY, true);
        threads.add(heartbeat);

        // Sync heartbeat to Apsaras（飞天） Container
        AsyncLoopThread syncContainerHbThread = SyncContainerHb.mkSupervisorInstance(conf);
        if (syncContainerHbThread != null) {
            threads.add(syncContainerHbThread);
        }

        /*
         * 5. 启动并定期执行 SyncSupervisorEvent#run() 方法（默认间隔 10 秒），从 nimbus 节点领取分配给当前节点的任务并启动执行
         */
        ConcurrentHashMap<String, String> workerThreadPids = new ConcurrentHashMap<>();
        SyncProcessEvent syncProcessEvent = new SyncProcessEvent(
                supervisorId, conf, localState, workerThreadPids, sharedContext, workerReportError, stormClusterState);

        EventManagerImp syncSupEventManager = new EventManagerImp();
        AsyncLoopThread syncSupEventThread = new AsyncLoopThread(syncSupEventManager);
        threads.add(syncSupEventThread);

        SyncSupervisorEvent syncSupervisorEvent = new SyncSupervisorEvent(
                supervisorId, conf, syncSupEventManager, stormClusterState, localState, syncProcessEvent, hb);

        // ${supervisor.monitor.frequency.secs}，默认为 10 秒
        int syncFrequency = JStormUtils.parseInt(conf.get(Config.SUPERVISOR_MONITOR_FREQUENCY_SECS));
        EventManagerPusher syncSupervisorPusher = new EventManagerPusher(syncSupEventManager, syncSupervisorEvent, syncFrequency);
        /*
         * 每间隔一段时间（默认为 10 秒）调用 EventManagerPusher#run()，
         * 本质上是调用 EventManagerImp#add(RunnableCallback) 将 syncSupervisorEvent 记录到自己的阻塞队列中，
         * 同时 EventManagerImp 也会循环消费阻塞队列，取出其中的 syncSupervisorEvent，并应用其 run 方法：SyncSupervisorEvent#run()
         */
        AsyncLoopThread syncSupervisorThread = new AsyncLoopThread(syncSupervisorPusher);
        threads.add(syncSupervisorThread);

        /*
         * 6. 启动 HTTP 服务（默认端口 7622），主要用于查看和下载当前节点的运行日志数据
         */
        Httpserver httpserver = null;
        if (!StormConfig.local_mode(conf)) { // distribute mode
            int port = ConfigExtension.getSupervisorDeamonHttpserverPort(conf); // 7622 default
            httpserver = new Httpserver(port, conf);
            httpserver.start();
        }

        /*
         * 7. 检查 supervisor 运行状况 & 同步 nimbus 配置信息
         */
        if (!StormConfig.local_mode(conf)) {
            if (ConfigExtension.isEnableCheckSupervisor(conf)) {
                // 启动一个线程用于检查 supervisor 健康状况
                SupervisorHealth supervisorHealth = new SupervisorHealth(conf, hb, supervisorId);
                AsyncLoopThread healthThread = new AsyncLoopThread(supervisorHealth, false, null, Thread.MIN_PRIORITY, true);
                threads.add(healthThread);
            }

            // 定期同步 nimbus 节点的 storm.yaml 配置（每隔 20 ～ 30 秒）
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
     * 添加一个 hook 方法，
     * 当 jvm 运行停止时调用 SupervisorManger#run() 方法
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
        try {
            /*
             * 解析配置文件：
             * 1. 解析 default.yaml
             * 2. 解析 storm.yaml
             * 3. 解析 -Dstorm.options 指定的命令行参数
             * 4. 替换所有配置项中的 JSTORM_HOME 占位符
             */
            Map<Object, Object> conf = Utils.readStormConfig();

            // 确保当前为集群运行模式
            StormConfig.validate_distributed_mode(conf);

            // 创建进程文件： ${storm.local.dir}/supervisor/pids/${pid}
            this.createPid(conf);

            // 创建并启动 supervisor
            SupervisorManger supervisorManager = this.mkSupervisor(conf, null);

            JStormUtils.redirectOutput("/dev/null");

            // 注册 SupervisorManger，当 JVM 进程停止时执行 shutdown 逻辑
            this.initShutdownHook(supervisorManager);

            // 循环监测 shutdown 方法是否执行完毕
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
        // 1. 设置线程默认异常处理器
        Thread.setDefaultUncaughtExceptionHandler(new DefaultUncaughtExceptionHandler());
        // 2. 空实现
        JStormServerUtils.startTaobaoJvmMonitor();
        Supervisor instance = new Supervisor();
        instance.run();
    }
}
