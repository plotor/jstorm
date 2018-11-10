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

import backtype.storm.Config;
import backtype.storm.generated.Nimbus;
import backtype.storm.generated.Nimbus.Iface;
import backtype.storm.generated.TopologyMetric;
import backtype.storm.scheduler.INimbus;
import backtype.storm.utils.Utils;
import com.alibaba.jstorm.callback.AsyncLoopRunnable;
import com.alibaba.jstorm.callback.AsyncLoopThread;
import com.alibaba.jstorm.callback.Callback;
import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.config.RefreshableComponents;
import com.alibaba.jstorm.daemon.nimbus.metric.ClusterMetricsRunnable;
import com.alibaba.jstorm.daemon.supervisor.Httpserver;
import com.alibaba.jstorm.daemon.worker.hearbeat.SyncContainerHb;
import com.alibaba.jstorm.schedule.CleanRunnable;
import com.alibaba.jstorm.schedule.FollowerRunnable;
import com.alibaba.jstorm.schedule.MonitorRunnable;
import com.alibaba.jstorm.utils.DefaultUncaughtExceptionHandler;
import com.alibaba.jstorm.utils.JStormServerUtils;
import com.alibaba.jstorm.utils.JStormUtils;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * NimbusServer workflow:
 * 1. cleanup interrupted topology delete /storm-local-dir/nimbus/topologyid/stormdis delete /storm-zk-root/storms/topologyid
 * 2. set /storm-zk-root/storms/topology stats as run
 * 3. start one thread, every nimbus.monitor.freq.secs set /storm-zk-root/storms/ all topology as monitor. when the topology's status is monitor, nimbus would reassign workers
 * 4. start one thread, every nimbus.cleanup.inbox.freq.secs cleanup useless jar
 *
 *
 * JStorm的主节点上运行着nimbus的守护进程，主要负责与ZK通信，分发代码，给集群中的从节点分配任务，监视集群状态等等。
 * 此外nimbus需要维护的所有状态都会存储在ZK中，JStorm为了减少对ZK的访问次数做了一些缓存
 *
 * NimbusServer 运行流程：
 * 1. 清除中断的 topology（删除本地目录 /storm-local-dir/nimbus/${topology_id}/stormdis 和 zk 上的 /storm-zk-root/storms/${topology_id}）
 * 2. 设置 /storm-zk-root/storms/topology 中的 topology 状态为 active
 * 3. 启动一个 monitor 线程，每 ${nimbus.monitor.reeq.secs} 检查 /storm-zk-root/storms 中所有 topology 状态，如果 topology 中有 task 是不活动的，则将其状态转换为 monitor，并重新分配 workers
 * 4. 启动一个 cleaner 线程，每 ${nimubs.cleanup.inbox.freq.secs} 清除无用的 jar
 *
 * @author version 1: Nathan Marz version 2: Lixin/Chenjun version 3: Longda
 */
public class NimbusServer {

    private static final Logger LOG = LoggerFactory.getLogger(NimbusServer.class);

    private NimbusData data;

    private ServiceHandler serviceHandler;

    private TopologyAssign topologyAssign;

    private THsHaServer thriftServer;

    private FollowerRunnable follower;

    private Httpserver hs;

    private final List<AsyncLoopThread> smartThreads = new ArrayList<>();

    public static void main(String[] args) throws Exception {
        // 设置默认的线程异常处理器
        Thread.setDefaultUncaughtExceptionHandler(new DefaultUncaughtExceptionHandler());

        /*
         * 加载并解析集群配置信息，全部以 KV 的形式封装到 Map 中：
         * 1. 解析 default.yaml
         * 2. 解析 storm.yaml
         * 3. 解析 -Dstorm.options 指定的命令行参数
         * 4. 替换所有配置项中的 JSTORM_HOME 占位符
         */
        Map config = Utils.readStormConfig();

        // 空实现
        JStormServerUtils.startTaobaoJvmMonitor();

        NimbusServer instance = new NimbusServer();

        // 创建一个默认的 nimbus 启动类
        INimbus iNimbus = new DefaultINimbus();

        // 启动 nimbus 服务
        instance.launchServer(config, iNimbus);
    }

    /**
     * 创建当前 JVM 进程对应的目录：${storm.local.dir}/nimbus/pids/${pid}
     * 一般是：jstorm-local/nimbus/pids/${pid}
     *
     * @param conf
     * @throws Exception
     */
    private void createPid(Map conf) throws Exception {
        // 创建 ${storm.local.dir}/nimbus 目录
        String pidDir = StormConfig.masterPids(conf);
        // 创建当前 JVM 进程对应的目录：${storm.local.dir}/nimbus/pids/${pid}
        JStormServerUtils.createPid(pidDir);
    }

    /**
     * 启动 nimbus 服务
     *
     * @param conf 配置项
     * @param inimbus
     */
    @SuppressWarnings("rawtypes")
    private void launchServer(final Map conf, INimbus inimbus) {
        LOG.info("Begin to start nimbus with conf " + conf);
        try {
            // 1. 验证当前为分布式运行模式，不允许以本地模式运行
            StormConfig.validate_distributed_mode(conf);

            // 2. 创建当前 JVM 进程对应的目录：${storm.local.dir}/nimbus/pids/${pid}，如果存在历史运行记录，则会进行清除
            this.createPid(conf);

            // 3. 注册 shutdown hook 方法，用于在 JVM 进程终止时执行清理逻辑
            this.initShutdownHook();

            // 4. 模板方法
            inimbus.prepare(conf, StormConfig.masterInimbus(conf));

            // 5. 基于 conf 创建 NimbusData 对象
            data = this.createNimbusData(conf, inimbus);

            // 6. 注册一个 follower 线程
            this.initFollowerThread(conf);

            // 7. 创建并启动一个后端 HTTP 服务（默认端口为 7621，主要用于查看和下载 nimbus 的日志数据）
            int port = ConfigExtension.getNimbusDeamonHttpserverPort(conf);
            hs = new Httpserver(port, conf);
            hs.start();

            // 8. 如果集群运行在 YARN 上，则初始化容器心跳线程
            this.initContainerHBThread(conf);

            // 9. 创建 ServiceHandler（实现了 Nimbus.Iface），并启动 Thrift 服务，用于处理 Nimbus 请求
            serviceHandler = new ServiceHandler(data);
            this.initThrift(conf);
        } catch (Throwable e) {
            if (e instanceof OutOfMemoryError) {
                LOG.error("Halting due to out of memory error...");
            }
            LOG.error("Fail to run nimbus ", e);
        } finally {
            this.cleanup();
        }
        LOG.info("Quit nimbus");
    }

    /**
     * handle manual conf changes, check every 15 sec
     */
    private void mkRefreshConfThread(final NimbusData nimbusData) {
        nimbusData.getScheduExec().scheduleAtFixedRate(new RunnableCallback() {
            @Override
            public void run() {
                LOG.debug("checking changes in storm.yaml...");

                Map newConf = Utils.readStormConfig();
                if (Utils.isConfigChanged(nimbusData.getConf(), newConf)) {
                    LOG.warn("detected changes in storm.yaml, updating...");
                    synchronized (nimbusData.getConf()) {
                        nimbusData.getConf().clear();
                        nimbusData.getConf().putAll(newConf);
                    }

                    RefreshableComponents.refresh(newConf);
                } else {
                    LOG.debug("no changes detected, stay put.");
                }
            }

            @Override
            public Object getResult() {
                return 15;
            }
        }, 15, 15, TimeUnit.SECONDS);

        LOG.info("Successfully init configuration refresh thread");
    }

    public ServiceHandler launcherLocalServer(final Map conf, INimbus inimbus) throws Exception {
        LOG.info("Begin to start nimbus on local model");
        StormConfig.validate_local_mode(conf);
        inimbus.prepare(conf, StormConfig.masterInimbus(conf));
        data = this.createNimbusData(conf, inimbus);
        this.init(conf);
        serviceHandler = new ServiceHandler(data);
        return serviceHandler;
    }

    /**
     * 主要作用是检测能否在 YARN 上运行 jstorm 集群，如果可以则需要创建一个新的线程用于处理。
     * 使用容器的目的是可以在一个物理集群上运行多个不同逻辑集群甚至多个 JStorm 集群，可以动态调整逻辑集群分到的资源，此外，YARN 能提供非常强的可扩展性。
     * 容器线程会被添加到 NimbusServer 中，这个容器线程是守护线程，且马上就会启动，这个线程的run方法里面包含两个处理：
     *
     * 1. handleWriteDir：这个方法的主要作用是清除掉容器上的过期心跳信息，如果 JStorm 集群容器目录下的心跳信息大于 10，则需要清除（从最老的开始）。
     * 2. handlReadDir：这里主要是用于维护本地是否能接受到集群上的hb信息，如果多次超时则要抛出异常。
     *
     * @param conf
     * @throws IOException
     */
    private void initContainerHBThread(Map conf) throws IOException {
        AsyncLoopThread thread = SyncContainerHb.mkNimbusInstance(conf);
        if (thread != null) {
            smartThreads.add(thread);
        }
    }

    private void initMetricRunnable() {
        AsyncLoopThread thread = new AsyncLoopThread(ClusterMetricsRunnable.getInstance());
        smartThreads.add(thread);
    }

    private void init(Map conf) throws Exception {
        data.init();

        NimbusUtils.cleanupCorruptTopologies(data);

        // 启动拓扑分配
        this.initTopologyAssign();

        // 状态更新
        this.initTopologyStatus();

        // 清除函数
        this.initCleaner(conf);

        this.initMetricRunnable();

        if (!data.isLocalMode()) {
            this.initMonitor(conf);
        }
    }

    /**
     * 创建 {@link NimbusData} 对象
     *
     * @param conf
     * @param inimbus
     * @return
     * @throws Exception
     */
    private NimbusData createNimbusData(Map conf, INimbus inimbus) throws Exception {
        return new NimbusData(conf, inimbus);
    }

    private void initTopologyAssign() {
        topologyAssign = TopologyAssign.getInstance();
        topologyAssign.init(data);
    }

    /**
     * 初始化 topology 运行状态，这里设置为 {@code StatusType.startup}
     *
     * @throws Exception
     */
    private void initTopologyStatus() throws Exception {
        // get active topology in ZK
        List<String> active_ids = data.getStormClusterState().active_storms();

        if (active_ids != null) {
            for (String topologyid : active_ids) {
                // set the topology status as startup
                // in fact, startup won't change anything
                NimbusUtils.transition(data, topologyid, false, StatusType.startup);
                NimbusUtils.updateTopologyTaskTimeout(data, topologyid);
                NimbusUtils.updateTopologyTaskHb(data, topologyid);
            }
        }
        LOG.info("Successfully init topology status");
    }

    @SuppressWarnings("rawtypes")
    private void initMonitor(Map conf) {
        final ScheduledExecutorService scheduExec = data.getScheduExec();

        if (!data.isLaunchedMonitor()) {
            // Schedule Nimbus monitor
            MonitorRunnable r1 = new MonitorRunnable(data);

            int monitor_freq_secs = JStormUtils.parseInt(conf.get(Config.NIMBUS_MONITOR_FREQ_SECS), 10);
            scheduExec.scheduleAtFixedRate(r1, 0, monitor_freq_secs, TimeUnit.SECONDS);

            data.setLaunchedMonitor(true);
            LOG.info("Successfully init Monitor thread");
        } else {
            LOG.info("We have launched Monitor thread before");
        }
    }

    /**
     * Right now, every 600 seconds, nimbus will clean jar under /LOCAL-DIR/nimbus/inbox,
     * which is the uploading topology directory
     */
    @SuppressWarnings("rawtypes")
    private void initCleaner(Map conf) throws IOException {
        final ScheduledExecutorService scheduExec = data.getScheduExec();

        if (!data.isLaunchedCleaner()) {
            // Schedule Nimbus inbox cleaner/nimbus/inbox jar
            String dir_location = StormConfig.masterInbox(conf);
            int inbox_jar_expiration_secs = JStormUtils.parseInt(conf.get(Config.NIMBUS_INBOX_JAR_EXPIRATION_SECS), 3600);
            CleanRunnable r2 = new CleanRunnable(dir_location, inbox_jar_expiration_secs);

            int cleanup_inbox_freq_secs = JStormUtils.parseInt(conf.get(Config.NIMBUS_CLEANUP_INBOX_FREQ_SECS), 600);

            scheduExec.scheduleAtFixedRate(r2, 0, cleanup_inbox_freq_secs, TimeUnit.SECONDS);
            data.setLaunchedCleaner(true);
            LOG.info("Successfully init " + dir_location + " cleaner");
        } else {
            LOG.info("cleaner thread has been started already");
        }
    }

    /**
     * 初始化并启动 thrift 服务
     *
     * @param conf
     * @throws TTransportException
     */
    @SuppressWarnings("rawtypes")
    private void initThrift(Map conf) throws TTransportException {
        // 获取 thrift 端口，默认为 8627
        Integer thrift_port = JStormUtils.parseInt(conf.get(Config.NIMBUS_THRIFT_PORT)); // ${nimbus.thrift.port}
        TNonblockingServerSocket socket = new TNonblockingServerSocket(thrift_port);

        // ${nimbus.thrift.max_buffer_size}
        Integer maxReadBufSize = JStormUtils.parseInt(conf.get(Config.NIMBUS_THRIFT_MAX_BUFFER_SIZE));
        // 设置服务运行参数
        THsHaServer.Args args = new THsHaServer.Args(socket);
        args.workerThreads(ServiceHandler.THREAD_NUM); // 64
        args.protocolFactory(new TBinaryProtocol.Factory(false, true, maxReadBufSize, -1));
        args.processor(new Nimbus.Processor<Iface>(serviceHandler));
        args.maxReadBufferBytes = maxReadBufSize;

        thriftServer = new THsHaServer(args);

        LOG.info("Successfully started nimbus: started Thrift server...");
        thriftServer.serve();
    }

    /**
     * leader-follower 模式
     *
     * @param conf
     */
    @SuppressWarnings("unused")
    private void initFollowerThread(Map conf) {
        // 如果当前 nimbus 成为 leader，则会触发此回调执行初始化操作
        Callback leaderCallback = new Callback() {
            @Override
            public <T> Object execute(T... args) {
                try {
                    init(data.getConf());
                } catch (Exception e) {
                    LOG.error("Nimbus init error after becoming a leader", e);
                    JStormUtils.halt_process(0, "Failed to init nimbus");
                }
                return null;
            }
        };
        // 创建并启动 follower 线程
        follower = new FollowerRunnable(data, 5000, leaderCallback);
        Thread thread = new Thread(follower);
        thread.setDaemon(true);
        thread.start();
        LOG.info("Successfully init Follower thread");
    }

    /**
     * 注册 shutdownHook 方法，用于在服务终止时执行一些清理工作
     */
    private void initShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                NimbusServer.this.cleanup();
            }
        });
    }

    /**
     * 服务停止时的清理工作
     */
    public void cleanup() {
        if (data == null || data.getIsShutdown().getAndSet(true)) {
            LOG.info("Notify to quit nimbus");
            return;
        }

        LOG.info("Begin to shutdown nimbus");
        AsyncLoopRunnable.getShutdown().set(true);

        data.getScheduExec().shutdownNow();

        for (AsyncLoopThread t : smartThreads) {
            t.cleanup();
            JStormUtils.sleepMs(10);
            t.interrupt();
            LOG.info("Successfully cleanup " + t.getThread().getName());
        }

        if (serviceHandler != null) {
            serviceHandler.shutdown();
        }

        if (topologyAssign != null) {
            topologyAssign.cleanup();
            LOG.info("Successfully shutdown TopologyAssign thread");
        }

        if (follower != null) {
            follower.clean();
            LOG.info("Successfully shutdown follower thread");
        }

        if (data != null) {
            data.cleanup();
            LOG.info("Successfully shutdown NimbusData");
        }

        if (thriftServer != null) {
            thriftServer.stop();
            LOG.info("Successfully shutdown thrift server");
        }

        if (hs != null) {
            hs.shutdown();
            LOG.info("Successfully shutdown httpserver");
        }

        LOG.info("Successfully shutdown nimbus");
        // make sure shutdown nimbus
        JStormUtils.halt_process(0, "!!!Shutdown!!!");
    }

    public void uploadMetrics(String topologyId, TopologyMetric topologyMetric) throws TException {
        this.serviceHandler.uploadTopologyMetrics(topologyId, topologyMetric);
    }

    public Map<String, Long> registerMetrics(String topologyId, Set<String> names) throws TException {
        return this.serviceHandler.registerMetrics(topologyId, names);
    }
}
