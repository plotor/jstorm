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
 * JStorm的主节点上运行着nimbus的守护进程，主要负责与ZK通信，分发代码，给集群中的从节点分配任务，监视集群状态等等。
 * 此外nimbus需要维护的所有状态都会存储在ZK中，JStorm为了减少对ZK的访问次数做了一些缓存
 *
 * NimbusServer workflow:
 * 1. cleanup interrupted topology delete /storm-local-dir/nimbus/topologyid/stormdis delete /storm-zk-root/storms/topologyid
 * 2. set /storm-zk-root/storms/topology stats as run
 * 3. start one thread, every nimbus.monitor.freq.secs set /storm-zk-root/storms/ all topology as monitor.
 * when the topology's status is monitor, nimbus would reassign workers
 * 4. start one thread, every nimbus.cleanup.inbox.freq.secs cleanup useless jar
 *
 * 清除中断的Topology（删除本地目录/storm-local-dir/nimbus/topologyid/stormdis和zk上的/storm-zk-root/storms/topologyid）
 * 设置/storm-zk-root/storms/topology中的Topology状态为active
 * 启动一个monitor线程，每nimbus.monitor.reeq.secs检查/storm-zk-root/storms中所有Topology状态，如果Topology中有task是不活动的则讲Topology状态转换为monitor（这个状态下nimbus会重新分配workers）
 * 启动一个cleaner线程，每nimubs.cleanup.inbox.freq.secs清除无用的jar
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

        // 加载集群配置信息
        Map config = Utils.readStormConfig();

        JStormServerUtils.startTaobaoJvmMonitor();

        NimbusServer instance = new NimbusServer();

        // 创建一个默认的 nimbus 启动类
        INimbus iNimbus = new DefaultInimbus();

        // 启动 nimbus 服务
        instance.launchServer(config, iNimbus);
    }

    /**
     * 创建当前 JVM 进程对应的目录：${storm.local.dir}/nimbus/pids/${pid}
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
     * @param conf
     * @param inimbus
     */
    @SuppressWarnings("rawtypes")
    private void launchServer(final Map conf, INimbus inimbus) {
        LOG.info("Begin to start nimbus with conf " + conf);

        try {
            // 1. 验证当前为分布式运行模式
            StormConfig.validate_distributed_mode(conf);

            // 2. 创建当前 JVM 进程对应的目录：${storm.local.dir}/nimbus/pids/${pid}
            this.createPid(conf);

            // 3. 注册 shutdown hook 方法，用于执行在 JVM 进程终止时的清理逻辑
            this.initShutdownHook();

            // 4. 模板方法
            inimbus.prepare(conf, StormConfig.masterInimbus(conf));

            // 5. 基于 conf 创建 NimbusData 对象
            data = this.createNimbusData(conf, inimbus);

            // 6. 注册一个 follower 线程
            this.initFollowerThread(conf);

            // 7.
            int port = ConfigExtension.getNimbusDeamonHttpserverPort(conf);
            hs = new Httpserver(port, conf);
            hs.start();

            // 8. 如果集群运行在 YARN 上，则初始化容器心跳线程
            this.initContainerHBThread(conf);

            serviceHandler = new ServiceHandler(data);

            // 9. 初始化并启动 thrift 服务
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
        data = createNimbusData(conf, inimbus);
        init(conf);
        serviceHandler = new ServiceHandler(data);
        return serviceHandler;
    }

    /**
     * 主要作用是得知是否能在资源管理器（yarn）上运行jstorm集群，如果可以的话，则需要创建一个新的线程用于处理。
     * (其实这里使用容器的目的是可以在一个物理集群上运行多个不一样的逻辑集群甚至多个JStorm集群，能动态调整逻辑集群分到的资源，
     * 此外，资源管理器能提供非常强的可扩展性)。容器线程会被添加到NimbusServer中，后续使用到的时候再详细讲解。
     * 这个容器线程也是守护线程，且马上就会启动，这个线程的run方法里面包含两个处理：
     *
     * 1. handleWriteDir：这个方法的主要作用是清除掉容器上的过期心跳信息，准确的说，如果JStorm集群容器目录下的心跳信息大于10，则需要清除（从最老的开始）。
     *   2. handlReadDir：这里主要是用于维护本地是否能接受到集群上的hb信息，如果多次超时则要抛出异常。
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

        // 拓扑分配
        initTopologyAssign();

        // 状态更新
        initTopologyStatus();

        // 清除函数
        initCleaner(conf);

        initMetricRunnable();

        if (!data.isLocalMode()) {
            initMonitor(conf);
            //mkRefreshConfThread(data);
        }
    }

    private NimbusData createNimbusData(Map conf, INimbus inimbus) throws Exception {
        // Callback callback=new TimerCallBack();
        // StormTimer timer=Timer.mkTimerTimer(callback);
        return new NimbusData(conf, inimbus);
    }

    private void initTopologyAssign() {
        topologyAssign = TopologyAssign.getInstance();
        topologyAssign.init(data);
    }

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
        Integer thrift_port = JStormUtils.parseInt(conf.get(Config.NIMBUS_THRIFT_PORT)); // ${nimbus.thrift.port}
        TNonblockingServerSocket socket = new TNonblockingServerSocket(thrift_port);

        Integer maxReadBufSize = JStormUtils.parseInt(conf.get(Config.NIMBUS_THRIFT_MAX_BUFFER_SIZE));

        THsHaServer.Args args = new THsHaServer.Args(socket);
        args.workerThreads(ServiceHandler.THREAD_NUM);
        args.protocolFactory(new TBinaryProtocol.Factory(false, true, maxReadBufSize, -1));

        args.processor(new Nimbus.Processor<Iface>(serviceHandler));
        args.maxReadBufferBytes = maxReadBufSize;

        thriftServer = new THsHaServer(args);

        LOG.info("Successfully started nimbus: started Thrift server...");
        thriftServer.serve();
    }

    /**
     * leader-follower 线程模型
     *
     * @param conf
     */
    @SuppressWarnings("unused")
    private void initFollowerThread(Map conf) {
        // when this nimbus become leader, we will execute this callback, to init some necessary data/thread
        Callback leaderCallback = new Callback() {
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
            public void run() {
                NimbusServer.this.cleanup();
            }
        });
    }

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
