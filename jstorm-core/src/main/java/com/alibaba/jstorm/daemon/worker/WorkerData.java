
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

package com.alibaba.jstorm.daemon.worker;

import backtype.storm.Config;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.StreamInfo;
import backtype.storm.messaging.ControlMessage;
import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.IContext;
import backtype.storm.messaging.TransportFactory;
import backtype.storm.scheduler.WorkerSlot;
import backtype.storm.serialization.KryoByteBufferSerializer;
import backtype.storm.serialization.KryoTupleDeserializer;
import backtype.storm.serialization.KryoTupleSerializer;
import backtype.storm.task.WorkerTopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.DisruptorQueue;
import backtype.storm.utils.ThriftTopologyUtils;
import backtype.storm.utils.Utils;
import backtype.storm.utils.WorkerClassLoader;
import com.alibaba.jstorm.callback.AsyncLoopDefaultKill;
import com.alibaba.jstorm.callback.AsyncLoopThread;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.Cluster;
import com.alibaba.jstorm.cluster.ClusterState;
import com.alibaba.jstorm.cluster.Common;
import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.common.metric.AsmGauge;
import com.alibaba.jstorm.common.metric.AsmMetric;
import com.alibaba.jstorm.common.metric.FullGcGauge;
import com.alibaba.jstorm.common.metric.QueueGauge;
import com.alibaba.jstorm.daemon.nimbus.StatusType;
import com.alibaba.jstorm.daemon.worker.timer.TimerTrigger;
import com.alibaba.jstorm.metric.JStormHealthReporter;
import com.alibaba.jstorm.metric.JStormMetrics;
import com.alibaba.jstorm.metric.JStormMetricsReporter;
import com.alibaba.jstorm.metric.MetricDef;
import com.alibaba.jstorm.metric.MetricType;
import com.alibaba.jstorm.metric.MetricUtils;
import com.alibaba.jstorm.schedule.Assignment;
import static com.alibaba.jstorm.schedule.Assignment.AssignmentType;
import com.alibaba.jstorm.schedule.default_assign.ResourceWorkerSlot;
import com.alibaba.jstorm.task.TaskShutdownDaemon;
import com.alibaba.jstorm.utils.JStormServerUtils;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.LogUtils;
import com.alibaba.jstorm.zk.ZkTool;
import com.codahale.metrics.Gauge;
import com.lmax.disruptor.TimeoutBlockingWaitStrategy;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.net.URL;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Worker 相关数据
 */
public class WorkerData {

    private static Logger LOG = LoggerFactory.getLogger(WorkerData.class);

    public static final int THREAD_POOL_NUM = 4;
    private ScheduledExecutorService threadPool;

    // system configuration
    private Map<Object, Object> conf;
    // worker configuration
    private Map<Object, Object> stormConf;

    // message queue
    private IContext context;

    private final String topologyId;
    private final String supervisorId;
    private final Integer port;
    private final String workerId;

    // worker status :active/shutdown
    private AtomicBoolean shutdown;
    private AtomicBoolean monitorEnable;

    // Topology status
    private StatusType topologyStatus;

    // ZK interface
    private ClusterState zkClusterState;  // Cluster 状态信息
    private StormClusterState zkCluster;

    // running taskId list in current worker
    private Set<Integer> taskIds;

    // 管理与其他 worker 的连接对象
    // connection to other workers <NodePort, ZMQConnection>
    private ConcurrentHashMap<WorkerSlot, IConnection> nodePortToSocket;

    // <taskId, NodePort>
    private ConcurrentHashMap<Integer, WorkerSlot> taskToNodePort;

    private ConcurrentSkipListSet<ResourceWorkerSlot> workerToResource;

    private volatile Set<Integer> outboundTasks;
    private Set<Integer> localNodeTasks = new HashSet<>();

    private ConcurrentHashMap<Integer, DisruptorQueue> innerTaskTransfer;
    private ConcurrentHashMap<Integer, DisruptorQueue> controlQueues;
    private ConcurrentHashMap<Integer, DisruptorQueue> deserializeQueues;

    /** [task_id, component_id] */
    private ConcurrentHashMap<Integer, String> tasksToComponent;
    /** [component_id, List<task_id>] */
    private ConcurrentHashMap<String, List<Integer>> componentToSortedTasks;

    private Map<String, Object> defaultResources;
    private Map<String, Object> userResources;
    private Map<String, Object> executorData;
    private Map registeredMetrics;

    // 从 jar 包反序列化而来的 topology (不包含 ackers)
    // raw topology is deserialized from local jar which doesn't contain ackers
    private StormTopology rawTopology;

    // 允许在当前 worker 中的 topology (包含 ackers)
    // sys topology is the running topology in the worker which contains ackers
    private StormTopology sysTopology;

    private ContextMaker contextMaker;

    // 关闭 worker 的入口
    // shutdown worker entrance
    private final AsyncLoopDefaultKill workHalt = new AsyncLoopDefaultKill();

    // 用来发送 tuple 的队列
    // sending tuple's queue
    private DisruptorQueue transferCtrlQueue;

    // 被终止的 task 列表
    private List<TaskShutdownDaemon> shutdownTasks;

    // 需要向外进行输出的任务状态
    private ConcurrentHashMap<Integer, Boolean> outTaskStatus; // true => active

    private FlusherPool flusherPool;

    // 上次进行任务分配的时间戳
    private volatile Long assignmentTS; // assignment timeStamp. last update time of assignment
    // 上次进行任务分配的分配类型
    private volatile AssignmentType assignmentType;

    private IConnection recvConnection;

    private JStormMetricsReporter metricReporter;

    // 异步汇报当前 Worker 健康状态的线程
    @SuppressWarnings("unused")
    private AsyncLoopThread healthReporterThread;

    // 标记 worker 正在初始化本地连接信息
    private AtomicBoolean workerInitConnectionStatus;

    // tuple 序列化
    private AtomicReference<KryoTupleSerializer> atomKryoSerializer = new AtomicReference<>();
    // tuple 反序列化
    private AtomicReference<KryoTupleDeserializer> atomKryoDeserializer = new AtomicReference<>();

    // 对于需要动态更新的数据或配置进行更新
    // the data/config which need dynamic update
    private UpdateListener updateListener;

    protected List<AsyncLoopThread> deserializeThreads = new ArrayList<>();
    protected List<AsyncLoopThread> serializeThreads = new ArrayList<>();

    /**
     * @param conf 集群运行配置信息
     * @param context 消息上下文，用来接收外部的消息
     * @param topologyId 当前 worker 运行任务所属的 topology
     * @param supervisorId 当前 worker 隶属的 supervisor
     * @param port 用来与外界进行通信的端口
     * @param workerId 标识当前 worker 的 id
     * @param jarPath topology 对应的 jar 文件路径
     * @throws Exception
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public WorkerData(Map conf, IContext context, String topologyId, String supervisorId,
                      int port, String workerId, String jarPath) throws Exception {
        this.conf = conf;
        this.context = context;
        this.topologyId = topologyId;
        this.supervisorId = supervisorId;
        this.port = port;
        this.workerId = workerId;

        this.shutdown = new AtomicBoolean(false);

        this.monitorEnable = new AtomicBoolean(true);
        this.topologyStatus = null;

        this.workerInitConnectionStatus = new AtomicBoolean(false);

        if (StormConfig.cluster_mode(conf).equals("distributed")) {
            // ${storm.local.dir}/workers/${worker_id}/pids
            String pidDir = StormConfig.worker_pids_root(conf, workerId);
            JStormServerUtils.createPid(pidDir);
        }

        // create zk interface
        this.zkClusterState = ZkTool.mk_distributed_cluster_state(conf);
        this.zkCluster = Cluster.mk_storm_cluster_state(zkClusterState);

        // 加载 topology 配置信息
        Map rawConf = StormConfig.read_supervisor_topology_conf(conf, topologyId);
        this.stormConf = new HashMap<>();
        this.stormConf.putAll(conf); // 集群配置
        this.stormConf.putAll(rawConf); // topology 配置

        // init topology.debug and other debug args
        JStormDebugger.update(stormConf);
        // 注册动态更新监听器
        this.registerUpdateListeners();

        JStormMetrics.setHistogramValueSize(ConfigExtension.getTopologyHistogramSize(stormConf));
        JStormMetrics.setTopologyId(topologyId);
        JStormMetrics.setPort(port);
        JStormMetrics.setDebug(ConfigExtension.isEnableMetricDebug(stormConf));
        JStormMetrics.enabled = ConfigExtension.isEnableMetrics(stormConf);
        JStormMetrics.enableStreamMetrics = ConfigExtension.isEnableStreamMetrics(stormConf);
        JStormMetrics.addDebugMetrics(ConfigExtension.getDebugMetricNames(stormConf));
        AsmMetric.setSampleRate(ConfigExtension.getMetricSampleRate(stormConf));

        ConfigExtension.setLocalSupervisorId(stormConf, this.supervisorId);
        ConfigExtension.setLocalWorkerId(stormConf, this.workerId);
        ConfigExtension.setLocalWorkerPort(stormConf, port);
        ControlMessage.setPort(port);

        JStormMetrics.registerWorkerTopologyMetric(
                JStormMetrics.workerMetricName(MetricDef.CPU_USED_RATIO, MetricType.GAUGE),
                new AsmGauge(new Gauge<Double>() {
                    @Override
                    public Double getValue() {
                        return JStormUtils.getCpuUsage();
                    }
                }));

        JStormMetrics.registerWorkerTopologyMetric(JStormMetrics.workerMetricName(MetricDef.HEAP_MEMORY_USED, MetricType.GAUGE),
                new AsmGauge(new Gauge<Double>() {
                    @Override
                    public Double getValue() {
                        return JStormUtils.getJVMHeapMemory();
                    }
                }));

        JStormMetrics.registerWorkerTopologyMetric(JStormMetrics.workerMetricName(MetricDef.MEMORY_USED, MetricType.GAUGE),
                new AsmGauge(new Gauge<Double>() {
                    @Override
                    public Double getValue() {
                        return JStormUtils.getMemUsage();
                    }
                }));

        JStormMetrics.registerWorkerTopologyMetric(JStormMetrics.workerMetricName(MetricDef.FULL_GC, MetricType.GAUGE),
                new AsmGauge(new FullGcGauge()) {
                    @Override
                    public AsmMetric clone() {
                        AsmMetric metric = new AsmGauge((Gauge<Double>) Utils.newInstance(this.gauge.getClass().getName()));
                        metric.setMetricName(this.getMetricName());
                        return metric;
                    }
                });

        JStormMetrics.registerWorkerMetric(MetricUtils.workerMetricName("GCTime", MetricType.GAUGE),
                new AsmGauge(new Gauge<Double>() {
                    final List<GarbageCollectorMXBean> gcBeans = ManagementFactory.getGarbageCollectorMXBeans();
                    double lastGcTime = this.getGcTime();

                    @Override
                    public Double getValue() {
                        double newGcTime = this.getGcTime();
                        double delta = newGcTime - lastGcTime;
                        lastGcTime = newGcTime;
                        return delta;
                    }

                    double getGcTime() {
                        double sum = 0;
                        for (GarbageCollectorMXBean gcBean : gcBeans) {
                            sum += gcBean.getCollectionTime();

                        }
                        // convert time to us
                        return sum * 1000;
                    }
                })
        );

        JStormMetrics.registerWorkerMetric(MetricUtils.workerMetricName("GCCount", MetricType.GAUGE),
                new AsmGauge(new Gauge<Double>() {
                    final List<GarbageCollectorMXBean> gcBeans = ManagementFactory.getGarbageCollectorMXBeans();
                    double lastGcNum = this.getGcNum();

                    @Override
                    public Double getValue() {
                        double newGcNum = this.getGcNum();
                        double delta = newGcNum - lastGcNum;
                        lastGcNum = newGcNum;
                        return delta;
                    }

                    double getGcNum() {
                        double cnt = 0;
                        for (GarbageCollectorMXBean gcBean : gcBeans) {
                            cnt += gcBean.getCollectionCount();
                        }
                        return cnt;
                    }
                })
        );

        LOG.info("Worker Configuration " + stormConf);
        try {
            boolean enableClassloader = ConfigExtension.isEnableTopologyClassLoader(stormConf);
            boolean enableDebugClassloader = ConfigExtension.isEnableClassloaderDebug(stormConf);

            if (jarPath == null && enableClassloader && !conf.get(Config.STORM_CLUSTER_MODE).equals("local")) {
                LOG.error("classloader is enabled, but no app jar was found!");
                throw new InvalidParameterException();
            }

            URL[] urlArray = new URL[0];
            if (jarPath != null) {
                String[] paths = jarPath.split(":");
                Set<URL> urls = new HashSet<>();
                for (String path : paths) {
                    if (StringUtils.isBlank(path)) {
                        continue;
                    }
                    URL url = new URL("File:" + path);
                    urls.add(url);
                }
                urlArray = urls.toArray(new URL[0]);
            }

            WorkerClassLoader.mkInstance(urlArray,
                    ClassLoader.getSystemClassLoader(), ClassLoader.getSystemClassLoader().getParent(),
                    enableClassloader, enableDebugClassloader);
        } catch (Exception e) {
            LOG.error("init jarClassLoader error!", e);
            throw new InvalidParameterException();
        }

        // register kryo serializer for HeapByteBuffer, it's a hack as HeapByteBuffer is non-public
        Config.registerSerialization(stormConf, "java.nio.HeapByteBuffer", KryoByteBufferSerializer.class);

        if (this.context == null) {
            // default is NettyContext
            this.context = TransportFactory.makeContext(stormConf);
        }

        int queueSize = JStormUtils.parseInt(stormConf.get(Config.TOPOLOGY_CTRL_BUFFER_SIZE), 256);
        long timeout = JStormUtils.parseLong(stormConf.get(Config.TOPOLOGY_DISRUPTOR_WAIT_TIMEOUT), 10);
        WaitStrategy waitStrategy = new TimeoutBlockingWaitStrategy(timeout, TimeUnit.MILLISECONDS);
        this.transferCtrlQueue = DisruptorQueue.mkInstance(
                "TotalTransfer", ProducerType.MULTI, queueSize, waitStrategy, false, 0, 0);

        //metric for transferCtrlQueue
        QueueGauge transferCtrlGauge = new QueueGauge(transferCtrlQueue, MetricDef.SEND_QUEUE);
        JStormMetrics.registerWorkerMetric(JStormMetrics.workerMetricName(
                MetricDef.SEND_QUEUE, MetricType.GAUGE), new AsmGauge(transferCtrlGauge));

        this.nodePortToSocket = new ConcurrentHashMap<>();
        this.taskToNodePort = new ConcurrentHashMap<>();
        this.workerToResource = new ConcurrentSkipListSet<>();
        this.innerTaskTransfer = new ConcurrentHashMap<>();
        this.controlQueues = new ConcurrentHashMap<>();
        this.deserializeQueues = new ConcurrentHashMap<>();
        this.tasksToComponent = new ConcurrentHashMap<>();
        this.componentToSortedTasks = new ConcurrentHashMap<>();

        Assignment assignment = zkCluster.assignment_info(this.topologyId, null);
        if (assignment == null) {
            String errMsg = "Failed to get assignment of " + this.topologyId;
            LOG.error(errMsg);
            throw new RuntimeException(errMsg);
        }
        workerToResource.addAll(assignment.getWorkers());

        // get current worker's task list
        this.taskIds = assignment.getCurrentWorkerTasks(this.supervisorId, port);
        if (taskIds.size() == 0) {
            throw new RuntimeException("Current worker doesn't contain any tasks!");
        }
        LOG.info("Current worker taskList:" + taskIds);

        // 加载 topology 的 StormTopology 对象
        rawTopology = StormConfig.read_supervisor_topology_code(conf, topologyId);
        // 添加 system topology
        sysTopology = Common.system_topology(stormConf, rawTopology);

        this.generateMaps();

        contextMaker = new ContextMaker(this);

        outTaskStatus = new ConcurrentHashMap<>();

        int minPoolSize = taskIds.size() > 5 ? 5 : taskIds.size();
        int maxPoolSize = 2 * taskIds.size();

        threadPool = Executors.newScheduledThreadPool(THREAD_POOL_NUM);
        TimerTrigger.setScheduledExecutorService(threadPool);

        // ${worker.flush.pool.min.size}
        if (ConfigExtension.getWorkerFlushPoolMinSize(stormConf) != null) {
            minPoolSize = ConfigExtension.getWorkerFlushPoolMinSize(stormConf);
        }
        // ${worker.flush.pool.max.size}
        if (ConfigExtension.getWorkerFlushPoolMaxSize(stormConf) != null) {
            maxPoolSize = ConfigExtension.getWorkerFlushPoolMaxSize(stormConf);
        }
        if (minPoolSize > maxPoolSize) {
            LOG.error("Irrational flusher pool parameter configuration");
        }
        flusherPool = new FlusherPool(minPoolSize, maxPoolSize, 30, TimeUnit.SECONDS);
        Flusher.setFlusherPool(flusherPool);

        if (!StormConfig.local_mode(stormConf)) {
            healthReporterThread = new AsyncLoopThread(new JStormHealthReporter(this));
        }

        try {
            assignmentTS = StormConfig.read_supervisor_topology_timestamp(conf, topologyId);
        } catch (FileNotFoundException e) {
            assignmentTS = System.currentTimeMillis();
        }

        outboundTasks = new HashSet<>();

        // kryo
        this.updateKryoSerializer();

        LOG.info("Successfully created WorkerData");
    }

    private void registerUpdateListeners() {
        updateListener = new UpdateListener();

        //disable/enable metrics on the fly
        updateListener.registerUpdater(new UpdateListener.IUpdater() {
            @Override
            public void update(Map conf) {
                metricReporter.updateMetricConfig(WorkerData.this.combineConf(conf));
            }
        });

        //disable/enable topology debug on the fly
        updateListener.registerUpdater(new UpdateListener.IUpdater() {
            @Override
            public void update(Map conf) {
                JStormDebugger.update(WorkerData.this.combineConf(conf));
            }
        });

        //change log level on the fly
        updateListener.registerUpdater(new UpdateListener.IUpdater() {
            @Override
            public void update(Map conf) {
                LogUtils.update(WorkerData.this.combineConf(conf));
            }
        });
    }

    private Map<Object, Object> combineConf(Map newConf) {
        Map<Object, Object> ret = new HashMap<>(this.conf);
        ret.putAll(newConf);
        return ret;
    }

    public UpdateListener getUpdateListener() {
        return updateListener;
    }

    // create kryo serializer
    public void updateKryoSerializer() {
        WorkerTopologyContext workerTopologyContext = contextMaker.makeWorkerTopologyContext(sysTopology);
        KryoTupleDeserializer kryoTupleDeserializer = new KryoTupleDeserializer(stormConf, workerTopologyContext, workerTopologyContext.getRawTopology());
        KryoTupleSerializer kryoTupleSerializer = new KryoTupleSerializer(stormConf, workerTopologyContext.getRawTopology());

        atomKryoDeserializer.getAndSet(kryoTupleDeserializer);
        atomKryoSerializer.getAndSet(kryoTupleSerializer);
    }

    /**
     * private ConcurrentHashMap<Integer, WorkerSlot> taskToNodePort;
     * private HashMap<Integer, String> tasksToComponent;
     * private Map<String, List<Integer>> componentToSortedTasks;
     * private Map<String, Map<String, Fields>> componentToStreamToFields;
     * private Map<String, Object> defaultResources;
     * private Map<String, Object> userResources;
     * private Map<String, Object> executorData;
     * private Map registeredMetrics;
     *
     * @throws Exception
     */
    private void generateMaps() throws Exception {
        // 获取当前 topology 所有的组件 ID 与 taskId 的正向与反向映射关系
        this.updateTaskComponentMap();
        this.defaultResources = new HashMap<>();
        this.userResources = new HashMap<>();
        this.executorData = new HashMap<>();
        this.registeredMetrics = new HashMap();
    }

    public Map<Object, Object> getRawConf() {
        return conf;
    }

    public AtomicBoolean getShutdown() {
        return shutdown;
    }

    public StatusType getTopologyStatus() {
        return topologyStatus;
    }

    public void setTopologyStatus(StatusType topologyStatus) {
        this.topologyStatus = topologyStatus;
    }

    public Map<Object, Object> getConf() {
        return conf;
    }

    public Map<Object, Object> getStormConf() {
        return stormConf;
    }

    public IContext getContext() {
        return context;
    }

    public String getTopologyId() {
        return topologyId;
    }

    public String getSupervisorId() {
        return supervisorId;
    }

    public Integer getPort() {
        return port;
    }

    public String getWorkerId() {
        return workerId;
    }

    public ClusterState getZkClusterState() {
        return zkClusterState;
    }

    public StormClusterState getZkCluster() {
        return zkCluster;
    }

    public Set<Integer> getTaskIds() {
        return taskIds;
    }

    public ConcurrentHashMap<WorkerSlot, IConnection> getNodePortToSocket() {
        return nodePortToSocket;
    }

    public ConcurrentHashMap<Integer, WorkerSlot> getTaskToNodePort() {
        return taskToNodePort;
    }

    public void updateWorkerToResource(Set<ResourceWorkerSlot> workers) {
        workerToResource.removeAll(workers);
        workerToResource.addAll(workers);
    }

    public ConcurrentHashMap<Integer, DisruptorQueue> getInnerTaskTransfer() {
        return innerTaskTransfer;
    }

    public ConcurrentHashMap<Integer, DisruptorQueue> getDeserializeQueues() {
        return deserializeQueues;
    }

    public ConcurrentHashMap<Integer, DisruptorQueue> getControlQueues() {
        return controlQueues;
    }

    public ConcurrentHashMap<Integer, String> getTasksToComponent() {
        return tasksToComponent;
    }

    public StormTopology getRawTopology() {
        return rawTopology;
    }

    public StormTopology getSysTopology() {
        return sysTopology;
    }

    public ContextMaker getContextMaker() {
        return contextMaker;
    }

    public AsyncLoopDefaultKill getWorkHalt() {
        return workHalt;
    }

    public DisruptorQueue getTransferCtrlQueue() {
        return transferCtrlQueue;
    }

    // public LinkedBlockingQueue<TransferData> getTransferCtrlQueue() {
    // return transferCtrlQueue;
    // }

    public Map<String, List<Integer>> getComponentToSortedTasks() {
        return componentToSortedTasks;
    }

    public Map<String, Object> getDefaultResources() {
        return defaultResources;
    }

    public Map<String, Object> getUserResources() {
        return userResources;
    }

    public Map<String, Object> getExecutorData() {
        return executorData;
    }

    public Map getRegisteredMetrics() {
        return registeredMetrics;
    }

    public List<TaskShutdownDaemon> getShutdownTasks() {
        return shutdownTasks;
    }

    public void setShutdownTasks(List<TaskShutdownDaemon> shutdownTasks) {
        this.shutdownTasks = shutdownTasks;
    }

    public void addShutdownTask(TaskShutdownDaemon shutdownTask) {
        this.shutdownTasks.add(shutdownTask);
    }

    public List<TaskShutdownDaemon> getShutdownDaemonbyTaskIds(Set<Integer> taskIds) {
        List<TaskShutdownDaemon> ret = new ArrayList<>();
        for (TaskShutdownDaemon shutdown : shutdownTasks) {
            if (taskIds.contains(shutdown.getTaskId())) {
                ret.add(shutdown);
            }
        }
        return ret;
    }

    public AtomicBoolean getWorkerInitConnectionStatus() {
        return workerInitConnectionStatus;
    }

    public void initOutboundTaskStatus(Set<Integer> outboundTasks) {
        for (Integer taskId : outboundTasks) {
            outTaskStatus.put(taskId, false);
        }
    }

    public Map<Integer, Boolean> getOutboundTaskStatus() {
        return outTaskStatus;
    }

    public void addOutboundTaskStatusIfAbsent(Integer taskId) {
        outTaskStatus.putIfAbsent(taskId, false);
    }

    public void removeOutboundTaskStatus(Integer taskId) {
        outTaskStatus.remove(taskId);
    }

    public void updateOutboundTaskStatus(Integer taskId, boolean isActive) {
        outTaskStatus.put(taskId, isActive);
    }

    public boolean isOutboundTaskActive(Integer taskId) {
        return outTaskStatus.get(taskId) != null ? outTaskStatus.get(taskId) : false;
    }

    public ScheduledExecutorService getThreadPool() {
        return threadPool;
    }

    public void setAssignmentTs(Long time) {
        assignmentTS = time;
    }

    public Long getAssignmentTs() {
        return assignmentTS;
    }

    public void setAssignmentType(AssignmentType type) {
        this.assignmentType = type;
    }

    public AssignmentType getAssignmentType() {
        return assignmentType;
    }

    public void updateWorkerData(Assignment assignment) throws Exception {
        // 更新当前 worker 的 taskId 列表
        this.updateTaskIds(assignment);
        // 更新当前 topology 所有的组件 ID 与 taskId 的正向与反向映射关系
        this.updateTaskComponentMap();
        // 更新当前 worker 对应的 topology 对象（原生、系统）
        this.updateStormTopology();
    }

    public void updateTaskIds(Assignment assignment) {
        this.taskIds.clear();
        this.taskIds.addAll(assignment.getCurrentWorkerTasks(supervisorId, port));
    }

    public Set<Integer> getLocalNodeTasks() {
        return localNodeTasks;
    }

    public void setLocalNodeTasks(Set<Integer> localNodeTasks) {
        this.localNodeTasks = localNodeTasks;
    }

    public void setOutboundTasks(Set<Integer> outboundTasks) {
        this.outboundTasks = outboundTasks;
    }

    public Set<Integer> getOutboundTasks() {
        return outboundTasks;
    }

    /**
     * 更新当前 topology 所有的组件 ID 与 taskId 的正向与反向映射关系
     *
     * @throws Exception
     */
    private void updateTaskComponentMap() throws Exception {
        // 获取当前 topology 对应的所有 [task_id, component_id]
        Map<Integer, String> tmp = Common.getTaskToComponent(Cluster.get_all_taskInfo(zkCluster, topologyId));

        this.tasksToComponent.putAll(tmp);
        LOG.info("Updated tasksToComponentMap:" + tasksToComponent);

        this.componentToSortedTasks.putAll(JStormUtils.reverse_map(tmp));
        for (Map.Entry<String, List<Integer>> entry : componentToSortedTasks.entrySet()) {
            List<Integer> tasks = entry.getValue();
            Collections.sort(tasks);
        }
    }

    private void updateStormTopology() {
        StormTopology rawTmp, sysTmp;
        try {
            rawTmp = StormConfig.read_supervisor_topology_code(conf, topologyId);
            sysTmp = Common.system_topology(stormConf, rawTopology);
        } catch (IOException e) {
            LOG.error("Failed to read supervisor topology code for " + topologyId, e);
            return;
        } catch (InvalidTopologyException e) {
            LOG.error("Failed to update sysTopology for " + topologyId, e);
            return;
        }

        this.updateTopology(rawTopology, rawTmp);
        this.updateTopology(sysTopology, sysTmp);
    }

    private void updateTopology(StormTopology oldTopology, StormTopology newTopology) {
        oldTopology.set_bolts(newTopology.get_bolts());
        oldTopology.set_spouts(newTopology.get_spouts());
        oldTopology.set_state_spouts(newTopology.get_state_spouts());
    }

    public AtomicBoolean getMonitorEnable() {
        return monitorEnable;
    }

    public IConnection getRecvConnection() {
        return recvConnection;
    }

    public void setRecvConnection(IConnection recvConnection) {
        this.recvConnection = recvConnection;
    }

    public JStormMetricsReporter getMetricsReporter() {
        return metricReporter;
    }

    public void setMetricsReporter(JStormMetricsReporter metricReporter) {
        this.metricReporter = metricReporter;
    }

    /**
     * 获取当前 topology 各个组件所输出的流信息：[component_id, [stream_id, fields]]
     *
     * @param topology
     * @return
     */
    public HashMap<String, Map<String, Fields>> generateComponentToStreamToFields(StormTopology topology) {
        HashMap<String, Map<String, Fields>> componentToStreamToFields = new HashMap<>();

        // 获取当前 topology 的组件 ID 列表
        Set<String> components = ThriftTopologyUtils.getComponentIds(topology);
        for (String component : components) {

            Map<String, Fields> streamToFieldsMap = new HashMap<>();

            // 获取当前组件输出的所有流信息
            Map<String, StreamInfo> streamInfoMap = ThriftTopologyUtils.getComponentCommon(topology, component).get_streams();
            for (Map.Entry<String, StreamInfo> entry : streamInfoMap.entrySet()) {
                String streamId = entry.getKey();
                StreamInfo streamInfo = entry.getValue();
                streamToFieldsMap.put(streamId, new Fields(streamInfo.get_output_fields()));
            }

            componentToStreamToFields.put(component, streamToFieldsMap);
        }
        return componentToStreamToFields;
    }

    public AtomicReference<KryoTupleDeserializer> getAtomKryoDeserializer() {
        return atomKryoDeserializer;
    }

    public AtomicReference<KryoTupleSerializer> getAtomKryoSerializer() {
        return atomKryoSerializer;
    }

    protected List<AsyncLoopThread> setDeserializeThreads() {
        WorkerTopologyContext workerTopologyContext = contextMaker.makeWorkerTopologyContext(sysTopology);
        int tasksNum = shutdownTasks.size();
        double workerRatio = ConfigExtension.getWorkerDeserializeThreadRatio(stormConf);
        int workerDeserThreadNum = Utils.getInt(Math.ceil(workerRatio * tasksNum));
        if (workerDeserThreadNum > 0 && tasksNum > 0) {
            double average = tasksNum / (double) workerDeserThreadNum;
            for (int i = 0; i < workerDeserThreadNum; i++) {
                int startRunTaskIndex = Utils.getInt(Math.rint(average * i));
                deserializeThreads.add(new AsyncLoopThread(new WorkerDeserializeRunnable(
                        shutdownTasks, stormConf, workerTopologyContext, startRunTaskIndex, i)));
            }
        }
        return deserializeThreads;
    }

    protected List<AsyncLoopThread> setSerializeThreads() {
        WorkerTopologyContext workerTopologyContext = contextMaker.makeWorkerTopologyContext(sysTopology);
        int tasksNum = shutdownTasks.size();
        double workerRatio = ConfigExtension.getWorkerSerializeThreadRatio(stormConf);
        int workerSerialThreadNum = Utils.getInt(Math.ceil(workerRatio * tasksNum));
        if (workerSerialThreadNum > 0 && tasksNum > 0) {
            double average = tasksNum / (double) workerSerialThreadNum;
            for (int i = 0; i < workerSerialThreadNum; i++) {
                int startRunTaskIndex = Utils.getInt(Math.rint(average * i));
                serializeThreads.add(new AsyncLoopThread(new WorkerSerializeRunnable(
                        shutdownTasks, stormConf, workerTopologyContext, startRunTaskIndex, i)));
            }
        }
        return serializeThreads;
    }

    public FlusherPool getFlusherPool() {
        return flusherPool;
    }
}
