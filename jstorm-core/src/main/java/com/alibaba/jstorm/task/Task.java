/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.jstorm.task;

import backtype.storm.Config;
import backtype.storm.hooks.ITaskHook;
import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.IContext;
import backtype.storm.scheduler.WorkerSlot;
import backtype.storm.serialization.KryoTupleSerializer;
import backtype.storm.spout.ISpout;
import backtype.storm.task.IBolt;
import backtype.storm.task.TopologyContext;
import backtype.storm.utils.DisruptorQueue;
import backtype.storm.utils.Utils;
import backtype.storm.utils.WorkerClassLoader;
import clojure.lang.Atom;
import com.alibaba.jstorm.callback.AsyncLoopDefaultKill;
import com.alibaba.jstorm.callback.AsyncLoopThread;
import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.Common;
import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.daemon.worker.WorkerData;
import com.alibaba.jstorm.daemon.worker.timer.TaskHeartbeatTrigger;
import com.alibaba.jstorm.schedule.Assignment.AssignmentType;
import com.alibaba.jstorm.task.comm.TaskSendTargets;
import com.alibaba.jstorm.task.comm.UnanchoredSend;
import com.alibaba.jstorm.task.error.ITaskReportErr;
import com.alibaba.jstorm.task.error.TaskReportError;
import com.alibaba.jstorm.task.error.TaskReportErrorAndDie;
import com.alibaba.jstorm.task.execute.BaseExecutors;
import com.alibaba.jstorm.task.execute.BoltExecutors;
import com.alibaba.jstorm.task.execute.TopologyMasterBoltExecutors;
import com.alibaba.jstorm.task.execute.spout.MultipleThreadSpoutExecutors;
import com.alibaba.jstorm.task.execute.spout.SingleThreadSpoutExecutors;
import com.alibaba.jstorm.task.execute.spout.SpoutExecutors;
import com.alibaba.jstorm.task.group.MkGrouper;
import com.alibaba.jstorm.utils.JStormServerUtils;
import com.alibaba.jstorm.utils.JStormUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author yannian/Longda
 */
public class Task implements Runnable {
    private final static Logger LOG = LoggerFactory.getLogger(Task.class);

    private Map<Object, Object> stormConf;

    private TopologyContext topologyContext;
    private TopologyContext userContext;
    private IContext context;

    private TaskTransfer taskTransfer;
    private TaskReceiver taskReceiver;
    private Map<Integer, DisruptorQueue> innerTaskTransfer;
    private Map<Integer, DisruptorQueue> deserializeQueues;

    // task 消息传输队列集合
    private Map<Integer, DisruptorQueue> controlQueues;
    private AsyncLoopDefaultKill workHalt;

    private String topologyId;
    private Integer taskId;
    private String componentId;
    private volatile TaskStatus taskStatus;
    private Atom openOrPrepareWasCalled;

    private StormClusterState zkCluster;
    private Object taskObj;
    private TaskBaseMetric taskStats;
    private WorkerData workerData;

    private TaskSendTargets taskSendTargets;
    private TaskReportErrorAndDie reportErrorDie;

    private boolean isTaskBatchTuple;
    private TaskShutdownDaemon taskShutdownDameon;
    private ConcurrentHashMap<WorkerSlot, IConnection> nodePortToSocket;
    private ConcurrentHashMap<Integer, WorkerSlot> taskToNodePort;

    /** 当前 task 对应的执行器 */
    private BaseExecutors baseExecutors;

    @SuppressWarnings("rawtypes")
    public Task(WorkerData workerData, int taskId) throws Exception {
        openOrPrepareWasCalled = new Atom(false);

        this.workerData = workerData;
        this.topologyContext = workerData.getContextMaker().makeTopologyContext(
                workerData.getSysTopology(), taskId, openOrPrepareWasCalled);
        this.userContext = workerData.getContextMaker().makeTopologyContext(
                workerData.getRawTopology(), taskId, openOrPrepareWasCalled);
        this.taskId = taskId;
        this.componentId = topologyContext.getThisComponentId();
        topologyContext.getStormConf().putAll(Common.component_conf(topologyContext, componentId));
        this.stormConf = topologyContext.getStormConf();

        this.taskStatus = new TaskStatus();

        this.innerTaskTransfer = workerData.getInnerTaskTransfer();
        this.deserializeQueues = workerData.getDeserializeQueues();
        this.controlQueues = workerData.getControlQueues();
        this.topologyId = workerData.getTopologyId();
        this.context = workerData.getContext();
        this.workHalt = workerData.getWorkHalt();
        this.zkCluster = workerData.getZkCluster();
        this.nodePortToSocket = workerData.getNodePortToSocket();
        this.taskToNodePort = workerData.getTaskToNodePort();
        // create report error callback,
        // in fact it is storm_cluster.report-task-error
        ITaskReportErr reportError = new TaskReportError(zkCluster, topologyId, taskId);

        // report error and halt worker
        reportErrorDie = new TaskReportErrorAndDie(reportError, workHalt);
        this.taskStats = new TaskBaseMetric(topologyId, componentId, taskId);
        //register auto hook
        List<String> listHooks = Config.getTopologyAutoTaskHooks(stormConf);
        for (String hook : listHooks) {
            ITaskHook iTaskHook = (ITaskHook) Utils.newInstance(hook);
            userContext.addTaskHook(iTaskHook);
        }

        LOG.info("Begin to deserialize taskObj " + componentId + ":" + this.taskId);

        try {
            WorkerClassLoader.switchThreadContext();
            this.taskObj = Common.get_task_object(
                    topologyContext.getRawTopology(), componentId, WorkerClassLoader.getInstance());
            WorkerClassLoader.restoreThreadContext();
        } catch (Exception e) {
            if (reportErrorDie != null) {
                reportErrorDie.report(e);
            } else {
                throw e;
            }
        }
        isTaskBatchTuple = ConfigExtension.isTaskBatchTuple(stormConf);
        LOG.info("Transfer/receive in batch mode :" + isTaskBatchTuple);

        LOG.info("Loading task " + componentId + ":" + this.taskId);
    }

    private TaskSendTargets makeSendTargets() {
        String component = topologyContext.getThisComponentId();

        // get current task's output
        // <Stream_id,<component, Grouping>>
        Map<String, Map<String, MkGrouper>> streamComponentGrouper =
                Common.outbound_components(topologyContext, workerData);

        return new TaskSendTargets(stormConf, component, streamComponentGrouper, topologyContext, taskStats);
    }

    private void updateSendTargets() {
        if (taskSendTargets != null) {
            Map<String, Map<String, MkGrouper>> streamComponentGrouper =
                    Common.outbound_components(topologyContext, workerData);
            taskSendTargets.updateStreamCompGrouper(streamComponentGrouper);
        } else {
            LOG.error("taskSendTargets is null!");
        }
    }

    /**
     * send "startup" tuple to system bolt
     *
     * @return
     */
    public TaskSendTargets echoToSystemBolt() {
        // send "startup" tuple to system bolt
        List<Object> msg = new ArrayList<>();
        msg.add("startup");

        // create task receive object
        TaskSendTargets sendTargets = this.makeSendTargets();

        // 基于 disruptor
        UnanchoredSend.send(topologyContext, sendTargets, taskTransfer, Common.SYSTEM_STREAM_ID, msg);
        return sendTargets;
    }

    public boolean isSingleThread(Map conf) {
        boolean isOnePending = JStormServerUtils.isOnePending(conf);
        return isOnePending || ConfigExtension.isSpoutSingleThread(conf);
    }

    /**
     * 基于组件类型创建对应的 Executor
     *
     * @return
     */
    public BaseExecutors mkExecutor() {
        BaseExecutors baseExecutor = null;

        if (taskObj instanceof IBolt) {
            if (taskId == topologyContext.getTopologyMasterId()) {
                baseExecutor = new TopologyMasterBoltExecutors(this);
            } else {
                baseExecutor = new BoltExecutors(this);
            }
        } else if (taskObj instanceof ISpout) {
            if (this.isSingleThread(stormConf)) {
                baseExecutor = new SingleThreadSpoutExecutors(this);
            } else {
                baseExecutor = new MultipleThreadSpoutExecutors(this);
            }
        }

        return baseExecutor;
    }

    /**
     * create executor to receive tuples and run bolt/spout execute function
     * 基于组件类型创建对应的 Executor
     */
    private RunnableCallback prepareExecutor() {
        return this.mkExecutor();
    }

    public TaskReceiver mkTaskReceiver() {
        String taskName = JStormServerUtils.getName(componentId, taskId); // componentId:taskId
        taskReceiver = new TaskReceiver(this, taskId, stormConf, topologyContext, innerTaskTransfer, taskStatus, taskName);
        deserializeQueues.put(taskId, taskReceiver.getDeserializeQueue());
        return taskReceiver;
    }

    /**
     * 启动 Task
     *
     * @return
     * @throws Exception
     */
    public TaskShutdownDaemon execute() throws Exception {
        // 发送 startup 信息给系统 bolt
        taskSendTargets = this.echoToSystemBolt();

        // 创建发射数据的 TaskTransfer 对象
        taskTransfer = this.mkTaskSending(workerData);

        // 创建线程获取数据，并封装成 tuple 传递给 spout 或 bolt
        RunnableCallback baseExecutor = this.prepareExecutor(); // 创建并获取组件对应的 executor
        this.setBaseExecutors((BaseExecutors) baseExecutor);
        AsyncLoopThread executor_threads = new AsyncLoopThread(baseExecutor, false, Thread.MAX_PRIORITY, true);

        taskReceiver = this.mkTaskReceiver();

        List<AsyncLoopThread> allThreads = new ArrayList<>();
        allThreads.add(executor_threads);

        LOG.info("Finished loading task " + componentId + ":" + taskId);

        // 创建并返回 Task 的管理对象 TaskShutdownDaemon
        taskShutdownDameon = this.getShutdown(allThreads, baseExecutor);
        return taskShutdownDameon;
    }

    /**
     * 创建 {@link TaskTransfer} 对象，用于发送 tuple
     *
     * @param workerData
     * @return
     */
    private TaskTransfer mkTaskSending(WorkerData workerData) {
        // 创建一个用于发送 tuple 的 serializer
        KryoTupleSerializer serializer = new KryoTupleSerializer(workerData.getStormConf(), topologyContext.getRawTopology());
        // 获取 task 名称：“componentId:taskId”
        String taskName = JStormServerUtils.getName(componentId, taskId);
        // Task sending all tuples through this Object
        return new TaskTransfer(this, taskName, serializer, taskStatus, workerData, topologyContext);
    }

    public TaskShutdownDaemon getShutdown(List<AsyncLoopThread> allThreads, RunnableCallback baseExecutor) {
        AsyncLoopThread ackerThread;
        if (baseExecutor instanceof SpoutExecutors) {
            ackerThread = ((SpoutExecutors) baseExecutor).getAckerRunnableThread();

            if (ackerThread != null) {
                allThreads.add(ackerThread);
            }
        }
        List<AsyncLoopThread> recvThreads = taskReceiver.getDeserializeThread();
        for (AsyncLoopThread recvThread : recvThreads) {
            allThreads.add(recvThread);
        }

        List<AsyncLoopThread> serializeThreads = taskTransfer.getSerializeThreads();
        allThreads.addAll(serializeThreads);
        TaskHeartbeatTrigger taskHeartbeatTrigger = ((BaseExecutors) baseExecutor).getTaskHbTrigger();

        return new TaskShutdownDaemon(
                taskStatus, topologyId, taskId, allThreads, zkCluster, taskObj, this, taskHeartbeatTrigger);
    }

    public TaskShutdownDaemon getTaskShutdownDameon() {
        return taskShutdownDameon;
    }

    @Override
    public void run() {
        try {
            // 调用 execute 方法执行任务
            taskShutdownDameon = this.execute();
        } catch (Throwable e) {
            LOG.error("init task error", e);
            if (reportErrorDie != null) {
                reportErrorDie.report(e);
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    public static TaskShutdownDaemon mk_task(WorkerData workerData, int taskId) throws Exception {
        Task t = new Task(workerData, taskId);
        return t.execute();
    }

    /**
     * Update task data which can be changed dynamically e.g. when scale-out of a task parallelism
     */
    public void updateTaskData() {
        /*
         * Only update the local task list of topologyContext here.
         * Because other relative parts in context shall be updated while the updating of WorkerData (Task2Component and Component2Task map)
         */
        List<Integer> localTasks = JStormUtils.mk_list(workerData.getTaskIds());
        topologyContext.setThisWorkerTasks(localTasks);
        userContext.setThisWorkerTasks(localTasks);

        // Update the TaskSendTargets
        this.updateSendTargets();
    }

    public long getWorkerAssignmentTs() {
        return workerData.getAssignmentTs();
    }

    public AssignmentType getWorkerAssignmentType() {
        return workerData.getAssignmentType();
    }

    public void unregisterDeserializeQueue() {
        deserializeQueues.remove(taskId);
    }

    public String getComponentId() {
        return componentId;
    }

    public Integer getTaskId() {
        return taskId;
    }

    public DisruptorQueue getExecuteQueue() {
        return innerTaskTransfer.get(taskId);
    }

    public DisruptorQueue getDeserializeQueue() {
        return deserializeQueues.get(taskId);
    }

    public Map<Object, Object> getStormConf() {
        return stormConf;
    }

    public TopologyContext getTopologyContext() {
        return topologyContext;
    }

    public TopologyContext getUserContext() {
        return userContext;
    }

    public TaskTransfer getTaskTransfer() {
        return taskTransfer;
    }

    public TaskReceiver getTaskReceiver() {
        return taskReceiver;
    }

    public Map<Integer, DisruptorQueue> getInnerTaskTransfer() {
        return innerTaskTransfer;
    }

    public Map<Integer, DisruptorQueue> getDeserializeQueues() {
        return deserializeQueues;
    }

    public Map<Integer, DisruptorQueue> getControlQueues() {
        return controlQueues;
    }

    public String getTopologyId() {
        return topologyId;
    }

    public TaskStatus getTaskStatus() {
        return taskStatus;
    }

    public StormClusterState getZkCluster() {
        return zkCluster;
    }

    public Object getTaskObj() {
        return taskObj;
    }

    public TaskBaseMetric getTaskStats() {
        return taskStats;
    }

    public WorkerData getWorkerData() {
        return workerData;
    }

    public TaskSendTargets getTaskSendTargets() {
        return taskSendTargets;
    }

    public TaskReportErrorAndDie getReportErrorDie() {
        return reportErrorDie;
    }

    public boolean isTaskBatchTuple() {
        return isTaskBatchTuple;
    }

    public ConcurrentHashMap<WorkerSlot, IConnection> getNodePortToSocket() {
        return nodePortToSocket;
    }

    public ConcurrentHashMap<Integer, WorkerSlot> getTaskToNodePort() {
        return taskToNodePort;
    }

    public BaseExecutors getBaseExecutors() {
        return baseExecutors;
    }

    public void setBaseExecutors(BaseExecutors baseExecutors) {
        this.baseExecutors = baseExecutors;
    }

}
