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

package com.alibaba.jstorm.task.execute.spout;

import backtype.storm.Config;
import backtype.storm.spout.ISpout;
import backtype.storm.spout.SpoutOutputCollectorCb;
import backtype.storm.task.ICollectorCallback;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.MessageId;
import backtype.storm.tuple.TupleImplExt;
import backtype.storm.utils.DisruptorQueue;
import backtype.storm.utils.Utils;
import com.alibaba.jstorm.client.spout.IAckValueSpout;
import com.alibaba.jstorm.client.spout.IFailValueSpout;
import com.alibaba.jstorm.common.metric.AsmHistogram;
import com.alibaba.jstorm.metric.JStormMetrics;
import com.alibaba.jstorm.metric.MetricDef;
import com.alibaba.jstorm.metric.MetricType;
import com.alibaba.jstorm.metric.MetricUtils;
import com.alibaba.jstorm.task.Task;
import com.alibaba.jstorm.task.TaskBaseMetric;
import com.alibaba.jstorm.task.TaskTransfer;
import com.alibaba.jstorm.task.acker.Acker;
import com.alibaba.jstorm.task.comm.TaskSendTargets;
import com.alibaba.jstorm.task.comm.TupleInfo;
import com.alibaba.jstorm.task.comm.UnanchoredSend;
import com.alibaba.jstorm.task.error.ITaskReportErr;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.TimeOutMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * spout collector, sending tuple through this Object
 *
 * @author yannian/Longda
 */
public class SpoutCollector extends SpoutOutputCollectorCb {

    private static Logger LOG = LoggerFactory.getLogger(SpoutCollector.class);

    protected TaskSendTargets sendTargets;
    protected Map storm_conf;
    protected TaskTransfer transfer_fn;

    protected TimeOutMap<Long, TupleInfo> pending;
    protected boolean isCacheTuple;
    // topology_context is system topology context
    protected TopologyContext topology_context;

    protected DisruptorQueue disruptorAckerQueue;
    protected TaskBaseMetric task_stats;
    protected backtype.storm.spout.ISpout spout;
    protected ITaskReportErr report_error;

    protected Integer task_id;
    protected Integer ackerNum;

    protected AsmHistogram emitTotalTimer;
    protected Random random;

    public SpoutCollector(Task task, TimeOutMap<Long, TupleInfo> pending, DisruptorQueue disruptorAckerQueue) {
        this.sendTargets = task.getTaskSendTargets();
        this.storm_conf = task.getStormConf();
        this.transfer_fn = task.getTaskTransfer();
        this.pending = pending;
        this.topology_context = task.getTopologyContext();

        this.disruptorAckerQueue = disruptorAckerQueue;

        this.task_stats = task.getTaskStats();
        this.spout = (ISpout) task.getTaskObj();
        this.task_id = task.getTaskId();
        this.report_error = task.getReportErrorDie();

        ackerNum = JStormUtils.parseInt(storm_conf.get(Config.TOPOLOGY_ACKER_EXECUTORS));

        random = new Random(Utils.secureRandomLong());

        if (spout instanceof IAckValueSpout || spout instanceof IFailValueSpout) {
            isCacheTuple = true;
        } else {
            isCacheTuple = false;
        }

        String componentId = topology_context.getThisComponentId();
        emitTotalTimer = (AsmHistogram) JStormMetrics.registerTaskMetric(MetricUtils.taskMetricName(
                topology_context.getTopologyId(), componentId, task_id, MetricDef.COLLECTOR_EMIT_TIME,
                MetricType.HISTOGRAM), new AsmHistogram());
        emitTotalTimer.setEnabled(false);
    }

    @Override
    public List<Integer> emit(String streamId, List<Object> tuple, Object messageId) {
        return this.sendSpoutMsg(streamId, tuple, messageId, null, null);
    }

    @Override
    public void emitDirect(int taskId, String streamId, List<Object> tuple, Object messageId) {
        this.sendSpoutMsg(streamId, tuple, messageId, taskId, null);
    }

    @Override
    public List<Integer> emit(String streamId, List<Object> tuple, Object messageId, ICollectorCallback callback) {
        return this.sendSpoutMsg(streamId, tuple, messageId, null, callback);
    }

    @Override
    public void emitDirect(int taskId, String streamId, List<Object> tuple, Object messageId, ICollectorCallback callback) {
        this.sendSpoutMsg(streamId, tuple, messageId, taskId, callback);
    }

    @Override
    public void emitDirectCtrl(int taskId, String streamId, List<Object> tuple, Object messageId) {
        this.sendCtrlMsg(streamId, tuple, messageId, taskId);
    }

    @Override
    public List<Integer> emitCtrl(String streamId, List<Object> tuple, Object messageId) {
        return this.sendCtrlMsg(streamId, tuple, messageId, null);
    }

    protected List<Integer> sendSpoutMsg(
            String outStreamId, List<Object> values, Object messageId, Integer outTaskId, ICollectorCallback callback) {
        return this.sendMsg(outStreamId, values, messageId, outTaskId, callback);
    }

    /**
     * @param topologyContext
     * @param taskTargets
     * @param transfer_fn
     * @param stream __ack_init
     * @param values
     */
    void unanchoredSend(TopologyContext topologyContext, TaskSendTargets taskTargets,
                        TaskTransfer transfer_fn, String stream, List<Object> values) {
        UnanchoredSend.send(topologyContext, taskTargets, transfer_fn, stream, values);
    }

    void transferCtr(TupleImplExt tupleExt) {
        transfer_fn.transferControl(tupleExt);
    }

    /**
     * 向 Acker 发送 ack_init 消息
     *
     * @param outStreamId
     * @param values
     * @param messageId
     * @param rootId
     * @param ackSeq 所有目标 task 的随机数列表
     * @param needAck
     */
    protected void sendMsgToAck(
            String outStreamId, List<Object> values, Object messageId, Long rootId, List<Long> ackSeq, boolean needAck) {
        if (needAck) {
            TupleInfo info = TupleInfo.buildTupleInfo(outStreamId, messageId, values, System.currentTimeMillis(), isCacheTuple);
            pending.putHead(rootId, info);
            // ackerTuple = <root_id, 所有目标 task 的 messageId 随机数值的异或, task_id>
            List<Object> ackerTuple = JStormUtils.mk_list((Object) rootId, JStormUtils.bit_xor_vals(ackSeq), task_id);
            // 向 Acker 发射 ack_init 消息，依据 __ack_init 这个 stream 直接找到目标 task 进行发送
            this.unanchoredSend(topology_context, sendTargets, transfer_fn, Acker.ACKER_INIT_STREAM_ID, ackerTuple);
        } else if (messageId != null) {
            // 不需要 ack 但是仍然实现了 IAckValueSpout 接口，需要为这种 spout 回调 ack 方法
            TupleInfo info = TupleInfo.buildTupleInfo(outStreamId, messageId, values, 0, isCacheTuple);
            AckSpoutMsg ack = new AckSpoutMsg(rootId, spout, null, info, task_stats);
            ack.run();
        }
    }

    /**
     * send spout message
     *
     * @param out_stream_id
     * @param values
     * @param message_id
     * @param out_task_id 目标 task_id
     * @param callback
     * @return
     */
    public List<Integer> sendMsg(
            String out_stream_id, List<Object> values, Object message_id, Integer out_task_id, ICollectorCallback callback) {
        final long startTime = emitTotalTimer.getTime();
        try {
            boolean needAck = (message_id != null) && (ackerNum > 0);
            // 生成随机的 rootId (随机 long 数值)，需要确保在当前 spout 唯一，否则无法保证 ack 的准确性
            Long root_id = this.getRootId(message_id);
            List<Integer> outTasks;
            // 获取目标 taskId 列表
            if (out_task_id != null) {
                outTasks = sendTargets.get(out_task_id, out_stream_id, values, null, root_id);
            } else {
                outTasks = sendTargets.get(out_stream_id, values, null, root_id);
            }

            List<Long> ackSeq = new ArrayList<>(); // 存放为所有 bolt task 生成的随机数值
            /*
             * 遍历所有的目标 task：
             * 1. 为每个目标 task 生成 messageId： <root_id, 随机数值>
             * 2. 向所有目标 task 发射 tuple 消息
             */
            for (Integer taskId : outTasks) {
                MessageId msgId;
                if (needAck) {
                    long as = MessageId.generateId(random); // 生成随机的 long 数值
                    msgId = MessageId.makeRootId(root_id, as); // <root_id, 随机数值>
                    ackSeq.add(as); // 添加到 ackSeq 中，用于后面亦或计算
                } else {
                    msgId = null;
                }

                // 获取当前 tuple 对应的目标 task 的传输队列，然后将 tuple 投递给该队列
                TupleImplExt tuple = new TupleImplExt(topology_context, values, task_id, out_stream_id, msgId);
                tuple.setTargetTaskId(taskId);
                transfer_fn.transfer(tuple);
            }
            // 向 Acker 发送 ack_init 消息
            this.sendMsgToAck(out_stream_id, values, message_id, root_id, ackSeq, needAck);
            if (callback != null) {
                callback.execute(out_stream_id, outTasks, values);
            }
            return outTasks;
        } finally {
            emitTotalTimer.updateTime(startTime);
        }
    }

    protected List<Integer> sendCtrlMsg(String out_stream_id, List<Object> values, Object message_id, Integer out_task_id) {
        final long startTime = emitTotalTimer.getTime();
        try {
            boolean needAck = (message_id != null) && (ackerNum > 0);
            Long root_id = this.getRootId(message_id);
            java.util.List<Integer> out_tasks;

            if (out_task_id != null) {
                out_tasks = sendTargets.get(out_task_id, out_stream_id, values, null, root_id);
            } else {
                out_tasks = sendTargets.get(out_stream_id, values, null, root_id);
            }

            List<Long> ackSeq = new ArrayList<>();
            for (Integer t : out_tasks) {
                MessageId msgId;
                if (needAck) {
                    long as = MessageId.generateId(random);
                    msgId = MessageId.makeRootId(root_id, as);
                    ackSeq.add(as);
                } else {
                    msgId = null;
                }

                TupleImplExt tp = new TupleImplExt(topology_context, values, task_id, out_stream_id, msgId);
                tp.setTargetTaskId(t);
                this.transferCtr(tp);
            }
            this.sendMsgToAck(out_stream_id, values, message_id, root_id, ackSeq, needAck);
            return out_tasks;
        } finally {
            emitTotalTimer.updateTime(startTime);
        }
    }

    @Override
    public void reportError(Throwable error) {
        report_error.report(error);
    }

    /**
     * 生成随机的root_id，但是需要确保在当前spout中不能有重复的，不然就不能保证ack的准确性了
     *
     * @param messageId
     * @return
     */
    protected Long getRootId(Object messageId) {
        boolean needAck = (messageId != null) && (ackerNum > 0);

        // This change storm logic
        // Storm can't make sure root_id is unique
        // storm's logic is root_id = MessageId.generateId(random);
        // when duplicate root_id, it will miss call ack/fail
        Long rootId = null;
        if (needAck) {
            // 随机 long
            rootId = MessageId.generateId(random);
        }
        return rootId;
    }

}
