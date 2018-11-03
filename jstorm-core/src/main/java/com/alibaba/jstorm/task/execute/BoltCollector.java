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

package com.alibaba.jstorm.task.execute;

import backtype.storm.Config;
import backtype.storm.task.ICollectorCallback;
import backtype.storm.task.OutputCollectorCb;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.MessageId;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImplExt;
import backtype.storm.utils.Utils;
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
import com.alibaba.jstorm.task.comm.UnanchoredSend;
import com.alibaba.jstorm.task.error.ITaskReportErr;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.RotatingMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

/**
 * bolt output interface, do emit/ack/fail
 *
 * @author yannian/Longda
 */
public class BoltCollector extends OutputCollectorCb {

    private static Logger LOG = LoggerFactory.getLogger(BoltCollector.class);

    protected ITaskReportErr reportError;
    protected TaskSendTargets sendTargets;
    protected TaskTransfer taskTransfer;
    protected TopologyContext topologyContext;
    protected Integer taskId;
    protected final RotatingMap<Tuple, Long> tupleStartTimes;
    protected TaskBaseMetric taskStats;
    protected final RotatingMap<Tuple, Long> pendingAcks;
    protected long lastRotate = System.currentTimeMillis();
    protected long rotateTime;

    protected Map stormConf;
    protected Integer ackerNum;
    protected AsmHistogram emitTimer;
    protected Random random;

    public BoltCollector(Task task, RotatingMap<Tuple, Long> tupleStartTimes, int message_timeout_secs) {
        this.rotateTime = 1000L * message_timeout_secs / (Acker.TIMEOUT_BUCKET_NUM - 1);
        this.reportError = task.getReportErrorDie();
        this.sendTargets = task.getTaskSendTargets();
        this.stormConf = task.getStormConf();
        this.taskTransfer = task.getTaskTransfer();
        this.topologyContext = task.getTopologyContext();
        this.taskId = task.getTaskId();
        this.taskStats = task.getTaskStats();

        this.pendingAcks = new RotatingMap<>(Acker.TIMEOUT_BUCKET_NUM);
        this.tupleStartTimes = tupleStartTimes;

        this.ackerNum = JStormUtils.parseInt(stormConf.get(Config.TOPOLOGY_ACKER_EXECUTORS));

        String componentId = topologyContext.getThisComponentId();
        this.emitTimer = (AsmHistogram) JStormMetrics.registerTaskMetric(MetricUtils.taskMetricName(
                topologyContext.getTopologyId(), componentId, taskId, MetricDef.COLLECTOR_EMIT_TIME, MetricType.HISTOGRAM),
                new AsmHistogram());
        this.emitTimer.setEnabled(false);
        this.random = new Random(Utils.secureRandomLong());
    }

    @Override
    public List<Integer> emit(String streamId, Collection<Tuple> anchors, List<Object> tuple) {
        return this.sendBoltMsg(streamId, anchors, tuple, null, null);
    }

    @Override
    public void emitDirect(int taskId, String streamId, Collection<Tuple> anchors, List<Object> tuple) {
        this.sendBoltMsg(streamId, anchors, tuple, taskId, null);
    }

    @Override
    public List<Integer> emit(String streamId, Collection<Tuple> anchors, List<Object> tuple, ICollectorCallback callback) {
        return this.sendBoltMsg(streamId, anchors, tuple, null, callback);
    }

    @Override
    public void emitDirect(int taskId, String streamId, Collection<Tuple> anchors, List<Object> tuple, ICollectorCallback callback) {
        this.sendBoltMsg(streamId, anchors, tuple, taskId, callback);
    }

    @Override
    public List<Integer> emitCtrl(String streamId, Collection<Tuple> anchors, List<Object> tuple) {
        return this.sendCtrlMsg(streamId, tuple, anchors, null);
    }

    @Override
    public void emitDirectCtrl(int taskId, String streamId, Collection<Tuple> anchors, List<Object> tuple) {
        this.sendCtrlMsg(streamId, tuple, anchors, taskId);
    }

    protected List<Integer> sendBoltMsg(String outStreamId, Collection<Tuple> anchors,
                                        List<Object> values, Integer outTaskId, ICollectorCallback callback) {
        return this.sendMsg(outStreamId, values, anchors, outTaskId, callback);
    }

    /**
     * 计算对目标 task 的 messageId
     *
     * @param anchors 大部分情况下 anchors 的 size 等于 1，为当前收到的 inputTuple。
     * @return
     */
    protected MessageId getMessageId(Collection<Tuple> anchors) {
        MessageId ret = null;
        if (anchors != null && ackerNum > 0) {
            Map<Long, Long> anchors_to_ids = new HashMap<>();
            for (Tuple tuple : anchors) {
                if (tuple.getMessageId() != null) {
                    Long edge_id = MessageId.generateId(random);
                    // 更新当前 inputTuple 的 edge_id 亦或值到 pending_acks
                    put_xor(pendingAcks, tuple, edge_id);
                    MessageId messageId = tuple.getMessageId();
                    if (messageId != null) {
                        // 这里将每一对 <root_id, edge_id> 放入 anchors_to_ids（一般情况下也只有一对），
                        // 由于 anchors_to_ids 是一个空 map，因此 put_xor 里面相当于将 <root_id, edge_id> 放入 anchors_to_ids
                        for (Long root_id : messageId.getAnchorsToIds().keySet()) {
                            put_xor(anchors_to_ids, root_id, edge_id);
                        }
                    }
                }
            }
            // new MessageId
            ret = MessageId.makeId(anchors_to_ids);
        }
        return ret;
    }

    /**
     * send bolt message
     *
     * @param out_stream_id
     * @param values
     * @param anchors
     * @param out_task_id
     * @param callback
     * @return
     */
    public List<Integer> sendMsg(String out_stream_id, List<Object> values,
                                 Collection<Tuple> anchors, Integer out_task_id, ICollectorCallback callback) {
        final long start = emitTimer.getTime();
        List<Integer> outTasks = null;
        try {
            // 获取所有目标 task 列表
            if (out_task_id != null) {
                outTasks = sendTargets.get(out_task_id, out_stream_id, values, anchors, null);
            } else {
                outTasks = sendTargets.get(out_stream_id, values, anchors, null);
            }

            // 提前删除可能超时的 tuple
            this.tryRotate();

            /*
             * 遍历所有的目标 task：
             * 1. 为每一个 task 生成 messageId：<root_id, 随机数值>
             * 2. 向所有下游 bolt 发射 tuple 消息
             */
            for (Integer taskId : outTasks) {
                // 计算对目标 task 的 messageId
                MessageId msgId = this.getMessageId(anchors);
                TupleImplExt tuple = new TupleImplExt(topologyContext, values, this.taskId, out_stream_id, msgId);
                tuple.setTargetTaskId(taskId);
                taskTransfer.transfer(tuple);
            }
        } catch (Exception e) {
            LOG.error("bolt emit error:", e);
        } finally {
            if (outTasks == null) {
                outTasks = new ArrayList<>();
            }
            if (callback != null) {
                callback.execute(out_stream_id, outTasks, values);
            }
            emitTimer.updateTime(start);
        }
        return outTasks;
    }

    /**
     * 删除可能超时的 tuple
     */
    private void tryRotate() {
        long now = System.currentTimeMillis();
        if (now - lastRotate > rotateTime) {
            pendingAcks.rotate();
            lastRotate = now;
        }
    }

    void unanchoredSend(TopologyContext topologyContext, TaskSendTargets taskTargets,
                        TaskTransfer transfer_fn, String stream, List<Object> values) {
        UnanchoredSend.send(topologyContext, taskTargets, transfer_fn, stream, values);
    }

    void transferCtr(TupleImplExt tupleExt) {
        taskTransfer.transferControl(tupleExt);
    }

    protected List<Integer> sendCtrlMsg(String out_stream_id, List<Object> values,
                                        Collection<Tuple> anchors, Integer out_task_id) {
        final long start = emitTimer.getTime();
        java.util.List<Integer> out_tasks = null;
        try {

            if (out_task_id != null) {
                out_tasks = sendTargets.get(out_task_id, out_stream_id, values, anchors, null);
            } else {
                out_tasks = sendTargets.get(out_stream_id, values, anchors, null);
            }

            this.tryRotate();
            for (Integer t : out_tasks) {
                MessageId msgId = this.getMessageId(anchors);

                TupleImplExt tp = new TupleImplExt(topologyContext, values, taskId, out_stream_id, msgId);
                tp.setTargetTaskId(t);
                this.transferCtr(tp);
            }
        } catch (Exception e) {
            LOG.error("bolt emit error:", e);
        } finally {
            emitTimer.updateTime(start);
        }
        return out_tasks;
    }

    @Override
    public void ack(Tuple input) {
        if (input.getMessageId() != null) {
            Long ack_val = 0L;
            // 取出当前 inputTuple 对应的 edge_id 值
            Object pend_val = pendingAcks.remove(input);
            if (pend_val != null) {
                ack_val = (Long) (pend_val);
            }

            // 向 Acker 发送 ack 消息
            for (Entry<Long, Long> entry : input.getMessageId().getAnchorsToIds().entrySet()) {
                this.unanchoredSend(topologyContext, sendTargets, taskTransfer, Acker.ACKER_ACK_STREAM_ID, // __ack_ack
                        JStormUtils.mk_list((Object) entry.getKey(), JStormUtils.bit_xor(entry.getValue(), ack_val)));
            }
        }

        Long latencyStart = (Long) tupleStartTimes.remove(input);
        taskStats.bolt_acked_tuple(input.getSourceComponent(), input.getSourceStreamId());
        if (latencyStart != null && JStormMetrics.enabled) {
            long endTime = System.currentTimeMillis();
            taskStats.update_bolt_acked_latency(
                    input.getSourceComponent(), input.getSourceStreamId(), latencyStart, endTime);
        }
    }

    @Override
    public void fail(Tuple input) {
        // if ackerNum == 0, we can just return
        if (input.getMessageId() != null) {
            pendingAcks.remove(input);
            for (Entry<Long, Long> e : input.getMessageId().getAnchorsToIds().entrySet()) {
                this.unanchoredSend(topologyContext, sendTargets, taskTransfer, Acker.ACKER_FAIL_STREAM_ID,
                        JStormUtils.mk_list((Object) e.getKey()));
            }
        }

        tupleStartTimes.remove(input);
        taskStats.bolt_failed_tuple(input.getSourceComponent(), input.getSourceStreamId());
    }

    @Override
    public void reportError(Throwable error) {
        reportError.report(error);
    }

    /**
     * 获取并更新指定 tuple 对应的 edge_id 亦或值到 pending
     *
     * @param pending
     * @param key
     * @param id
     */
    public static void put_xor(RotatingMap<Tuple, Long> pending, Tuple key, Long id) {
        Long curr = pending.get(key); // 获取当前 tuple 对应的 edge_id 亦或值
        if (curr == null) {
            curr = 0L;
        }
        pending.put(key, JStormUtils.bit_xor(curr, id));
    }

    public static void put_xor(Map<Long, Long> pending, Long key, Long id) {
        Long curr = pending.get(key);
        if (curr == null) {
            curr = 0L;
        }
        pending.put(key, JStormUtils.bit_xor(curr, id));
    }

}
