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

package com.alibaba.jstorm.task.acker;

import backtype.storm.Config;
import backtype.storm.task.IBolt;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.RotatingMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * @author yannian/Longda
 */
public class Acker implements IBolt {

    private static final long serialVersionUID = 4430906880683183091L;

    private static final Logger LOG = LoggerFactory.getLogger(Acker.class);

    public static final String ACKER_COMPONENT_ID = "__acker";
    public static final String ACKER_INIT_STREAM_ID = "__ack_init";
    public static final String ACKER_ACK_STREAM_ID = "__ack_ack";
    public static final String ACKER_FAIL_STREAM_ID = "__ack_fail";

    public static final int TIMEOUT_BUCKET_NUM = 3;

    private OutputCollector collector = null;
    private RotatingMap<Object, AckObject> pending = null;
    private long lastRotate = System.currentTimeMillis();
    private long rotateTime;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.pending = new RotatingMap<>(TIMEOUT_BUCKET_NUM, true);
        this.rotateTime = 1000L * JStormUtils.parseInt(stormConf.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS), 30) / (TIMEOUT_BUCKET_NUM - 1);
    }

    @Override
    public void execute(Tuple input) {
        Object id = input.getValue(0);
        AckObject curr = pending.get(id);
        String stream_id = input.getSourceStreamId();
        // __acker_init 消息，由 spout 发送，直接放入 pending map 中
        if (Acker.ACKER_INIT_STREAM_ID.equals(stream_id)) {
            if (curr == null) {
                curr = new AckObject();
                curr.val = input.getLong(1);
                curr.spout_task = input.getInteger(2);
                pending.put(id, curr);
            } else {
                // bolt's ack first come
                curr.update_ack(input.getValue(1));
                curr.spout_task = input.getInteger(2);
            }

        }
        // __ack_ack 消息，来自于 Bolt 发送
        else if (Acker.ACKER_ACK_STREAM_ID.equals(stream_id)) {
            if (curr != null) {
                // 执行亦或运算
                curr.update_ack(input.getValue(1));
            } else {
                // two case
                // one is timeout
                // the other is bolt's ack first come
                curr = new AckObject();
                curr.val = input.getLong(1);
                pending.put(id, curr);
            }
        }
        // __ack_fail 消息
        else if (Acker.ACKER_FAIL_STREAM_ID.equals(stream_id)) {
            if (curr == null) {
                // do nothing
                // already timeout, should go fail
                return;
            }
            curr.failed = true;
        } else {
            LOG.info("Unknown source stream, " + stream_id + " from task-" + input.getSourceTask());
            return;
        }

        // 告诉spout这个消息ack/fail了
        Integer task = curr.spout_task;
        if (task != null) {
            if (curr.val == 0) {
                pending.remove(id);
                List values = JStormUtils.mk_list(id);
                collector.emitDirect(task, Acker.ACKER_ACK_STREAM_ID, values);
            } else {
                if (curr.failed) {
                    pending.remove(id);
                    List values = JStormUtils.mk_list(id);
                    collector.emitDirect(task, Acker.ACKER_FAIL_STREAM_ID, values);
                }
            }
        }

        // 这里只是更新metrics
        // add this operation to update acker stats
        collector.ack(input);

        long now = System.currentTimeMillis();
        if (now - lastRotate > rotateTime) {
            lastRotate = now;
            Map<Object, AckObject> tmp = pending.rotate();
            if (tmp.size() > 0) {
                LOG.warn("Acker's timeout item size:{}", tmp.size());
            }
        }

    }

    @Override
    public void cleanup() {
        LOG.info("Successfully cleanup");
    }

}
