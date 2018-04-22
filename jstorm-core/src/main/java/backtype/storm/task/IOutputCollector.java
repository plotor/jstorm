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

package backtype.storm.task;

import backtype.storm.tuple.Tuple;

import java.util.Collection;
import java.util.List;

/**
 * Bolt 输出收集器
 */
public interface IOutputCollector extends IErrorReporter {

    /**
     * 向外发送数据
     *
     * @param streamId 消息被输出到的流
     * @param anchors 待发送消息的标记，通常代表该消息是由哪些消息产生的，主要用于消息的 ack 系统。
     * @param tuple 待输出的消息
     * @return 所有收到该消息的目标 taskId 集合
     */
    List<Integer> emit(String streamId, Collection<Tuple> anchors, List<Object> tuple);

    /**
     * 定向发送数据
     *
     * @param taskId
     * @param streamId
     * @param anchors
     * @param tuple
     */
    void emitDirect(int taskId, String streamId, Collection<Tuple> anchors, List<Object> tuple);

    /**
     * 标记消息被成功处理
     *
     * @param input
     */
    void ack(Tuple input);

    /**
     * 标记消息处理失败
     *
     * @param input
     */
    void fail(Tuple input);
}
