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

package backtype.storm.coordination;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IComponent;
import backtype.storm.tuple.Tuple;

import java.io.Serializable;
import java.util.Map;

/**
 * IBatchBolt 主要用于 Storm 中的批处理，目前 Storm 主要基于该接口来实现可靠的消息传输，在这种情况下，批处理会比单一消息处理更为高效。
 *
 * Storm 的事务 Topology 和 Trident 主要基于 IBatchBolt 实现
 *
 * 使用 IBatchBolt，Storm 会自动帮用户进行 ack、fail 和 anchor 操作，用户不需要关心这一点
 *
 * 使用 {@link BatchOutputCollector} 作为输出收集器
 *
 * @param <T>
 */
public interface IBatchBolt<T> extends Serializable, IComponent {

    /**
     * 用来初始化一个 Batch
     *
     * @param conf
     * @param context
     * @param collector
     * @param id batch 的唯一标识
     */
    void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, T id);

    void execute(Tuple tuple);

    /**
     * 在批处理结束时被调用
     */
    void finishBatch();

}
