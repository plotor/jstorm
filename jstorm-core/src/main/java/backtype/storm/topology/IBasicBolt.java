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

package backtype.storm.topology;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

import java.util.Map;

/**
 * 与 IBolt 基本一致，区别在于：
 *
 * 1. 输出收集器使用 {@link BasicOutputCollector}，并且该参数放置在 execute 方法中，而不是 prepare 中
 * 2. 实现了 {@link IComponent} 接口
 *
 * 使用 IBasicBolt，Storm 会自动帮用户进行 ack、fail 和 anchor 操作，用户不需要关心这一点，这是由执行器 {@link BasicBoltExecutor} 实现的
 */
public interface IBasicBolt extends IComponent {

    /* 使用 BasicOutputCollector 作为输出收集器 */

    void prepare(Map stormConf, TopologyContext context);

    /**
     * Process the input tuple and optionally emit new tuples based on the input tuple.
     *
     * All acking is managed for you. Throw a FailedException if you want to fail the tuple.
     */
    void execute(Tuple input, BasicOutputCollector collector);

    void cleanup();
}