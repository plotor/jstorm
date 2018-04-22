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

package backtype.storm.spout;

import java.util.List;

/**
 * Spout 输出收集器
 */
public interface ISpoutOutputCollector {

    /**
     * Returns the task ids that received the tuples.
     *
     * @param streamId 消息被输出到的流
     * @param tuple 要输出的消息
     * @param messageId 输出消息的标识信息
     * @return
     */
    List<Integer> emit(String streamId, List<Object> tuple, Object messageId);

    /**
     * 相对于 {@see emit} 的主要区别在于发出的消息只有 taskId 所指定的 Task 才可以接受到这条消息，
     * 该方法要求 streamId 对应的流必须被定义为直接流（Direct Stream），同时接收端的 Task 也必须以
     * 直接分组（Direct Grouping）的方式来接收消息
     *
     * @param taskId 指定接收此消息的 Task
     * @param streamId
     * @param tuple
     * @param messageId
     */
    void emitDirect(int taskId, String streamId, List<Object> tuple, Object messageId);

    void reportError(Throwable error);
}
