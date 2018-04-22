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

package backtype.storm.tuple;

import backtype.storm.generated.GlobalStreamId;

/**
 * Tuple 是 storm 的主要数据结构，storm 发送消息的过程中，每一条消息实际上都是一个 Tuple 对象
 *
 * The tuple is the main data structure in Storm. A tuple is a named list of values, where each value can be any type.
 * Tuples are dynamically typed -- the types of the fields do not need to be declared.
 *
 * Tuples have helper methods like getInteger and getString to get field values without having to cast the result.
 *
 * Storm needs to know how to serialize all the values in a tuple.
 * By default, Storm knows how to serialize the primitive types, strings, and byte arrays.
 * If you want to use another type, you'll need to implement and register a serializer for that type.
 * See {@link https://github.com/nathanmarz/storm/wiki/Serialization} for more info.
 */
public interface Tuple extends ITuple {

    /**
     * 获取与该消息对应的 GlobalStreamId
     *
     * Returns the global stream id (component + stream) of this tuple.
     */
    GlobalStreamId getSourceGlobalStreamId();

    /**
     * Gets the id of the component that created this tuple.
     */
    String getSourceComponent();

    /**
     * Gets the id of the task that created this tuple.
     */
    int getSourceTask();

    /**
     * Gets the id of the stream that this tuple was emitted to.
     */
    String getSourceStreamId();

    /**
     * Gets the message id that associated with this tuple.
     */
    MessageId getMessageId();
}
