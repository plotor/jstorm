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

import backtype.storm.tuple.Fields;

/**
 * 定义了 topology 中每个组件的输出字段声明，
 * 每个 topology 都需要基于此接口来指定输出到哪些流、声明输出的字段列表以及指明输出流是否是直接流（Direct Stream）
 */
public interface OutputFieldsDeclarer {
    /**
     * Uses default stream id.
     */
    void declare(Fields fields);

    void declare(boolean direct, Fields fields);

    void declareStream(String streamId, Fields fields);

    void declareStream(String streamId, boolean direct, Fields fields);
}
