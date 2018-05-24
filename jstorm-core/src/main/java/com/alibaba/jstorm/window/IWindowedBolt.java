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

package com.alibaba.jstorm.window;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IComponent;
import backtype.storm.tuple.Tuple;

import java.util.Map;

/**
 * A bolt abstraction for supporting time and count based sliding（滑动） & tumbling（滚动） windows.
 *
 * 滚动窗口（Tumbling Window）
 * 每个Tuple只能属于其中一个滚动窗口，一个Tuple不能同时是两个或者两个以上窗口的元素。比如下面我们以消息到达的时间来划分成一个个滚动窗口，窗口大小是5秒:
 * 第一个窗口w1包含的是0~5th到达的数据，第二窗口w2包含的是5~10th秒到达的数据，第三个窗口包含的是10~15th秒到达的数据。每个窗口每隔5秒被计算，窗口和窗口直接没有重叠。
 *
 * 滑动窗口（Sliding Window）
 * tuples被划分到一个个滑动窗口，窗口滑动的间隔是sliding interval。每个窗口间会有重叠，重叠部分的tuple可以先后属于前后两个窗口。比如下面我们以事件处理时间划分滑动窗口，窗口大小是10秒，滑动间隔是5秒：
 * 第一个窗口w1包含的是0~10th到达的数据，第二窗口w2包含的是5~15th秒到达的数据。消息时间e3~e6都是窗口w1和w2的一部分。在15th秒时，窗口w2会被计算，这时候e1 e2的数据会会过期，会从队列中移除。
 *
 * jstorm 窗口机制的整体流程如下：
 *
 * 1. 创建WindowedBoltExecutor
 * 2. 到达一条tuple，都会先抽取消息的时间（processing time / event time），然后为这条消息分配窗口（一个或多个，取决于窗口类型）。
 * 3. 对这条tuple，遍历步骤2分配的窗口，调用execute(T tuple, Object state, TimeWindow window)进行计算。 在计算时，如果检测到这个window对应的用户状态为空，则调用Object initWindowState()初始化window状态。
 * 4. 检查window是否到期，如果到期，则调用void purgeWindow(Object state, TimeWindow window)，同时删除对应的window状态。
 *
 * 需要注意的是，jstorm的window机制不能跟acker系统一起正确地工作。因此如果你的业务重度依赖于acker来实现错误处理，那么不能使用最新的window机制。
 */
interface IWindowedBolt<T extends Tuple> extends IComponent {

    void prepare(Map stormConf, TopologyContext context, OutputCollector collector);

    void cleanup();

    /**
     * Init window state. This will be called before calling execute method.
     *
     * 初始化窗口的状态，这个状态是用户自定义状态，当窗口第一次建立时创建，即第一条属于该窗口的消息流入时创建。
     * jstorm框架会管理每个窗口对应的用户状态。可以认为是这个窗口计算的结果以及上下文信息。
     *
     * @return user-defined window state
     */
    Object initWindowState(TimeWindow window);

    /**
     * Execute a tuple within a window. If a tuple belongs to multiple windows,
     * all windows will be iterated and this method will be called on each window.
     *
     * 窗口消息到来时执行的计算。与storm不同，jstorm并不会憋数据，而是增量计算。因此每来一条消息都会直接
     * 触发计算以及更新窗口状态。如果一条消息属于多个窗口，那么每个窗口都会计算一次。
     *
     * @param tuple 新到达的消息
     * @param state 用户自定义状态，由框架传给用户（最开始由用户创建）。有一个限制：必须是引用类型，不能是String, Integer这种类型。
     * @param window 当前消息所属的窗口，如果有多个窗口，则execute方法本身被调用多次，每次调用时state和window都不相同。
     */
    void execute(T tuple, Object state, TimeWindow window);

    /**
     * purge一个window。当window到期时被调用。用户可以对此时的window状态做自定义操作，如存储到外部系统等。
     *
     * @param state window状态
     * @param window 到期的窗口
     */
    void purgeWindow(Object state, TimeWindow window);

}
