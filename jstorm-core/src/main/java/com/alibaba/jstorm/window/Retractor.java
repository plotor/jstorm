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

import backtype.storm.tuple.Tuple;

import java.io.Serializable;
import java.util.Collection;

/**
 * A retractor enables re-computing of purged historic windows.
 * Note that for such re-computing, user would need to pull window states from their own state factory,
 * e.g., HBase, HDFS, DB, etc. instead of state factory within JStorm.
 *
 * @author wange
 * @since 16/12/19
 */
public interface Retractor extends Serializable {

    /**
     * 这个element所属的窗口为windows这个集合，由用户指定如何处理这条乱序消息。
     * 以word count为例，我们可能每隔1分钟计算word count，然后最终把每个window下的word count输出到hbase或者tair中。
     * 在watermark=18:00:00时，假设之前计算出来的17:30分这个窗口的word: aa的count=100。接下来我们又收到一条消息，timestamp=17:30:00，word=aa。
     * 此时我们需要更新HBase/Tair中17:30分这个窗口中对应的aa的count值。那么我们可以在retract方法中，直接去update HBase或者tair。
     * 当然，如果经常会发生乱序，一条条处理效率显然是太慢了。建议用户可以保存一个Map<TimeWindow, List<Tuple>>（或者使用guava中的Multimap）对象， 当达到一定数量再触发一次计算。
     * 不过说了这么多，使用retractor的一个最大前提还是：计算结果必须是可被更新的。否则就只能丢弃然后打个日志了。
     *
     * @param element
     * @param windows
     */
    void retract(Tuple element, Collection<TimeWindow> windows);

}
