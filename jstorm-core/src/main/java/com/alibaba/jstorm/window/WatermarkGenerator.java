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

import backtype.storm.task.TopologyContext;

import java.io.Serializable;
import java.util.Map;

/**
 * 使用event time的时候，需要定义watermark，否则jstorm会使用默认的watermark实现：{@link PeriodicWatermarkGenerator}。 即定期往下游发送watermark。
 *
 * watermark标识了一个流是一直前进，不可回退的。即，假设当前收到了2017-01-13 18:00:00的watermark，那么意味着上游后续要发送的所有数据都不
 * 会早于18:00:00。如果早于这个时间，则认为是late element。处理策略参见下面late element。
 *
 * 当收到一条消息时，jstorm同时会调用{@link WatermarkGenerator#onElement}方法，以更新它内部的timestamp。
 * 同时，通过watermarkInterval来定时发送watermark和检查event windows是否需要purge。
 *
 * jstorm支持几种通过watermark触发window purge的策略：
 * 1. GLOBAL_MAX_TIMESTAMP 全局最大时间戳。这种策略会始终使用接收到的所有task中的最大的时间戳。如果这个时间戳>窗口边界，就会purge window
 * 2. MAX_TIMESTAMP_WITH_RATIO 这个策略在上面的基础上，还指定了收到上游watermark的task的比例，默认为0.9。即，只有当时间戳>窗口边界， 且收到了90%以上的task的watermark，才会purge window
 * 3. TASK_MAX_GLOBAL_MIN_TIMESTAMP 这个策略，会记录所有上游task发送的watermark的值，然后取所有task的watermark的最小值作为当前时间戳， 以防止各别task的timestamp很大导致窗口被过早purge。这种策略是jstorm的默认策略。
 *
 *
 * event time的场景中，消息的乱序几乎是必然会出现的。在上面的场景中即为，当前watermark已经到达18:00:00，但是下一条消息到达时，
 * 发现它的时间戳是17:30:00。这就是一条乱序的消息。默认情况下，jstorm会丢弃这条消息。也可以通过实现 {@link Retractor} 接口来
 * 重新计算一个已经purge的窗口值。
 *
 * @author wange
 * @since 16/12/16
 */
public interface WatermarkGenerator extends Serializable {

    void init(Map conf, TopologyContext context);

    long getCurrentWatermark();

    void onElement(long timestamp);

    long getWatermarkInterval();

}
