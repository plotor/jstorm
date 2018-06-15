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

package backtype.storm.messaging;

import backtype.storm.utils.DisruptorQueue;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This interface needs to be implemented for messaging plugin.
 *
 * Messaging plugin is specified via Storm config parameter, storm.messaging.transport.
 *
 * A messaging plugin should have a default constructor and implements IContext interface.
 * Upon construction, we will invoke IContext::prepare(storm_conf) to
 * enable context to be configured according to storm configuration.
 *
 * 定义了创建和关闭 Socket 连接的方法
 */
public interface IContext {

    /**
     * invoked at the startup of messaging plugin
     *
     * @param storm_conf storm configuration
     */
    void prepare(Map storm_conf);

    /**
     * invoked when a connection is terminated
     */
    void term();

    /**
     * 创建并返回一个 Socket 连接（主要用于接收消息）
     *
     * @param topology_id topology ID
     * @param port port #
     * @return server side connection
     */
    IConnection bind(String topology_id, int port, ConcurrentHashMap<Integer, DisruptorQueue> deserializedQueue,
                     DisruptorQueue recvControlQueue, boolean bstartRec, Set<Integer> workerTasks);

    /**
     * 创建到指定 host 和 port 的连接（主要用于发送消息）
     *
     * @param topology_id
     * @param host
     * @param port
     * @param sourceTasks
     * @param targetTasks
     * @return
     */
    IConnection connect(String topology_id, String host, int port, Set<Integer> sourceTasks, Set<Integer> targetTasks);

    /**
     * establish a client side connection to a remote server
     *
     * This interface was just for testing
     *
     * @param topology_id topology ID
     * @param host remote host
     * @param port remote port
     * @return client side connection
     */
    @Deprecated
    IConnection connect(String topology_id, String host, int port);
}
