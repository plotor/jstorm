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
import org.jboss.netty.channel.Channel;

import java.util.List;

public interface IConnection {

    /**
     * 用于接收消息，flags 表示是否为阻塞的方式（1 表示非阻塞，其他值表示阻塞）
     *
     * (flags != 1) synchronously (flags==1) asynchronously
     */
    Object recv(Integer taskId, int flags);

    /**
     * In the new design, receive flow is through registerQueue, then push message into queue
     */
    void registerQueue(Integer taskId, DisruptorQueue recvQueu);

    void enqueue(TaskMessage message, Channel channel);

    /**
     * 向某一个 Task 发送消息
     *
     * @param messages
     */
    void send(List<TaskMessage> messages);

    void send(TaskMessage message);

    void sendDirect(TaskMessage message);

    boolean available(int taskId);

    /**
     * 关闭当前连接
     */
    void close();

    boolean isClosed();
}
