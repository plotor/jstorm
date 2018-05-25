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

package com.alibaba.jstorm.callback;

import backtype.storm.daemon.Shutdownable;

/**
 * Base Runnable/Callback function
 *
 * 对 {@link Runnable}/{@link Callback}/{@link Shutdownable} 的一个聚合，提供了基础实现
 *
 * @author yannian
 */
public class RunnableCallback implements Runnable, Callback, Shutdownable {

    @Override
    public void run() {
    }

    @Override
    public <T> Object execute(T... args) {
        return null;
    }

    @Override
    public void shutdown() {
    }

    public void preRun() {
    }

    public void postRun() {
    }

    public Exception error() {
        return null;
    }

    public Object getResult() {
        return null;
    }

    public String getThreadName() {
        return null;
    }

}
