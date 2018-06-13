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

import backtype.storm.utils.Time;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.SmartThread;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.Thread.UncaughtExceptionHandler;

/**
 * 对于 {@link SmartThread} 的唯一实现
 *
 * wraps timer thread to execute afn every several seconds, if an exception is thrown, run killFn
 *
 * @author yannian
 */
public class AsyncLoopThread implements SmartThread {

    private static final Logger LOG = LoggerFactory.getLogger(AsyncLoopThread.class);

    private Thread thread;
    private RunnableCallback afn;

    public AsyncLoopThread(RunnableCallback afn) {
        this.init(afn, false, Thread.NORM_PRIORITY, true);
    }

    public AsyncLoopThread(RunnableCallback afn, boolean daemon, int priority, boolean start) {
        this.init(afn, daemon, priority, start);
    }

    public AsyncLoopThread(RunnableCallback afn, boolean daemon, RunnableCallback kill_fn, int priority, boolean start) {
        this.init(afn, daemon, kill_fn, priority, start);
    }

    public void init(RunnableCallback afn, boolean daemon, int priority, boolean start) {
        RunnableCallback kill_fn = new AsyncLoopDefaultKill();
        this.init(afn, daemon, kill_fn, priority, start);
    }

    /**
     * @param afn 异步线程函数
     * @param daemon 是否是守护线程
     * @param kill_fn 进程被 kill 时操作触发的线程函数
     * @param priority 线程优先级
     * @param start 是否启动
     */
    private void init(RunnableCallback afn, boolean daemon, RunnableCallback kill_fn, int priority, boolean start) {
        if (kill_fn == null) {
            // 如果没有设置，则默认创建一个
            kill_fn = new AsyncLoopDefaultKill();
        }

        // 基于 AsyncLoopRunnable 对于 afn 和 kfn 进行包装
        Runnable runnable = new AsyncLoopRunnable(afn, kill_fn);
        thread = new Thread(runnable);
        String threadName = afn.getThreadName();
        if (threadName == null) {
            // 以 afn 的 simpleName 作为线程名称
            threadName = afn.getClass().getSimpleName();
        }
        // 配置线程
        thread.setName(threadName);
        thread.setDaemon(daemon);
        thread.setPriority(priority);
        thread.setUncaughtExceptionHandler(new UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                LOG.error("UncaughtException", e);
                JStormUtils.halt_process(1, "UncaughtException");
            }
        });

        this.afn = afn;

        if (start) {
            // 启动线程
            thread.start();
        }
    }

    @Override
    public void start() {
        thread.start();
    }

    @Override
    public void join() throws InterruptedException {
        thread.join();
    }

    /**
     * {@link VisibleForTesting} 只是一个标记，提醒调用方该方法仅仅用于测试
     * 如果需要在 UT 中对于私有方法进行测试，还是需要使用反射
     */
    @VisibleForTesting
    public void join(int times) throws InterruptedException {
        thread.join(times);
    }

    @Override
    public void interrupt() {
        thread.interrupt();
    }

    @Override
    public Boolean isSleeping() {
        return Time.isThreadWaiting(thread);
    }

    public Thread getThread() {
        return thread;
    }

    @Override
    public void cleanup() {
        afn.shutdown();
    }
}
