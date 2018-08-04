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

import com.alibaba.jstorm.utils.JStormUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * AsyncLoopThread runnable
 *
 * The class wraps RunnableCallback fn, if an exception is thrown, will run killFn
 *
 * 对于 {@link RunnableCallback} 的包装
 *
 * @author yannian
 */
public class AsyncLoopRunnable implements Runnable {

    private static Logger LOG = LoggerFactory.getLogger(AsyncLoopRunnable.class);

    // 标记是否 shutdown
    private static AtomicBoolean shutdown = new AtomicBoolean(false);
    private AtomicBoolean shutdowned = new AtomicBoolean(false);

    public static AtomicBoolean getShutdown() {
        return shutdown;
    }

    /** 线程函数 */
    private RunnableCallback fn;
    private RunnableCallback killFn;
    private long lastTime = System.currentTimeMillis();

    /**
     * 对于 fn 和 killFn 的包装
     *
     * @param fn
     * @param killFn
     */
    public AsyncLoopRunnable(RunnableCallback fn, RunnableCallback killFn) {
        this.fn = fn;
        this.killFn = killFn;
    }

    @Override
    public void run() {
        if (fn == null) {
            LOG.error("fn==null");
            throw new RuntimeException("AsyncLoopRunnable no core function ");
        }

        // 模板方法
        fn.preRun();

        try {
            while (!shutdown.get()) {
                // 执行自定义 callback 逻辑
                fn.run();

                if (shutdown.get()) {
                    this.shutdown();
                    return;
                }

                Exception e = fn.error();
                if (e != null) {
                    throw e;
                }

                // 获取睡眠时间（单位：秒）
                Object rtn = fn.getResult();
                if (this.needQuit(rtn)) {
                    this.shutdown();
                    return;
                }
            }
        } catch (Throwable e) {
            if (shutdown.get()) {
                this.shutdown();
            } else {
                LOG.error("Async loop died!!!" + e.getMessage(), e);
                killFn.execute(e);
            }
        }
    }

    /**
     * 基于指定的间隔时间判定是否终止当前线程，
     * 如果设置了睡眠时间则不会终止，并执行睡眠
     *
     * @param rtn
     * @return
     */
    private boolean needQuit(Object rtn) {
        if (rtn != null) {
            long sleepTime = Long.parseLong(String.valueOf(rtn));
            if (sleepTime < 0) {
                // 未设置睡眠时间
                return true;
            } else if (sleepTime > 0) {
                long now = System.currentTimeMillis();
                long cost = now - lastTime;
                long sleepMs = sleepTime * 1000 - cost; // 期望睡眠时间 - 中间消耗的时间
                if (sleepMs > 0) {
                    // 还没有达到期望睡眠时间，继续睡眠
                    JStormUtils.sleepMs(sleepMs);
                    lastTime = System.currentTimeMillis();
                } else {
                    lastTime = now;
                }

            }
        }
        return false;
    }

    private void shutdown() {
        if (!shutdowned.getAndSet(true)) { // 如果之前是 false，则执行 shutdown 逻辑，并标记 shutdowned = true
            fn.postRun();
            fn.shutdown();
            LOG.info("Successfully shutdown");
        }
    }

}
