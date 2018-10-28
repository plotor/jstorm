package com.alibaba.jstorm.callback;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author zhenchao.wang 2018-06-13 22:14
 * @version 1.0.0
 */
public class MyRunnableCallback extends RunnableCallback {

    private static AtomicInteger count = new AtomicInteger();

    @Override
    public void run() {
        System.out.println("[" + count.incrementAndGet() + "] thread-" + Thread.currentThread().getId() + " is running.");
    }

    @Override
    public Object getResult() {
        return 1;
    }

    public static void main(String[] args) {
        MyRunnableCallback callback = new MyRunnableCallback();
        new AsyncLoopThread(callback);
    }
}
