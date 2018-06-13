package com.alibaba.jstorm.callback;

/**
 * @author zhenchao.wang 2018-06-13 22:14
 * @version 1.0.0
 */
public class MyRunnableCallback extends RunnableCallback {

    @Override
    public void run() {
        System.out.println("Thread-" + Thread.currentThread().getId() + " is running.");
    }

    @Override
    public Object getResult() {
        return 3;
    }
}
