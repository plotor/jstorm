package com.alibaba.jstorm.callback;

/**
 * @author zhenchao.wang 2018-05-26 11:35
 * @version 1.0.0
 */
public class AsyncLoopThreadTest {

    /*@Test
    public void async() throws Exception {

        TimeUnit.SECONDS.sleep(5);
    }*/

    public static void main(String[] args) {
        AsyncLoopThread loop = new AsyncLoopThread(new MyRunnableCallback());
    }
}