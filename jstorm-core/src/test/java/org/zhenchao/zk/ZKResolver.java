package org.zhenchao.zk;

import backtype.storm.utils.Utils;
import com.alibaba.jstorm.schedule.Assignment;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.data.Stat;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

/**
 * @author zhenchao.wang 2018-06-27 09:28
 * @version 1.0.0
 */
public class ZKResolver {

    private static CuratorFramework client;

    @BeforeClass
    public static void init() throws Exception {
        client = CuratorFrameworkFactory.newClient("10.38.164.192:2181", new ExponentialBackoffRetry(1000, 3));
        client.start();
    }

    @Test
    public void getData() throws Exception {
        String ROOT_PATH = "/jstorm/assignments";
        List<String> paths = client.getChildren().forPath(ROOT_PATH);
        for (final String path : paths) {
            String absolutePath = ROOT_PATH + "/" + path;
            System.out.println(path);
            Stat stat = client.checkExists().forPath(absolutePath);
            System.out.println("stat:" + stat);

            byte[] data = client.getData().forPath(absolutePath);
            Assignment assignment = (Assignment) Utils.maybe_deserialize(data);
            System.out.println(assignment);
        }
    }
}
