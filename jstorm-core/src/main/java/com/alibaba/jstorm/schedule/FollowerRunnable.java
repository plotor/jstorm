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

package com.alibaba.jstorm.schedule;

import backtype.storm.Config;
import backtype.storm.utils.Utils;
import com.alibaba.jstorm.blobstore.BlobStore;
import com.alibaba.jstorm.blobstore.BlobStoreUtils;
import com.alibaba.jstorm.blobstore.BlobSynchronizer;
import com.alibaba.jstorm.blobstore.LocalFsBlobStore;
import com.alibaba.jstorm.callback.Callback;
import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.Cluster;
import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.daemon.nimbus.NimbusData;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.NetWorkUtils;
import com.google.common.collect.Sets;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * leader-follower 线程模型
 */
public class FollowerRunnable implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(FollowerRunnable.class);

    private NimbusData data;

    private int sleepTime;

    private volatile boolean state = true;

    private RunnableCallback blobSyncCallback;

    private Callback leaderCallback;

    private final String hostPort;

    public static final String NIMBUS_DIFFER_COUNT_ZK = "nimbus.differ.count.zk";

    public static final Integer SLAVE_NIMBUS_WAIT_TIME = 60;

    @SuppressWarnings("unchecked")
    public FollowerRunnable(final NimbusData data, int sleepTime, Callback leaderCallback) {
        this.data = data;
        this.sleepTime = sleepTime;
        this.leaderCallback = leaderCallback;

        // 判断是不是本地模式，对于本地模式不适用
        boolean isLocalIp;
        if (!ConfigExtension.isNimbusUseIp(data.getConf())) {
            this.hostPort = NetWorkUtils.hostname() + ":" + Utils.getInt(data.getConf().get(Config.NIMBUS_THRIFT_PORT));
            isLocalIp = NetWorkUtils.hostname().equals("localhost");
        } else {
            this.hostPort = NetWorkUtils.ip() + ":" + Utils.getInt(data.getConf().get(Config.NIMBUS_THRIFT_PORT));
            isLocalIp = NetWorkUtils.ip().equals("127.0.0.1");
        }
        try {
            if (isLocalIp) {
                throw new Exception("the hostname which nimbus get is localhost");
            }
        } catch (Exception e1) {
            LOG.error("failed to get nimbus host!", e1);
            throw new RuntimeException(e1);
        }

        // 更新 ZK 上的节点信息
        try {
            data.getStormClusterState().update_nimbus_slave(hostPort, data.uptime());
            data.getStormClusterState().update_nimbus_detail(hostPort, null);
        } catch (Exception e) {
            LOG.error("failed to register nimbus host!", e);
            throw new RuntimeException();
        }

        // 判断是否存在 leader，如果不存在这尝试成为 leader
        StormClusterState zkClusterState = data.getStormClusterState();
        try {
            if (!zkClusterState.leader_existed()) {
                this.tryToBeLeader(data.getConf());
            }
        } catch (Exception e) {
            LOG.error("failed to register nimbus details!", e);
            throw new RuntimeException();
        }
        try {
            if (!zkClusterState.leader_existed()) {
                this.tryToBeLeader(data.getConf());
            }
        } catch (Exception e1) {
            try {
                // 尝试两次均失败，则删除 ZK 上的信息
                data.getStormClusterState().unregister_nimbus_host(hostPort);
                data.getStormClusterState().unregister_nimbus_detail(hostPort);
            } catch (Exception e2) {
                LOG.info("remove registered nimbus information due to task errors");
            } finally {
                LOG.error("try to be leader error.", e1);
                throw new RuntimeException(e1);
            }
        }

        /*
         * 如果 nimbus 使用的是本地存储，则需要需要添加一个回调函数，
         * 这个回调函数执行当这个nimbus不是leader的时，对blob进行同步。
         * 此外还需要将那些active的blob存到ZK中，而将死掉的进行清除，
         * 毕竟本地模式存储无法保证一致性，所以需要ZK进行维护， 而hdfs自带容错机制，能保证数据的一致性
         */
        blobSyncCallback = new RunnableCallback() {
            @Override
            public void run() {
                blobSync();
            }
        };
        if (data.getBlobStore() instanceof LocalFsBlobStore) {
            try {
                // register call back for blob-store
                data.getStormClusterState().blobstore(blobSyncCallback);
                setupBlobstore();
            } catch (Exception e) {
                LOG.error("setup blob store error", e);
            }
        }
    }

    // sets up blobstore state for all current keys
    private void setupBlobstore() throws Exception {
        BlobStore blobStore = data.getBlobStore();
        StormClusterState clusterState = data.getStormClusterState();
        Set<String> localSetOfKeys = Sets.newHashSet(blobStore.listKeys());
        Set<String> allKeys = Sets.newHashSet(clusterState.active_keys());
        Set<String> localAvailableActiveKeys = Sets.intersection(localSetOfKeys, allKeys);
        // keys on local but not on zk, we will delete it
        Set<String> keysToDelete = Sets.difference(localSetOfKeys, allKeys);
        LOG.debug("deleting keys not on zookeeper {}", keysToDelete);
        for (String key : keysToDelete) {
            blobStore.deleteBlob(key);
        }
        LOG.debug("Creating list of key entries for blobstore inside zookeeper {} local {}",
                allKeys, localAvailableActiveKeys);
        for (String key : localAvailableActiveKeys) {
            int versionForKey = BlobStoreUtils.getVersionForKey(key, data.getNimbusHostPortInfo(), data.getConf());
            clusterState.setup_blobstore(key, data.getNimbusHostPortInfo(), versionForKey);
        }
    }

    public boolean isLeader(String zkMaster) {
        if (StringUtils.isBlank(zkMaster)) {
            return false;
        }

        if (hostPort.equalsIgnoreCase(zkMaster)) {
            return true;
        }

        // Two nimbus running on the same node isn't allowed
        // so just checks ip is enough here
        String[] part = zkMaster.split(":");
        return NetWorkUtils.equals(part[0], NetWorkUtils.ip());
    }

    /**
     * 首先判断当前保存在ZK上的集群中是否有leader，如果没有则选举当前nimbus为leader线程。
     * 如果有了leader线程，则需要判断是否跟当前的nimbus相同，如果不相同则停止当前的nimbus，
     * 毕竟已经有leader存在了。如果是相同的，则需要判断本地的状态中，如果还没有设置为leader，
     * 表明当前nimbus还没有进行初始化，则先设置nimbus为leader然后回调函数进行初始化，也就是调用init(conf)方法。
     * 获取一个端口（默认的端口是7621）用于构建HttpServer实例对象。可以用于处理和接受tcp连接，启动一个新的线程进行httpserver的监听。
     */
    @Override
    public void run() {
        LOG.info("Follower thread starts!");
        while (state) {
            StormClusterState zkClusterState = data.getStormClusterState();
            try {
                Thread.sleep(sleepTime);
                if (!zkClusterState.leader_existed()) {
                    // 不存在 leader，尝试成为 leader
                    this.tryToBeLeader(data.getConf());
                    continue;
                }

                String master = zkClusterState.get_leader_host();
                boolean isZkLeader = isLeader(master);
                if (isZkLeader) {
                    if (!data.isLeader()) {
                        zkClusterState.unregister_nimbus_host(hostPort);
                        zkClusterState.unregister_nimbus_detail(hostPort);
                        data.setLeader(true);
                        leaderCallback.execute();
                    }
                    continue;
                } else {
                    if (data.isLeader()) {
                        LOG.info("New zk master is " + master);
                        JStormUtils.halt_process(1, "Lost zk master node, halt process");
                        return;
                    }
                }

                // here the nimbus is not leader
                if (data.getBlobStore() instanceof LocalFsBlobStore) {
                    this.blobSync();
                }
                zkClusterState.update_nimbus_slave(hostPort, data.uptime());
                update_nimbus_detail();
            } catch (InterruptedException ignored) {
            } catch (Exception e) {
                if (state) {
                    LOG.error("Unknown exception ", e);
                }
            }
        }
        LOG.info("Follower thread has been closed!");
    }

    public void clean() {
        state = false;
    }

    private synchronized void blobSync() {
        if (!data.isLeader()) {
            try {
                BlobStore blobStore = data.getBlobStore();
                StormClusterState clusterState = data.getStormClusterState();
                Set<String> localKeys = Sets.newHashSet(blobStore.listKeys());
                Set<String> zkKeys = Sets.newHashSet(clusterState.blobstore(blobSyncCallback));
                BlobSynchronizer blobSynchronizer = new BlobSynchronizer(blobStore, data.getConf());
                blobSynchronizer.setNimbusInfo(data.getNimbusHostPortInfo());
                blobSynchronizer.setBlobStoreKeySet(localKeys);
                blobSynchronizer.setZookeeperKeySet(zkKeys);
                blobSynchronizer.syncBlobs();
            } catch (Exception e) {
                LOG.error("blob sync error", e);
            }
        }
    }

    private void tryToBeLeader(final Map conf) throws Exception {
        boolean allowed = this.check_nimbus_priority();
        if (allowed) {
            RunnableCallback masterCallback = new RunnableCallback() {
                @Override
                public void run() {
                    try {
                        tryToBeLeader(conf);
                    } catch (Exception e) {
                        LOG.error("tryToBeLeader error", e);
                        // 30???
                        JStormUtils.halt_process(30, "Cant't be master" + e.getMessage());
                    }
                }
            };
            LOG.info("This nimbus can be leader");
            data.getStormClusterState().try_to_be_leader(Cluster.MASTER_SUBTREE, hostPort, masterCallback);
        } else {
            LOG.info("This nimbus can't be leader");
        }
    }

    /**
     * Compared with other nimbus to get priority of this nimbus
     */
    private boolean check_nimbus_priority() throws Exception {
        int gap = this.update_nimbus_detail();
        if (gap == 0) {
            return true;
        }

        int left = SLAVE_NIMBUS_WAIT_TIME;
        while (left > 0) {
            LOG.info("nimbus.differ.count.zk is {}, so after {} seconds, nimbus will try to be leader!", gap, left);
            Thread.sleep(10 * 1000);
            left -= 10;
        }

        StormClusterState zkClusterState = data.getStormClusterState();

        List<String> followers = zkClusterState.list_dirs(Cluster.NIMBUS_SLAVE_DETAIL_SUBTREE, false);
        if (followers == null || followers.size() == 0) {
            return false;
        }

        for (String follower : followers) {
            if (follower != null && !follower.equals(hostPort)) {
                Map bMap = zkClusterState.get_nimbus_detail(follower, false);
                if (bMap != null) {
                    Object object = bMap.get(NIMBUS_DIFFER_COUNT_ZK);
                    if (object != null && (JStormUtils.parseInt(object)) < gap) {
                        LOG.info("Current node can't be leader, due to {} has higher priority", follower);
                        return false;
                    }
                }
            }
        }

        return true;
    }

    private int update_nimbus_detail() throws Exception {
        // update count = count of zk's binary files - count of nimbus's binary files
        StormClusterState zkClusterState = data.getStormClusterState();

        // if we use other blobstore, such as HDFS, all nimbus slave can be leader
        // but if we use local blobstore, we should count topologies files
        int diffCount = 0;
        if (data.getBlobStore() instanceof LocalFsBlobStore) {
            Set<String> keysOnZk = Sets.newHashSet(zkClusterState.active_keys());
            Set<String> keysOnLocal = Sets.newHashSet(data.getBlobStore().listKeys());
            // we count number of keys which is on zk but not on local
            diffCount = Sets.difference(keysOnZk, keysOnLocal).size();
        }

        Map mtmp = zkClusterState.get_nimbus_detail(hostPort, false);
        if (mtmp == null) {
            mtmp = new HashMap();
        }
        mtmp.put(NIMBUS_DIFFER_COUNT_ZK, diffCount);
        zkClusterState.update_nimbus_detail(hostPort, mtmp);
        LOG.debug("update nimbus details " + mtmp);

        return diffCount;
    }

    /**
     * Check whether current node is master
     */
    private void checkOwnMaster() throws Exception {
        int retry_times = 10;

        StormClusterState zkClient = data.getStormClusterState();
        for (int i = 0; i < retry_times; i++, JStormUtils.sleepMs(sleepTime)) {

            if (!zkClient.leader_existed()) {
                continue;
            }

            String zkHost = zkClient.get_leader_host();
            if (hostPort.equals(zkHost)) {
                // current process own master
                return;
            }
            LOG.warn("Current nimbus has started thrift, but fail to set as leader in zk:" + zkHost);
        }

        String err = "Current nimbus failed to set as leader in zk, halting process";
        LOG.error(err);
        JStormUtils.halt_process(0, err);

    }

}
