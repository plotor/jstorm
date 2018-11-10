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

        // 判断是不是本地模式，对于本地模式不适用 HA 机制
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

        // 在 ZK 上注册一个 nimbus 从节点
        try {
            // 更新运行时间到 ZK:nimbus_slave/${hostPort} 节点
            data.getStormClusterState().update_nimbus_slave(hostPort, data.uptime());
            // 清空 ZK:nimbus_slave_detail/${hostPort} 节点
            data.getStormClusterState().update_nimbus_detail(hostPort, null);
        } catch (Exception e) {
            LOG.error("failed to register nimbus host!", e);
            throw new RuntimeException();
        }

        // 判断是否存在 leader，如果不存在这尝试成为 leader
        StormClusterState zkClusterState = data.getStormClusterState();
        try {
            // 如果不存在 leader(检查 ZK:nimbus_master 节点是否存在)，则尝试成为 leader
            if (!zkClusterState.leader_existed()) {
                this.tryToBeLeader(data.getConf());
            }
        } catch (Exception e) {
            LOG.error("failed to register nimbus details!", e);
            throw new RuntimeException();
        }
        try {
            // 检查 ZK:nimbus_master 节点是否存在来判定集群是否有 leader 节点
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
                // 如果是使用 LocalFsBlobStore 来存储 blobstore 相关数据则写入 ZK:blobstore 节点来保证数据一致性
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

    /**
     * 判断当前 nimbus 节点是否是 leader
     *
     * @param zkMaster
     * @return
     */
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

    @Override
    public void run() {
        LOG.info("Follower thread starts!");
        while (state) {
            StormClusterState zkClusterState = data.getStormClusterState();
            try {
                Thread.sleep(sleepTime); // 默认是 5 秒
                if (!zkClusterState.leader_existed()) {
                    // 不存在 leader，尝试成为 leader
                    this.tryToBeLeader(data.getConf());
                    continue;
                }
                // 集群已经存在 leader
                String master = zkClusterState.get_leader_host();
                // 判断当前 nimbus 节点是否是 leader
                boolean isZkLeader = this.isLeader(master);
                if (isZkLeader) {
                    if (!data.isLeader()) {
                        // 从候选从节点中删除当前节点的相关信息
                        zkClusterState.unregister_nimbus_host(hostPort);
                        zkClusterState.unregister_nimbus_detail(hostPort);
                        data.setLeader(true);
                        // 触发回调策略
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

                // 如果当前 nimbus 不是 leader，更新 blobstore 和从节点信息到 ZK
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
        // 依据候选 nimbus 从节点的优先级来决定当前 nimbus 从节点是否有资格尝试成为 leader
        boolean allowed = this.check_nimbus_priority();
        if (allowed) {
            // 回调策略再次尝试
            RunnableCallback masterCallback = new RunnableCallback() {
                @Override
                public void run() {
                    try {
                        tryToBeLeader(conf);
                    } catch (Exception e) {
                        LOG.error("tryToBeLeader error", e);
                        JStormUtils.halt_process(30, "Cant't be master" + e.getMessage());
                    }
                }
            };
            // 尝试成为 leader 节点
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
            Thread.sleep(10 * 1000); // 60 秒
            left -= 10;
        }

        StormClusterState zkClusterState = data.getStormClusterState();

        // 枚举 ZK:nimbus_slave_detail 下面的子路径
        List<String> followers = zkClusterState.list_dirs(Cluster.NIMBUS_SLAVE_DETAIL_SUBTREE, false);
        if (followers == null || followers.size() == 0) {
            // 没有从节点可用
            return false;
        }

        for (String follower : followers) {
            if (follower != null && !follower.equals(hostPort)) {
                // 获取指定 follower 节点的详细信息
                Map bMap = zkClusterState.get_nimbus_detail(follower, false);
                if (bMap != null) {
                    Object object = bMap.get(NIMBUS_DIFFER_COUNT_ZK); // 获取 ${nimbus.differ.count.zk} 数值
                    if (object != null && (JStormUtils.parseInt(object)) < gap) {
                        LOG.info("Current node can't be leader, due to {} has higher priority", follower);
                        return false;
                    }
                }
            }
        }

        return true;
    }

    /**
     * 更新 nimbus_slave_detail/${hostPort} 信息
     *
     * @return
     * @throws Exception
     */
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
        mtmp.put(NIMBUS_DIFFER_COUNT_ZK, diffCount); // ${nimbus.differ.count.zk}
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
