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

package com.alibaba.jstorm.daemon.nimbus;

import backtype.storm.utils.Utils;
import com.alibaba.jstorm.cache.JStormCache;
import com.alibaba.jstorm.cache.RocksDBCache;
import com.alibaba.jstorm.cache.TimeoutMemCache;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.utils.OSInfo;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * In the old design, DBCache will cache all taskInfo/taskErrors, this will be useful for huge topology
 * But the latest zk design, taskInfo is only one znode, taskErros has few znode
 * So remove them from DBCache Skip timely refresh taskInfo/taskErrors
 */
public class NimbusCache {
    private static final Logger LOG = LoggerFactory.getLogger(NimbusCache.class);

    public static final String TIMEOUT_MEM_CACHE_CLASS = TimeoutMemCache.class.getName();
    public static final String ROCKS_DB_CACHE_CLASS = RocksDBCache.class.getName();

    protected JStormCache memCache;
    protected JStormCache dbCache;
    protected StormClusterState zkCluster;

    /**
     * 对于本地以及 linux、mac 以外的平台均采用 TimeoutMemCache，
     * 此外如果指定了缓存实现则使用自定义缓存，否则使用 rocksdb 作为缓存实现
     *
     * @param conf
     * @return
     */
    public String getNimbusCacheClass(Map conf) {
        boolean isLinux = OSInfo.isLinux();
        boolean isMac = OSInfo.isMac();
        boolean isLocal = StormConfig.local_mode(conf);

        if (isLocal) {
            return TIMEOUT_MEM_CACHE_CLASS;
        }

        if (!isLinux && !isMac) {
            return TIMEOUT_MEM_CACHE_CLASS;
        }

        // 获取 ${nimbus.cache.class} 配置项指定的缓存实现类
        String nimbusCacheClass = ConfigExtension.getNimbusCacheClass(conf);
        if (!StringUtils.isBlank(nimbusCacheClass)) {
            return nimbusCacheClass;
        }

        return ROCKS_DB_CACHE_CLASS;
    }

    public NimbusCache(Map conf, StormClusterState zkCluster) {
        super();

        // 获取本地缓存的具体实现类
        String dbCacheClass = this.getNimbusCacheClass(conf);
        LOG.info("NimbusCache db cache will use {}", dbCacheClass);

        try {
            dbCache = (JStormCache) Utils.newInstance(dbCacheClass);

            String dbDir = StormConfig.masterDbDir(conf);
            // 设置本地缓存数据存放目录
            conf.put(RocksDBCache.ROCKSDB_ROOT_DIR, dbDir); // ${storm.local.dir}/nimbus/rocksdb
            // 是否在 nimbus 启动时清空数据，默认为 true
            conf.put(RocksDBCache.ROCKSDB_RESET, ConfigExtension.getNimbusCacheReset(conf));
            dbCache.init(conf);
            if (dbCache instanceof TimeoutMemCache) {
                memCache = dbCache;
            } else {
                memCache = new TimeoutMemCache();
                memCache.init(conf);
            }
        } catch (UnsupportedClassVersionError e) {
            if (e.getMessage().contains("Unsupported major.minor version")) {
                LOG.error("!!!Please update jdk version to 7 or higher!!!");
            }
            LOG.error("Failed to create NimbusCache!", e);
            throw new RuntimeException(e);
        } catch (Exception e) {
            LOG.error("Failed to create NimbusCache!", e);
            throw new RuntimeException(e);
        }

        this.zkCluster = zkCluster;
    }

    public JStormCache getMemCache() {
        return memCache;
    }

    public JStormCache getDbCache() {
        return dbCache;
    }

    public void cleanup() {
        dbCache.cleanup();

    }
}
