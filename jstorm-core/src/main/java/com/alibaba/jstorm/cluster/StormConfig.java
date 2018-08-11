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

package com.alibaba.jstorm.cluster;

import backtype.storm.Config;
import backtype.storm.generated.KeyNotFoundException;
import backtype.storm.generated.StormTopology;
import backtype.storm.nimbus.NimbusInfo;
import backtype.storm.utils.LocalState;
import backtype.storm.utils.Utils;
import com.alibaba.jstorm.blobstore.AtomicOutputStream;
import com.alibaba.jstorm.blobstore.BlobStore;
import com.alibaba.jstorm.blobstore.BlobStoreUtils;
import com.alibaba.jstorm.blobstore.LocalFsBlobStore;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.daemon.nimbus.NimbusData;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.LoadConf;
import com.alibaba.jstorm.utils.PathUtils;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StormConfig {
    private final static Logger LOG = LoggerFactory.getLogger(StormConfig.class);
    public final static String RESOURCES_SUBDIR = "resources";
    public final static String WORKER_DATA_SUBDIR = "worker_shared_data";

    public static final String FILE_SEPERATEOR = File.separator;

    public static String clojureConfigName(String name) {
        return name.toUpperCase().replace("_", "-");
    }

    public static Map read_storm_config() {
        return Utils.readStormConfig();
    }

    public static Map read_yaml_config(String name) {
        return LoadConf.findAndReadYaml(name, true, false);
    }

    public static Map read_default_config() {
        return Utils.readDefaultConfig();
    }

    public static List<Object> All_CONFIGS() {
        List<Object> rtn = new ArrayList<>();
        Config config = new Config();
        Class<?> ConfigClass = config.getClass();
        Field[] fields = ConfigClass.getFields();
        for (int i = 0; i < fields.length; i++) {
            try {
                Object obj = fields[i].get(null);
                rtn.add(obj);
            } catch (IllegalArgumentException | IllegalAccessException e) {
                LOG.error(e.getMessage(), e);
            }
        }
        return rtn;
    }

    public static HashMap<String, Object> getClassFields(Class<?> cls)
            throws IllegalArgumentException, IllegalAccessException {
        java.lang.reflect.Field[] list = cls.getDeclaredFields();
        HashMap<String, Object> rtn = new HashMap<>();
        for (java.lang.reflect.Field f : list) {
            String name = f.getName();
            rtn.put(name, f.get(null).toString());

        }
        return rtn;
    }

    public static String cluster_mode(Map conf) {
        return (String) conf.get(Config.STORM_CLUSTER_MODE);

    }

    /**
     * please use ConfigExtension.getClusterName(Map conf)
     */
    @Deprecated
    public static String cluster_name(Map conf) {
        return ConfigExtension.getClusterName(conf);
    }

    /**
     * 校验是否是本地模式
     *
     * @param conf
     * @return
     */
    public static boolean local_mode(Map conf) {
        String mode = (String) conf.get(Config.STORM_CLUSTER_MODE); // storm.cluster.mode: distributed or local
        if (mode != null) {
            if ("local".equals(mode)) {
                return true;
            }

            if ("distributed".equals(mode)) {
                return false;
            }
        }
        throw new IllegalArgumentException("Illegal cluster mode in conf:" + mode);
    }

    public static boolean try_local_mode(Map conf) {
        try {
            return local_mode(conf);
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * 验证当前是否是 distributed 运行模式
     */
    public static void validate_distributed_mode(Map<?, ?> conf) {
        if (StormConfig.local_mode(conf)) {
            // 不允许以 local 模式运行
            throw new IllegalArgumentException("Cannot start server in local mode!");
        }
    }

    public static void validate_local_mode(Map<?, ?> conf) {
        if (!StormConfig.local_mode(conf)) {
            throw new IllegalArgumentException("Cannot start server in distributed mode!");
        }

    }

    /**
     * 创建并返回 ${storm.local.dir}/workers
     *
     * @param conf
     * @return
     * @throws IOException
     */
    public static String worker_root(Map conf) throws IOException {
        String ret = String.valueOf(conf.get(Config.STORM_LOCAL_DIR)) + FILE_SEPERATEOR + "workers"; // ${storm.local.dir}/workers
        FileUtils.forceMkdir(new File(ret));
        return ret;
    }

    /**
     * 创建并返回 ${local_dir}/workers/${worker_id}
     *
     * @param conf
     * @param id
     * @return
     * @throws IOException
     */
    public static String worker_root(Map conf, String id) throws IOException {
        String ret = worker_root(conf) + FILE_SEPERATEOR + id;
        FileUtils.forceMkdir(new File(ret));
        return ret;
    }

    /**
     * 创建并返回 ${local_dir}/workers/${worker_id}/pids
     *
     * @param conf
     * @param id
     * @return
     * @throws IOException
     */
    public static String worker_pids_root(Map conf, String id) throws IOException {
        String ret = worker_root(conf, id) + FILE_SEPERATEOR + "pids";
        FileUtils.forceMkdir(new File(ret));
        return ret;
    }

    public static String worker_pid_path(Map conf, String id, String pid) throws IOException {
        return worker_pids_root(conf, id) + FILE_SEPERATEOR + pid;
    }

    /**
     * 创建并返回 ${local_dir}/workers/${worker_id}/heartbeats
     *
     * @param conf
     * @param id
     * @return
     * @throws IOException
     */
    public static String worker_heartbeats_root(Map conf, String id) throws IOException {
        String ret = worker_root(conf, id) + FILE_SEPERATEOR + "heartbeats";
        FileUtils.forceMkdir(new File(ret));
        return ret;
    }

    public static String default_worker_shared_dir(Map conf) throws IOException {
        String ret = String.valueOf(conf.get(Config.STORM_LOCAL_DIR)) + FILE_SEPERATEOR + WORKER_DATA_SUBDIR;

        FileUtils.forceMkdir(new File(ret));
        return ret;
    }

    private static String drpc_local_dir(Map conf) throws IOException {
        String ret = String.valueOf(conf.get(Config.STORM_LOCAL_DIR)) + FILE_SEPERATEOR + "drpc";
        FileUtils.forceMkdir(new File(ret));
        return ret;
    }

    /**
     * 创建并返回 ${storm.local.dir}/supervisor
     *
     * @param conf
     * @return
     * @throws IOException
     */
    private static String supervisor_local_dir(Map conf) throws IOException {
        String ret = String.valueOf(conf.get(Config.STORM_LOCAL_DIR)) + FILE_SEPERATEOR + "supervisor"; // ${storm.local.dir}/supervisor
        FileUtils.forceMkdir(new File(ret));
        return ret;
    }

    /**
     * ${storm.local.dir}/supervisor/stormdist
     *
     * @param conf
     * @return
     * @throws IOException
     */
    public static String supervisor_stormdist_root(Map conf) throws IOException {
        String ret = stormdist_path(supervisor_local_dir(conf));
        FileUtils.forceMkdir(new File(ret));
        return ret;
    }

    /**
     * ${storm.local.dir}/supervisor/stormdist/${topology_id}
     *
     * @param conf
     * @param topologyId
     * @return
     * @throws IOException
     */
    public static String supervisor_stormdist_root(Map conf, String topologyId) throws IOException {
        return supervisor_stormdist_root(conf) + FILE_SEPERATEOR + topologyId;
    }

    /**
     * Return supervisor's pid dir
     *
     * 创建并返回 ${storm.local.dir}/supervisor/pids
     */
    public static String supervisorPids(Map conf) throws IOException {
        String ret = supervisor_local_dir(conf) + FILE_SEPERATEOR + "pids"; // ${storm.local.dir}/supervisor/pids
        try {
            FileUtils.forceMkdir(new File(ret));
        } catch (IOException e) {
            LOG.error("Failed to create dir " + ret, e);
            throw e;
        }
        return ret;
    }

    /**
     * Return drpc's pid dir
     */
    public static String drpcPids(Map conf) throws IOException {
        String ret = drpc_local_dir(conf) + FILE_SEPERATEOR + "pids";
        try {
            FileUtils.forceMkdir(new File(ret));
        } catch (IOException e) {
            LOG.error("Failed to create dir " + ret, e);
            throw e;
        }
        return ret;
    }

    /**
     * Return supervisor's heartbeat dir for apsara (heartbeat read dir)
     *
     * @throws IOException
     */
    public static String supervisorHearbeatForContainer(Map conf) throws IOException {
        String ret = supervisor_local_dir(conf) + FILE_SEPERATEOR + "supervisor.heartbeat";
        try {
            FileUtils.forceMkdir(new File(ret));
        } catch (IOException e) {
            LOG.error("Failed to create dir " + ret, e);
            throw e;
        }
        return ret;
    }

    public static String master_stormjar_key(String topologyId) {
        return topologyId + "-stormjar.jar";
    }

    public static String master_stormjar_bak_key(String topologyId) {
        return topologyId + "-stormjar.jar.bak";
    }

    /**
     * ${topology_id}-stormcode.ser
     *
     * @param topologyId
     * @return
     */
    public static String master_stormcode_key(String topologyId) {
        return topologyId + "-stormcode.ser";
    }

    /**
     * ${topology_id}-stormcode.ser.bak
     *
     * @param topologyId
     * @return
     */
    public static String master_stormcode_bak_key(String topologyId) {
        return topologyId + "-stormcode.ser.bak";
    }

    /**
     * ${topology_id}-stormconf.ser
     *
     * @param topologyId
     * @return
     */
    public static String master_stormconf_key(String topologyId) {
        return topologyId + "-stormconf.ser";
    }

    public static String master_stormlib_key(String topologyId, String libName) {
        return topologyId + "-lib-" + libName;
    }

    public static String stormjar_path(String stormroot) {
        return stormroot + FILE_SEPERATEOR + "stormjar.jar";
    }

    public static String stormcode_path(String stormroot) {
        return stormroot + FILE_SEPERATEOR + "stormcode.ser";
    }

    /**
     * @param stormroot
     * @return
     */
    public static String stormconf_path(String stormroot) {
        return stormroot + FILE_SEPERATEOR + "stormconf.ser";
    }

    public static String stormlib_path(String stormroot, String libname) {
        return stormroot + FILE_SEPERATEOR + "lib" + FILE_SEPERATEOR + libname;
    }

    public static String stormlib_path(String stormroot) {
        return stormroot + FILE_SEPERATEOR + "lib";
    }

    public static String stormdist_path(String stormroot) {
        return stormroot + FILE_SEPERATEOR + "stormdist";
    }

    public static String supervisor_storm_resources_path(String stormroot) {
        return stormroot + FILE_SEPERATEOR + RESOURCES_SUBDIR;
    }

    public static String stormtmp_path(String stormroot) {
        return stormroot + FILE_SEPERATEOR + "tmp";
    }

    public static String stormts_path(String stormroot) {
        return stormroot + FILE_SEPERATEOR + "timestamp";
    }

    public static LocalState worker_state(Map conf, String id) throws IOException {
        // ${local_dir}/workers/${worker_id}/heartbeats
        String path = worker_heartbeats_root(conf, id);
        return new LocalState(path);
    }

    /**
     * 创建 ${storm.local.dir}/nimbus 目录
     *
     * @param conf
     * @return
     * @throws IOException
     */
    public static String masterLocalDir(Map conf) throws IOException {
        String ret = String.valueOf(conf.get(Config.STORM_LOCAL_DIR)) + FILE_SEPERATEOR + "nimbus"; // storm.local.dir: jstorm-local/nimbus
        try {
            FileUtils.forceMkdir(new File(ret));
        } catch (IOException e) {
            LOG.error("Failed to create dir " + ret, e);
            throw e;
        }
        return ret;
    }

    public static String metricLocalDir(Map conf) throws IOException {
        String ret = String.valueOf(conf.get(Config.STORM_LOCAL_DIR)) + FILE_SEPERATEOR + "metrics";
        try {
            FileUtils.forceMkdir(new File(ret));
        } catch (IOException e) {
            LOG.error("Failed to create dir " + ret, e);
            throw e;
        }
        return ret;
    }

    public static String masterStormdistRoot(Map conf) throws IOException {
        String ret = stormdist_path(masterLocalDir(conf));
        FileUtils.forceMkdir(new File(ret));
        return ret;
    }

    public static String masterStormdistRoot(Map conf, String topologyId) throws IOException {
        return masterStormdistRoot(conf) + FILE_SEPERATEOR + topologyId;
    }

    public static String masterStormTmpRoot(Map conf) throws IOException {
        String ret = stormtmp_path(masterLocalDir(conf));
        FileUtils.forceMkdir(new File(ret));
        return ret;
    }

    public static String masterStormTmpRoot(Map conf, String topologyId) throws IOException {
        return masterStormTmpRoot(conf) + FILE_SEPERATEOR + topologyId;
    }

    /**
     * ${storm.local.dir}/nimbus/inbox
     *
     * @param conf
     * @return
     * @throws IOException
     */
    public static String masterInbox(Map conf) throws IOException {
        String ret = masterLocalDir(conf) + FILE_SEPERATEOR + "inbox"; // ${storm.local.dir}/nimbus/inbox
        try {
            FileUtils.forceMkdir(new File(ret));
        } catch (IOException e) {
            LOG.error("Failed to create dir " + ret, e);
            throw e;
        }
        return ret;
    }

    /**
     * 创建 ${storm.local.dir}/nimbus/ininumbus 目录
     *
     * @param conf
     * @return
     * @throws IOException
     */
    public static String masterInimbus(Map conf) throws IOException {
        String ret = masterLocalDir(conf) + FILE_SEPERATEOR + "ininumbus";
        try {
            FileUtils.forceMkdir(new File(ret));
        } catch (IOException e) {
            LOG.error("Failed to create dir " + ret, e);
            throw e;
        }
        return ret;
    }

    /**
     * 创建并返回 创建 ${storm.local.dir}/nimbus/pids 目录
     */
    public static String masterPids(Map conf) throws IOException {
        String ret = masterLocalDir(conf) + FILE_SEPERATEOR + "pids"; // jstorm-local/nimbus/pids
        try {
            FileUtils.forceMkdir(new File(ret));
        } catch (IOException e) {
            LOG.error("Failed to create dir " + ret, e);
            throw e;
        }
        return ret;
    }

    /**
     * Return nimbus's heartbeat dir for apsara
     * 创建 ${storm.local.dir}/nimbus/nimbus.heartbeat
     */
    public static String masterHearbeatForContainer(Map conf) throws IOException {
        String ret = masterLocalDir(conf) + FILE_SEPERATEOR + "nimbus.heartbeat"; // ${storm.local.dir}/nimbus/nimbus.heartbeat
        try {
            FileUtils.forceMkdir(new File(ret));
        } catch (IOException e) {
            LOG.error("Failed to create dir " + ret, e);
            throw e;
        }
        return ret;
    }

    public static String masterDbDir(Map conf) throws IOException {
        return masterLocalDir(conf) + FILE_SEPERATEOR + "rocksdb"; // ${storm.local.dir}/nimbus/rocksdb
    }

    public static String metricDbDir(Map conf) throws IOException {
        return metricLocalDir(conf) + FILE_SEPERATEOR + "rocksdb";
    }

    /**
     * 创建 ${storm.local.dir}/supervisor/tmp
     *
     * @param conf
     * @return
     * @throws IOException
     */
    public static String supervisorTmpDir(Map conf) throws IOException {
        String ret = null;
        try {
            ret = supervisor_local_dir(conf) + FILE_SEPERATEOR + "tmp"; // ${storm.local.dir}/supervisor/tmp
            FileUtils.forceMkdir(new File(ret));
        } catch (IOException e) {
            LOG.error("Failed to create dir " + ret, e);
            throw e;
        }
        return ret;
    }

    /**
     * 创建并返回 {@link LocalState} 实例，以 ${storm.local.dir}/supervisor/localstate 作为存储路径
     *
     * @param conf
     * @return
     * @throws IOException
     */
    public static LocalState supervisorState(Map conf) throws IOException {
        LocalState localState;
        try {
            // ${storm.local.dir}/supervisor/localstate
            String localstateDir = supervisor_local_dir(conf) + FILE_SEPERATEOR + "localstate";
            FileUtils.forceMkdir(new File(localstateDir));
            localState = new LocalState(localstateDir);
        } catch (IOException e) {
            LOG.error("Failed to create supervisor LocalState", e);
            throw e;
        }
        return localState;
    }

    /**
     * merge storm conf into cluster conf
     *
     * 加载 topology 配置信息
     */
    public static Map read_supervisor_topology_conf(Map conf, String topologyId) throws IOException {
        // ${storm.local.dir}/supervisor/stormdist/${topology_id}
        String topologyRoot = StormConfig.supervisor_stormdist_root(conf, topologyId);
        // ${storm.local.dir}/supervisor/stormdist/${topology_id}/stormconf.ser
        String confPath = StormConfig.stormconf_path(topologyRoot);
        return (Map) readLocalObject(topologyId, confPath);
    }

    public static StormTopology read_supervisor_topology_code(Map conf, String topologyId) throws IOException {
        String topologyRoot = StormConfig.supervisor_stormdist_root(conf, topologyId);
        String codePath = StormConfig.stormcode_path(topologyRoot);
        return (StormTopology) readLocalObject(topologyId, codePath);
    }

    /**
     * 获取 STORM-LOCAL-DIR/supervisor/stormdist/ 目录下面所有的 topology 名称
     *
     * @param conf
     * @return
     * @throws IOException
     */
    @SuppressWarnings("rawtypes")
    public static List<String> get_supervisor_toplogy_list(Map conf) throws IOException {
        // get the path: STORM-LOCAL-DIR/supervisor/stormdist/
        String path = StormConfig.supervisor_stormdist_root(conf);
        return PathUtils.read_dir_contents(path);
    }

    /**
     * 读取指定 topology 的配置信息
     * ${jstorm-local/nimbus/stormlist/{topology_id}/stormconf.ser}
     *
     * @param topologyId
     * @param blobStore
     * @return
     * @throws IOException
     * @throws KeyNotFoundException
     */
    public static Map read_nimbus_topology_conf(String topologyId, BlobStore blobStore) throws IOException, KeyNotFoundException {
        return Utils.javaDeserialize(
                // {topology_id}-stormconf.ser
                blobStore.readBlob(master_stormconf_key(topologyId)), Map.class);
    }

    public static void write_nimbus_topology_conf(String topologyId, Map topoConf, NimbusData data)
            throws Exception {
        String confKey = master_stormconf_key(topologyId);
        AtomicOutputStream out = data.getBlobStore().updateBlob(confKey);
        out.write(Utils.serialize(topoConf));
        out.close();
        if (data.getBlobStore() instanceof LocalFsBlobStore) {
            NimbusInfo nimbusInfo = data.getNimbusHostPortInfo();
            int versionForKey = BlobStoreUtils.getVersionForKey(confKey, nimbusInfo, data.getConf());
            data.getStormClusterState().setup_blobstore(confKey, nimbusInfo, versionForKey);
        }
    }

    public static Map read_nimbusTmp_topology_conf(Map conf, String topologyId) throws IOException {
        String topologyRoot = StormConfig.masterStormTmpRoot(conf, topologyId);
        return read_topology_conf(topologyRoot, topologyId);
    }

    public static Map read_topology_conf(String topologyRoot, String topologyId) throws IOException {
        String readFile = StormConfig.stormconf_path(topologyRoot);
        return (Map) readLocalObject(topologyId, readFile);
    }

    /**
     * 读取指定 topology 的序列化结构信息
     * 位于：jstorm-local/nimbus/stormlist/{topology_id}/stormcode.ser
     *
     * @param topologyId
     * @param blobStore
     * @return
     * @throws IOException
     * @throws KeyNotFoundException
     */
    public static StormTopology read_nimbus_topology_code(String topologyId, BlobStore blobStore) throws IOException, KeyNotFoundException {
        return Utils.javaDeserialize(
                // {topology_id}-stormcode.ser
                blobStore.readBlob(master_stormcode_key(topologyId)), StormTopology.class);
    }

    public static void write_nimbus_topology_code(String topologyId, byte[] data, NimbusData nimbusData) throws Exception {
        String codeKey = master_stormcode_key(topologyId);
        AtomicOutputStream out = nimbusData.getBlobStore().updateBlob(codeKey);
        out.write(data);
        out.close();
        if (nimbusData.getBlobStore() instanceof LocalFsBlobStore) {
            NimbusInfo nimbusInfo = nimbusData.getNimbusHostPortInfo();
            int versionForKey = BlobStoreUtils.getVersionForKey(codeKey, nimbusInfo, nimbusData.getConf());
            nimbusData.getStormClusterState().setup_blobstore(codeKey, nimbusInfo, versionForKey);
        }
    }

    public static long read_supervisor_topology_timestamp(Map conf, String topologyId) throws IOException {
        String stormRoot = supervisor_stormdist_root(conf, topologyId);
        String timeStampPath = stormts_path(stormRoot);

        byte[] data = FileUtils.readFileToByteArray(new File(timeStampPath));
        return JStormUtils.bytesToLong(data);
    }

    /**
     * 记录时间戳到 ${storm.local.dir}/supervisor/stormdist/${topology_id}/timestamp
     *
     * @param conf
     * @param topologyId
     * @param timeStamp
     * @throws IOException
     */
    public static void write_supervisor_topology_timestamp(Map conf, String topologyId, long timeStamp) throws IOException {
        // ${storm.local.dir}/supervisor/stormdist/${topology_id}
        String stormRoot = supervisor_stormdist_root(conf, topologyId);
        // ${storm.local.dir}/supervisor/stormdist/${topology_id}/timestamp
        String timeStampPath = stormts_path(stormRoot);

        byte[] data = JStormUtils.longToBytes(timeStamp);
        FileUtils.writeByteArrayToFile(new File(timeStampPath), data);
    }

    public static long read_nimbus_topology_timestamp(Map conf, String topologyId) throws IOException {
        String stormRoot = masterStormdistRoot(conf, topologyId);
        String timeStampPath = stormts_path(stormRoot);

        byte[] data = FileUtils.readFileToByteArray(new File(timeStampPath));
        return JStormUtils.bytesToLong(data);
    }

    public static void write_nimbus_topology_timestamp(Map conf, String topologyId, long timeStamp) throws IOException {
        String stormRoot = masterStormdistRoot(conf, topologyId);
        String timeStampPath = stormts_path(stormRoot);

        byte[] data = JStormUtils.longToBytes(timeStamp);
        FileUtils.writeByteArrayToFile(new File(timeStampPath), data);
    }

    @SuppressWarnings("unchecked")
    public static Object readLocalObject(String topologyId, String readFile) throws IOException {
        String errMsg = "Failed to get topology configuration of " + topologyId + " file:" + readFile;

        byte[] bconf = FileUtils.readFileToByteArray(new File(readFile));
        if (bconf == null) {
            errMsg += ", failed to read";
            LOG.error(errMsg);
            throw new IOException(errMsg);
        }

        Object ret;
        try {
            ret = Utils.javaDeserialize(bconf);
        } catch (Exception e) {
            errMsg += ", failed to serialize the data";
            LOG.error(errMsg);
            throw new IOException(errMsg);
        }

        return ret;
    }

    /**
     * 获取 ${storm.local.dir}/supervisor/stormdist/${topology_id} 最近一次修改时间
     *
     * @param conf
     * @param topologyId
     * @return
     * @throws IOException
     */
    public static long get_supervisor_topology_Bianrymodify_time(Map conf, String topologyId) throws IOException {
        // ${storm.local.dir}/supervisor/stormdist/${topology_id}
        String topologyRoot = StormConfig.supervisor_stormdist_root(conf, topologyId);
        File f = new File(topologyRoot);
        return f.lastModified();
    }

}
