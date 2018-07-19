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

package backtype.storm;

import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.Nimbus;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.SubmitOptions;
import backtype.storm.generated.TopologyAssignException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.BufferFileInputStream;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;
import com.alibaba.jstorm.client.ConfigExtension;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Use this class to submit topologies to run on the Storm cluster.
 * You should run your program with the "storm jar" command from the command-line, and then use this class to submit your topologies.
 */
@SuppressWarnings("unchecked")
public class StormSubmitter {

    public static Logger LOG = LoggerFactory.getLogger(StormSubmitter.class);

    private static Nimbus.Iface localNimbus = null;

    public static void setLocalNimbus(Nimbus.Iface localNimbusHandler) {
        StormSubmitter.localNimbus = localNimbusHandler;
    }

    /**
     * Submits a topology to run on the cluster. A topology runs forever or until explicitly(明确的) killed.
     *
     * @param name the name of the topology.
     * @param stormConf the topology-specific configuration. See {@link Config}.
     * @param topology the topology to execute.
     * @throws AlreadyAliveException    if a topology with this name is already running
     * @throws InvalidTopologyException if an invalid topology was submitted
     */
    public static void submitTopology(String name, Map stormConf, StormTopology topology) throws AlreadyAliveException, InvalidTopologyException {
        submitTopology(name, stormConf, topology, null);
    }

    public static void submitTopology(String name, Map stormConf, StormTopology topology, SubmitOptions opts, List<File> jarFiles)
            throws AlreadyAliveException, InvalidTopologyException {
        if (jarFiles == null) {
            jarFiles = new ArrayList<>();
        }
        Map<String, String> jars = new HashMap<>(jarFiles.size());
        List<String> names = new ArrayList<>(jarFiles.size());

        for (File f : jarFiles) {
            if (!f.exists()) {
                LOG.info(f.getName() + " does not exist: " + f.getAbsolutePath());
                continue;
            }
            jars.put(f.getName(), f.getAbsolutePath());
            names.add(f.getName());
        }
        LOG.info("Files: " + names + " will be loaded");
        stormConf.put(GenericOptionsParser.TOPOLOGY_LIB_PATH, jars);
        stormConf.put(GenericOptionsParser.TOPOLOGY_LIB_NAME, names);
        submitTopology(name, stormConf, topology, opts);
    }

    public static void submitTopology(String name, Map stormConf, StormTopology topology, SubmitOptions opts, ProgressListener listener) throws AlreadyAliveException, InvalidTopologyException {
        submitTopology(name, stormConf, topology, opts);
    }

    /**
     * Submits a topology to run on the cluster. A topology runs forever or until explicitly killed.
     *
     * @param name the name of the storm.
     * @param stormConf the topology-specific configuration. See {@link Config}.
     * @param topology the processing to execute.
     * @throws AlreadyAliveException    if a topology with this name is already running
     * @throws InvalidTopologyException if an invalid topology was submitted
     */
    public static void submitTopology(String name, Map stormConf, StormTopology topology, SubmitOptions opts) throws AlreadyAliveException, InvalidTopologyException {
        if (!Utils.isValidConf(stormConf)) {
            throw new IllegalArgumentException("Storm conf is not valid. Must be json-serializable");
        }

        // 添加配置（构建 topology 期间添加的、提交 topology 时传入的，以及命令行参数）
        Map userTotalConf = new HashMap();
        userTotalConf.putAll(TopologyBuilder.getStormConf()); // add the configuration generated during topology building
        userTotalConf.putAll(stormConf);
        userTotalConf.putAll(Utils.readCommandLineOpts());

        // 加载配置文件配置
        Map conf = Utils.readStormConfig();
        conf.putAll(stormConf);
        putUserInfo(conf, stormConf);

        try {
            String serConf = Utils.to_json(userTotalConf); // 转换成 json 形式
            if (localNimbus != null) {
                // 本地提交
                LOG.info("Submitting topology " + name + " in local mode");
                localNimbus.submitTopology(name, null, serConf, topology);
            } else {
                // 集群提交
                NimbusClient client = NimbusClient.getConfiguredClient(conf);
                try {
                    // 是否允许热部署？${topology.hot.deploy.enable}
                    boolean enableDeploy = ConfigExtension.getTopologyHotDeplogyEnable(userTotalConf);
                    // ${topology.upgrade}
                    boolean isUpgrade = ConfigExtension.isUpgradeTopology(userTotalConf);
                    // 是否允许动态更新
                    boolean dynamicUpdate = enableDeploy || isUpgrade;

                    if (topologyNameExists(client, conf, name) != dynamicUpdate) {
                        if (dynamicUpdate) {
                            // 动态更新，但是对应的 topology 不存在
                            throw new RuntimeException("Topology with name `" + name + "` does not exist on cluster");
                        } else {
                            // 提交 topology，但是对应的 topology 已经存在
                            throw new RuntimeException("Topology with name `" + name + "` already exists on cluster");
                        }
                    }

                    /*
                     * 1318 [main] INFO  backtype.storm.StormSubmitter - Jar not uploaded to master yet. Submitting jar...
                     * 1321 [main] INFO  backtype.storm.StormSubmitter - Uploading topology jar passport-rcs-swirler-1.0.0.jar to assigned location: /home/work/data/nimbus/inbox/stormjar-875b4616-11e1-4b58-9970-0b5f457166bf.jar
                     * 10822 [main] INFO  backtype.storm.StormSubmitter - Successfully uploaded topology jar to assigned location: /home/work/data/nimbus/inbox/stormjar-875b4616-11e1-4b58-9970-0b5f457166bf.jar
                     * 10822 [main] INFO  backtype.storm.StormSubmitter - Submitting topology passport-scribe-to-hbase-topology in distributed mode with conf {"prs_sth.bolt.spi.names":"action_log_extract, scribe_to_hbase","nimbus.host":"vru1-hadoop-storm01.awsde","topology.workers":8,"prs_zookeeper.host":"azvrusrv","topology.java.home":"\/opt\/soft\/jdk1.8.0\/","prs_nimbus.ip":"vru1-hadoop-storm01.awsde","prs_topology.java.home":"\/opt\/soft\/jdk1.8.0\/","prs_sth.cache.enable":"true","prs_kafka.zk.path":"\/kafka\/azvrusrv-hadoop\/brokers","prs_risk.control.state.bolt.parallelism":"20","prs_action.log.kafka.spout.parallelism":"8","prs_batch.emit.interval.millis":"500","prs_kafka.zk.host":"azvrusrv.zk.hadoop.srv:11000","prs_kafka.zk.spout.key.as":"\/azvrusrv-hadoop\/passport\/passport_antispam_stat","prs_risk.control.state.spout.parallelism":"8","topology.acker.executors":200,"prs_ack.executor.num":"200","prs_current.env":"azvrusrv","prs_kafka.zk.spout.key.al":"\/azvrusrv-hadoop\/passport\/passport_action","topology.max.spout.pending":400,"topology.kerberos.principal":"s_passport_mt_azru@XIAOMI.HADOOP","topology.trident.batch.emit.interval.millis":500,"prs_action.log.extract.bolt.parallelism":"40","prs_scribe.hbase.bolt.parallelism":"80","topology.kerberos.passwd":"WdzfASgmDJX0zGh","prs_sth.as.bolt.spi.names":"risk_control_state","prs_storm.worker.num":"8"}
                     * 12143 [main] INFO  backtype.storm.StormSubmitter - Finished submitting topology: passport-scribe-to-hbase-topology
                     */
                    submitJar(client, conf);
                    LOG.info("Submitting topology " + name + " in distributed mode with conf " + serConf);
                    if (opts != null) {
                        client.getClient().submitTopologyWithOpts(name, path, serConf, topology, opts);
                    } else {
                        // for backward compatibility
                        client.getClient().submitTopology(name, path, serConf, topology);
                    }
                } finally {
                    client.close();
                }
            }
            LOG.info("Finished submitting topology: " + name);
        } catch (InvalidTopologyException e) {
            LOG.warn("Topology submission exception", e);
            throw e;
        } catch (AlreadyAliveException e) {
            LOG.warn("Topology is already alive!", e);
            throw e;
        } catch (TopologyAssignException e) {
            LOG.warn("Failed to assign " + e.get_msg(), e);
            throw new RuntimeException(e);
        } catch (TException e) {
            LOG.warn("Failed to assign ", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Submits a topology to run on the cluster with a progress bar. A topology runs forever or until explicitly killed.
     *
     * @param name the name of the storm.
     * @param stormConf the topology-specific configuration. See {@link Config}.
     * @param topology the processing to execute.
     * @throws AlreadyAliveException    if a topology with this name is already running
     * @throws InvalidTopologyException if an invalid topology was submitted
     */

    public static void submitTopologyWithProgressBar(String name, Map stormConf, StormTopology topology) throws AlreadyAliveException, InvalidTopologyException {
        submitTopologyWithProgressBar(name, stormConf, topology, null);
    }

    /**
     * Submits a topology to run on the cluster with a progress bar. A topology runs forever or until explicitly killed.
     *
     * @param name the name of the storm.
     * @param stormConf the topology-specific configuration. See {@link Config}.
     * @param topology the processing to execute.
     * @param opts to manipulate the starting of the topology
     * @throws AlreadyAliveException    if a topology with this name is already running
     * @throws InvalidTopologyException if an invalid topology was submitted
     */

    public static void submitTopologyWithProgressBar(String name, Map stormConf, StormTopology topology, SubmitOptions opts) throws AlreadyAliveException, InvalidTopologyException {
        /**
         * progress bar is removed in jstorm
         */
        submitTopology(name, stormConf, topology, opts);
    }

    /**
     * 检查对应的 topology 名称是否存在
     *
     * @param client
     * @param conf
     * @param name
     * @return
     */
    public static boolean topologyNameExists(NimbusClient client, Map conf, String name) {
        try {
            client.getClient().getTopologyInfoByName(name);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private static String submittedJar = null;
    private static String path = null;

    /**
     * 上传 jar 到 nimbus
     *
     * @param client
     * @param conf
     */
    private static void submitJar(NimbusClient client, Map conf) {
        if (submittedJar == null) {
            try {
                LOG.info("Jar not uploaded to master yet. Submitting jar...");
                String localJar = System.getProperty("storm.jar");
                path = client.getClient().beginFileUpload();
                String[] pathCache = path.split("/");
                String uploadLocation = path + "/stormjar-" + pathCache[pathCache.length - 1] + ".jar";
                List<String> lib = (List<String>) conf.get(GenericOptionsParser.TOPOLOGY_LIB_NAME);
                Map<String, String> libPath = (Map<String, String>) conf.get(GenericOptionsParser.TOPOLOGY_LIB_PATH);
                if (lib != null && lib.size() != 0) {
                    for (String libName : lib) {
                        String jarPath = path + "/lib/" + libName;
                        client.getClient().beginLibUpload(jarPath);
                        submitJar(conf, libPath.get(libName), jarPath, client);
                    }
                } else {
                    if (localJar == null) {
                        // no lib, no client jar
                        throw new RuntimeException("No client app jar found, please upload it");
                    }
                }

                if (localJar != null) {
                    submittedJar = submitJar(conf, localJar, uploadLocation, client);
                } else {
                    // no client jar, but with lib jar
                    client.getClient().finishFileUpload(uploadLocation);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else {
            LOG.info("Jar has already been uploaded to master. Will not submit again.");
        }
    }

    public static String submitJar(Map conf, String localJar, String uploadLocation, NimbusClient client) {
        if (localJar == null) {
            throw new RuntimeException("Must submit topologies using the 'jstorm' client script so that " +
                    "StormSubmitter knows which jar to upload.");
        }

        try {
            LOG.info("Uploading topology jar " + localJar + " to assigned location: " + uploadLocation);
            int bufferSize = 512 * 1024;
            Object maxBufSizeObject = conf.get(Config.NIMBUS_THRIFT_MAX_BUFFER_SIZE);
            if (maxBufSizeObject != null) {
                bufferSize = Utils.getInt(maxBufSizeObject) / 2;
            }

            BufferFileInputStream is = new BufferFileInputStream(localJar, bufferSize);
            while (true) {
                byte[] toSubmit = is.read();
                if (toSubmit.length == 0) {
                    break;
                }
                client.getClient().uploadChunk(uploadLocation, ByteBuffer.wrap(toSubmit));
            }
            client.getClient().finishFileUpload(uploadLocation);
            LOG.info("Successfully uploaded topology jar to assigned location: " + uploadLocation);
            return uploadLocation;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void putUserInfo(Map conf, Map stormConf) {
        stormConf.put("user.group", conf.get("user.group"));
        stormConf.put("user.name", conf.get("user.name"));
        stormConf.put("user.password", conf.get("user.password"));
    }

    /**
     * Interface use to track progress of file upload
     */
    public interface ProgressListener {
        /**
         * called before file is uploaded
         *
         * @param srcFile - jar file to be uploaded
         * @param targetFile - destination file
         * @param totalBytes - total number of bytes of the file
         */
        void onStart(String srcFile, String targetFile, long totalBytes);

        /**
         * called whenever a chunk of bytes is uploaded
         *
         * @param srcFile - jar file to be uploaded
         * @param targetFile - destination file
         * @param bytesUploaded - number of bytes transferred so far
         * @param totalBytes - total number of bytes of the file
         */
        void onProgress(String srcFile, String targetFile, long bytesUploaded, long totalBytes);

        /**
         * called when the file is uploaded
         *
         * @param srcFile - jar file to be uploaded
         * @param targetFile - destination file
         * @param totalBytes - total number of bytes of the file
         */
        void onCompleted(String srcFile, String targetFile, long totalBytes);
    }
}