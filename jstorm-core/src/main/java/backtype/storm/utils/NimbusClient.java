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

package backtype.storm.utils;

import backtype.storm.Config;
import backtype.storm.generated.Nimbus;
import backtype.storm.security.auth.ThriftClient;
import backtype.storm.security.auth.ThriftConnectionType;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class NimbusClient extends ThriftClient {

    private static final Logger LOG = LoggerFactory.getLogger(NimbusClient.class);

    private Nimbus.Client _client;
    private static String clientVersion = Utils.getVersion();

    @SuppressWarnings("unchecked")
    public static NimbusClient getConfiguredClient(Map conf) {
        return getConfiguredClient(conf, null);
    }

    @SuppressWarnings("unchecked")
    public static NimbusClient getConfiguredClient(Map conf, Integer timeout) {
        return getConfiguredClientAs(conf, timeout, null);
    }

    public static NimbusClient getConfiguredClientAs(Map conf, String asUser) {
        return getConfiguredClientAs(conf, null, asUser);
    }

    /**
     * 客户端与服务端版本匹配校验
     *
     * @param client
     */
    public static void checkVersion(NimbusClient client) {
        String serverVersion;
        try {
            serverVersion = client.getClient().getVersion();
        } catch (TException e) {
            LOG.warn("Failed to get nimbus version ");
            return;
        }
        if (!clientVersion.equals(serverVersion)) {
            LOG.warn("Your client version:  " + clientVersion + " but nimbus version: " + serverVersion);
        }
    }

    public static NimbusClient getConfiguredClientAs(Map conf, Integer timeout, String asUser) {
        try {
            if (conf.containsKey(Config.STORM_DO_AS_USER)) { // storm.doAsUser
                if (asUser != null && !asUser.isEmpty()) {
                    LOG.warn("You have specified a doAsUser as param {} and a doAsParam as config, config will take precedence.",
                            asUser, conf.get(Config.STORM_DO_AS_USER));
                }
                // 配置中的 ${storm.doAsUser} 优先级高于参数
                asUser = (String) conf.get(Config.STORM_DO_AS_USER);
            }
            NimbusClient client = new NimbusClient(conf, null, null, timeout, asUser);
            // 验证客户端与服务端版本是否一致
            checkVersion(client);
            return client;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public NimbusClient(Map conf, String host, int port) throws TTransportException {
        this(conf, host, port, null);
    }

    public NimbusClient(Map conf, String host, int port, Integer timeout) throws TTransportException {
        super(conf, ThriftConnectionType.NIMBUS, host, port, timeout, null);
        _client = new Nimbus.Client(_protocol);
    }

    public NimbusClient(Map conf, String host, Integer port, Integer timeout, String asUser) throws TTransportException {
        super(conf, ThriftConnectionType.NIMBUS, host, port, timeout, asUser);
        _client = new Nimbus.Client(_protocol);
    }

    public NimbusClient(Map conf, String host) throws TTransportException {
        super(conf, ThriftConnectionType.NIMBUS, host, null, null, null);
        _client = new Nimbus.Client(_protocol);
    }

    public Nimbus.Client getClient() {
        return _client;
    }

}
