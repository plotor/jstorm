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

package com.alibaba.jstorm.daemon.supervisor;

import backtype.storm.Config;
import backtype.storm.utils.LocalState;
import backtype.storm.utils.Utils;
import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.Common;
import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.daemon.worker.LocalAssignment;
import com.alibaba.jstorm.utils.JStormServerUtils;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * supervisor heartbeat, just write SupervisorInfo to ZK
 *
 * @author Johnfang (xiaojian.fxj@alibaba-inc.com)
 */
class Heartbeat extends RunnableCallback {

    private static final Logger LOG = LoggerFactory.getLogger(Heartbeat.class);

    private Map<Object, Object> conf;
    private StormClusterState stormClusterState;
    private String supervisorId;
    private String myHostName;
    private final int startTime;
    private final int frequency;

    private SupervisorInfo supervisorInfo;
    private AtomicBoolean hbUpdateTrigger;
    protected volatile HealthStatus healthStatus;

    private LocalState localState;

    @SuppressWarnings({"rawtypes", "unchecked"})
    public Heartbeat(Map conf, StormClusterState stormClusterState, String supervisorId, LocalState localState) {
        String myHostName = JStormServerUtils.getHostName(conf);
        this.stormClusterState = stormClusterState;
        this.supervisorId = supervisorId;
        this.conf = conf;
        this.myHostName = myHostName;
        this.startTime = TimeUtils.current_time_secs();
        this.frequency = JStormUtils.parseInt(conf.get(Config.SUPERVISOR_HEARTBEAT_FREQUENCY_SECS));
        this.hbUpdateTrigger = new AtomicBoolean(true);
        this.localState = localState;
        this.healthStatus = HealthStatus.INFO;

        /*
         * 初始化 supervisor 信息，包括
         *  - 计算可用的端口号集合
         *  - 当前 JStorm 版本、build 时间
         */
        this.initSupervisorInfo(conf);

        LOG.info("Successfully inited supervisor heartbeat thread, " + supervisorInfo);
    }

    /**
     * 初始化 supervisor 信息，包括
     * - 计算可用的端口号集合
     * - 当前 JStorm 版本、build 时间
     *
     * @param conf
     */
    private void initSupervisorInfo(Map conf) {
        // 基于 CPU 核心数和物理内存计算并返回允许的端口数，并从基础端口开始累加（默认为 6800）
        Set<Integer> portList = JStormUtils.getDefaultSupervisorPortList(conf);
        if (!StormConfig.local_mode(conf)) {
            try {
                boolean isLocalIP = myHostName.equals("127.0.0.1") || myHostName.equals("localhost");
                if (isLocalIP) {
                    throw new Exception("the hostname supervisor got is localhost");
                }
            } catch (Exception e1) {
                LOG.error("get supervisor host error!", e1);
                throw new RuntimeException(e1);
            }
            supervisorInfo = new SupervisorInfo(myHostName, supervisorId, portList, conf);
        } else {
            // 本地模式
            supervisorInfo = new SupervisorInfo(myHostName, supervisorId, portList, conf);
        }

        supervisorInfo.setVersion(Utils.getVersion());
        String buildTs = Utils.getBuildTime();
        supervisorInfo.setBuildTs(buildTs);
        LOG.info("jstorm version:{}, build ts:{}", supervisorInfo.getVersion(), supervisorInfo.getBuildTs());
    }

    @SuppressWarnings("unchecked")
    public void update() {
        // 更新最新一次更新 SupervisorInfo 数据的时间
        supervisorInfo.setTimeSecs(TimeUtils.current_time_secs());
        // 更新截止上次心跳更新时的启动时间
        supervisorInfo.setUptimeSecs(TimeUtils.current_time_secs() - startTime);

        // 依据具体配置和资源占用，调整端口号列表
        this.updateSupervisorInfo();

        try {
            // 将 supervisor 信息写入 ZK：supervisors/${supervisor_id}
            stormClusterState.supervisor_heartbeat(supervisorId, supervisorInfo);
        } catch (Exception e) {
            LOG.error("Failed to update SupervisorInfo to ZK", e);
        }
    }

    /**
     * 依据具体配置和资源占用，调整端口号列表
     */
    private void updateSupervisorInfo() {
        // 获取端口号列表（包含已经占用的）
        Set<Integer> portList = this.calculateCurrentPortList();
        LOG.debug("portList : {}", portList);
        supervisorInfo.setWorkerPorts(portList);
    }

    public void updateHealthStatus(HealthStatus status) {
        this.healthStatus = status;
    }

    public HealthStatus getHealthStatus() {
        return healthStatus;
    }

    @Override
    public Object getResult() {
        return frequency;
    }

    @Override
    public void run() {
        boolean updateHb = hbUpdateTrigger.getAndSet(false);
        if (updateHb) {
            this.update();
        }
    }

    public int getStartTime() {
        return startTime;
    }

    public SupervisorInfo getSupervisorInfo() {
        return supervisorInfo;
    }

    public void updateHbTrigger(boolean update) {
        hbUpdateTrigger.set(update);
    }

    /**
     * 获取端口号列表（包含已经占用的）
     *
     * @return
     */
    private Set<Integer> calculateCurrentPortList() {
        // 基于 CPU 核心数和物理内存计算并返回允许的端口数，并从基础端口开始累加（默认为 6800）
        Set<Integer> defaultPortList = JStormUtils.getDefaultSupervisorPortList(conf);

        // 获取已经使用的端口号
        Set<Integer> usedList;
        try {
            usedList = this.getLocalAssignmentPortList();
        } catch (IOException e) {
            supervisorInfo.setErrorMessage(null);
            return defaultPortList;
        }

        // 获取可用的端口号数目
        int availablePortNum = this.calculateAvailablePortNumber(defaultPortList, usedList);

        if (availablePortNum >= (defaultPortList.size() - usedList.size())) {
            supervisorInfo.setErrorMessage(null);
            return defaultPortList;
        } else {
            List<Integer> freePortList = new ArrayList<>(defaultPortList);
            freePortList.removeAll(usedList);
            Collections.sort(freePortList);
            Set<Integer> portList = new HashSet<>(usedList); // 已经使用的排在前面
            for (int i = 1; i <= availablePortNum; i++) {
                portList.add(freePortList.get(i));
            }
            supervisorInfo.setErrorMessage("Supervisor is lack of resources, " +
                    "reduce the number of workers from " + defaultPortList.size() +
                    " to " + (usedList.size() + availablePortNum));
            return portList;
        }
    }

    /**
     * 计算可用的端口号数目，
     * 如果满足（非 linux || CPU 核心数小于等于 4 || 不允许自动调整 slot）则直接以计划端口号数目减去已经占用的端口数目
     * 否则会基于 CPU 和 内存使用率进行进一步计算
     *
     * @param defaultList 计划端口号列表
     * @param usedList 已经使用的端口号
     * @return
     */
    private int calculateAvailablePortNumber(Set<Integer> defaultList, Set<Integer> usedList) {
        if (healthStatus.isMoreSeriousThan(HealthStatus.WARN)) {
            LOG.warn("Due to no enough resource, limit supervisor's ports and block scheduling");
            // set free port number to zero
            return 0;
        }

        // 获取 CPU 核心数
        int vcores = JStormUtils.getNumProcessors();
        // 获取 CPU 使用率
        double cpuUsage = JStormUtils.getTotalCpuUsage();

        // do not adjust port list if match the following conditions
        if (cpuUsage <= 0.0  // non-linux,
                || vcores <= 4   // machine configuration is too low
                || !ConfigExtension.isSupervisorEnableAutoAdjustSlots(conf) // auto adjust is disabled
                ) {
            // 非 linux || CPU 核心数小于等于 4 || 不允许自动调整 slot
            return defaultList.size() - usedList.size();
        }

        long freeMemory = JStormUtils.getFreePhysicalMem() * 1024L;     // in Byte
        long reserveMemory = ConfigExtension.getStormMachineReserveMem(conf);
        Long availablePhysicalMemorySize = freeMemory - reserveMemory;
        int reserveCpuUsage = ConfigExtension.getStormMachineReserveCpuPercent(conf);

        if (availablePhysicalMemorySize < 0 || cpuUsage + reserveCpuUsage > 100D) {
            // memory is not enough, or cpu is not enough, set free ports number to zero
            return 0;
        } else {
            int availableCpuNum = (int) Math.round((100 - cpuUsage) / 100 * vcores);
            return JStormUtils.getSupervisorPortNum(conf, availableCpuNum, availablePhysicalMemorySize);
        }
    }

    /**
     * 获取已经分配的端口号
     *
     * @return
     * @throws IOException
     */
    private Set<Integer> getLocalAssignmentPortList() throws IOException {
        Map<Integer, LocalAssignment> localAssignment;
        try {
            localAssignment = (Map<Integer, LocalAssignment>) localState.get(Common.LS_LOCAL_ASSIGNMENTS); // ${local-assignments}
        } catch (IOException e) {
            LOG.error("get LS_LOCAL_ASSIGNMENTS of localState failed .");
            throw e;
        }
        if (localAssignment == null) {
            return new HashSet<>();
        }
        return localAssignment.keySet();
    }
}
