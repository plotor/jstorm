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

package backtype.storm.topology;

import backtype.storm.Config;
import backtype.storm.utils.Utils;
import com.alibaba.jstorm.utils.JStormUtils;

import java.util.HashMap;
import java.util.Map;

public abstract class BaseConfigurationDeclarer<T extends ComponentConfigurationDeclarer> implements ComponentConfigurationDeclarer<T> {

    private Map conf = Utils.readStormConfig();

    @Override
    public T addConfiguration(String config, Object value) {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(config, value);
        return this.addConfigurations(configMap);
    }

    @Override
    public T setDebug(boolean debug) {
        return this.addConfiguration(Config.TOPOLOGY_DEBUG, debug);
    }

    @Override
    public T setMaxTaskParallelism(Number val) {
        if (val != null) {
            val = val.intValue();
        }
        return this.addConfiguration(Config.TOPOLOGY_MAX_TASK_PARALLELISM, val);
    }

    @Override
    public T setMaxSpoutPending(Number val) {
        if (val != null) {
            val = val.intValue();
        }
        return this.addConfiguration(Config.TOPOLOGY_MAX_SPOUT_PENDING, val);
    }

    @Override
    public T setNumTasks(Number val) {
        if (val != null) {
            val = val.intValue();
        }
        return this.addConfiguration(Config.TOPOLOGY_TASKS, val);
    }

    @Override
    public T setMemoryLoad(Number onHeap) {
        return this.setMemoryLoad(onHeap, JStormUtils.parseDouble(conf.get(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB)));
    }

    @Override
    public T setMemoryLoad(Number onHeap, Number offHeap) {
        Map configMap = new HashMap();
        if (onHeap != null) {
            onHeap = onHeap.doubleValue();
            configMap.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB, onHeap);
        }
        if (offHeap != null) {
            offHeap = offHeap.doubleValue();
            configMap.put(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB, offHeap);
        }
        return this.addConfigurations(configMap);
    }

    @Override
    public T setCPULoad(Number amount) {
        Map configMap = new HashMap();
        if (amount != null) {
            configMap.put(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT, amount);
        }
        return this.addConfigurations(configMap);
    }
}
