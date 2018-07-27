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

import backtype.storm.generated.Bolt;
import backtype.storm.generated.ComponentCommon;
import backtype.storm.generated.SpoutSpec;
import backtype.storm.generated.StateSpoutSpec;
import backtype.storm.generated.StormTopology;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@SuppressWarnings("unchecked")
public class ThriftTopologyUtils {

    /**
     * 获取 topology 所有组件的 ID
     *
     * @param topology
     * @return
     */
    public static Set<String> getComponentIds(StormTopology topology) {
        Set<String> ret = new HashSet<>();
        for (StormTopology._Fields fields : StormTopology.metaDataMap.keySet()) {
            Map<String, Object> componentMap = (Map<String, Object>) topology.getFieldValue(fields);
            ret.addAll(componentMap.keySet());
        }
        return ret;
    }

    /**
     * 获取当前 topology 的所有组件信息：
     * [bolt_id, Bolt]
     * [spout_id, SpoutSpec]
     * [spout_id, StateSpoutSpec]
     *
     * @param topology
     * @return
     */
    public static Map<String, Object> getComponents(StormTopology topology) {
        Map<String, Object> ret = new HashMap<>();
        for (StormTopology._Fields f : StormTopology.metaDataMap.keySet()) {
            Map<String, Object> componentMap = (Map<String, Object>) topology.getFieldValue(f);
            ret.putAll(componentMap);
        }
        return ret;
    }

    public static ComponentCommon getComponentCommon(StormTopology topology, String componentId) {
        for (StormTopology._Fields f : StormTopology.metaDataMap.keySet()) {
            Map<String, Object> componentMap = (Map<String, Object>) topology.getFieldValue(f);
            if (componentMap.containsKey(componentId)) {
                Object component = componentMap.get(componentId);
                if (component instanceof Bolt) {
                    return ((Bolt) component).get_common();
                }
                if (component instanceof SpoutSpec) {
                    return ((SpoutSpec) component).get_common();
                }
                if (component instanceof StateSpoutSpec) {
                    return ((StateSpoutSpec) component).get_common();
                }
                throw new RuntimeException("Unreachable code! No get_common conversion for component " + component);
            }
        }
        throw new IllegalArgumentException("Could not find component common for " + componentId);
    }
}
