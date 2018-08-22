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

package backtype.storm.task;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.generated.ComponentCommon;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Grouping;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.ThriftTopologyUtils;
import backtype.storm.utils.Utils;
import org.json.simple.JSONAware;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class GeneralTopologyContext implements JSONAware {

    private StormTopology _topology;
    private Map<Integer, String> _taskToComponent;
    private Map<String, List<Integer>> _componentToTasks;
    private Map<String, Map<String, Fields>> _componentToStreamToFields;
    private String _topologyId;
    protected Map _stormConf;
    protected int _topologyMasterId;

    // pass in componentToSortedTasks for the case of running tons of tasks in single executor
    public GeneralTopologyContext(StormTopology topology, Map stormConf, Map<Integer, String> taskToComponent,
                                  Map<String, List<Integer>> componentToSortedTasks,
                                  Map<String, Map<String, Fields>> componentToStreamToFields, String topologyId) {
        _topology = topology;
        _stormConf = stormConf;
        _taskToComponent = taskToComponent;
        _topologyId = topologyId;
        _componentToTasks = componentToSortedTasks;
        _componentToStreamToFields = componentToStreamToFields;
        _topologyMasterId = _componentToTasks.get("__topology_master").get(0);
    }

    /**
     * Gets the unique id assigned to this topology. The id is the storm name with a unique nonce appended to it.
     *
     * @return the topology id
     */
    public String getTopologyId() {
        return _topologyId;
    }

    /**
     * Please use the getTopologId() instead.
     *
     * @return the topology id
     */
    @Deprecated
    public String getStormId() {
        return _topologyId;
    }

    /**
     * Gets the Thrift object representing the topology.
     *
     * @return the Thrift definition representing the topology
     */
    public StormTopology getRawTopology() {
        return _topology;
    }

    /**
     * Gets the component id for the specified task id.
     * The component id maps to a component id specified for a Spout or Bolt in the topology definition.
     *
     * @param taskId the task id
     * @return the component id for the input task id
     */
    public String getComponentId(int taskId) {
        if (taskId == Constants.SYSTEM_TASK_ID) {
            return Constants.SYSTEM_COMPONENT_ID;
        } else {
            return _taskToComponent.get(taskId);
        }
    }

    /**
     * Gets the set of streams declared for the specified component.
     */
    public Set<String> getComponentStreams(String componentId) {
        return this.getComponentCommon(componentId).get_streams().keySet();
    }

    /**
     * Gets the task ids allocated for the given component id. The task ids are always returned in ascending order.
     */
    public List<Integer> getComponentTasks(String componentId) {
        List<Integer> ret = _componentToTasks.get(componentId);
        if (ret == null) {
            return new ArrayList<>();
        } else {
            return new ArrayList<>(ret);
        }
    }

    public List<Integer> getComponentsTasks(Set<String> componentIds) {
        List<Integer> ret = new ArrayList<>();
        for (String componentId : componentIds) {
            List<Integer> tasks = _componentToTasks.get(componentId);
            if (tasks != null) {
                ret.addAll(tasks);
            }
        }
        return ret;
    }

    public int getTaskIndexById(int taskId) {
        String componentId = this.getComponentId(taskId);
        List<Integer> tasks = new ArrayList<>(this.getComponentTasks(componentId));
        Collections.sort(tasks);
        for (int i = 0; i < tasks.size(); i++) {
            if (tasks.get(i) == taskId) {
                return i;
            }
        }
        throw new RuntimeException("Fatal: could not find this task id in this component");
    }

    /**
     * Gets the declared output fields for the specified component/stream.
     */
    public Fields getComponentOutputFields(String componentId, String streamId) {
        Fields ret = _componentToStreamToFields.get(componentId).get(streamId);
        if (ret == null) {
            throw new IllegalArgumentException("No output fields defined for component:stream " + componentId + ":" + streamId);
        }
        return ret;
    }

    /**
     * Gets the declared output fields for the specified global stream id.
     */
    public Fields getComponentOutputFields(GlobalStreamId id) {
        return this.getComponentOutputFields(id.get_componentId(), id.get_streamId());
    }

    /**
     * Gets the declared inputs to the specified component.
     *
     * @return A map from subscribed component/stream to the grouping subscribed with.
     */
    public Map<GlobalStreamId, Grouping> getSources(String componentId) {
        return this.getComponentCommon(componentId).get_inputs();
    }

    /**
     * Gets information about who is consuming the outputs of the specified component, and how.
     * 获取当前组件的下游组件 ID，以及消息分组方式
     *
     * struct GlobalStreamId {
     * 1: required string componentId; // 当前组件输入流来源组件 ID
     * 2: required string streamId; // 当前组件所输出的特定的流
     * }
     *
     * @param componentId 组件 ID
     * @return Map from stream id to component id to the Grouping used.
     */
    public Map<String, Map<String, Grouping>> getTargets(String componentId) {
        Map<String, Map<String, Grouping>> ret = new HashMap<>();
        // 遍历处理当前 topology 中所有的组件
        for (String otherComponentId : this.getComponentIds()) {
            // 获取当前组件从哪些 GlobalStreamId 以何种方式获取数据
            Map<GlobalStreamId, Grouping> inputs = this.getComponentCommon(otherComponentId).get_inputs();
            for (GlobalStreamId id : inputs.keySet()) {
                if (id.get_componentId().equals(componentId)) {
                    // 如果 id 是当前 componentId 的下游
                    Map<String, Grouping> curr = ret.get(id.get_streamId());
                    if (curr == null) {
                        curr = new HashMap<>();
                    }
                    curr.put(otherComponentId, inputs.get(id));
                    ret.put(id.get_streamId(), curr);
                }
            }
        }
        return ret;
    }

    @Override
    public String toJSONString() {
        Map obj = new HashMap();
        obj.put("task->component", _taskToComponent);
        return Utils.to_json(obj);
    }

    /**
     * Gets a map from task id to component id.
     */
    public Map<Integer, String> getTaskToComponent() {
        return _taskToComponent;
    }

    /**
     * Gets a list of all component ids in this topology
     */
    public Set<String> getComponentIds() {
        return ThriftTopologyUtils.getComponentIds(this.getRawTopology());
    }

    public ComponentCommon getComponentCommon(String componentId) {
        return ThriftTopologyUtils.getComponentCommon(this.getRawTopology(), componentId);
    }

    public int maxTopologyMessageTimeout() {
        Integer max = Utils.getInt(_stormConf.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS));
        for (String spout : this.getRawTopology().get_spouts().keySet()) {
            ComponentCommon common = this.getComponentCommon(spout);
            String jsonConf = common.get_json_conf();
            if (jsonConf != null) {
                Map conf = (Map) Utils.from_json(jsonConf);
                Object comp = conf.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS);
                if (comp != null) {
                    max = Math.max(Utils.getInt(comp), max);
                }
            }
        }
        return max;
    }

    public Map getStormConf() {
        return _stormConf;
    }

    public int getTopologyMasterId() {
        return _topologyMasterId;
    }
}