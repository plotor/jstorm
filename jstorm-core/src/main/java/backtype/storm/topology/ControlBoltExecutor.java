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

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * IControlBolt 到 IRichBolt 的适配器
 *
 * @author JohnFang (xiaojian.fxj@alibaba-inc.com).
 */
public class ControlBoltExecutor implements IRichBolt {

    private static final long serialVersionUID = -8796519996526343665L;

    public static Logger LOG = LoggerFactory.getLogger(ControlBoltExecutor.class);

    private IControlBolt _bolt;

    public ControlBoltExecutor(IControlBolt bolt) {
        _bolt = bolt;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        _bolt.declareOutputFields(declarer);
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        _bolt.prepare(stormConf, context, new ControlOutputCollector(collector));

    }

    @Override
    public void execute(Tuple input) {
        _bolt.execute(input);
    }

    @Override
    public void cleanup() {
        _bolt.cleanup();
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return _bolt.getComponentConfiguration();
    }

}