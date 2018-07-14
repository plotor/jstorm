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

import backtype.storm.spout.CheckPointState.Action;
import backtype.storm.spout.CheckpointSpout;
import backtype.storm.state.State;
import backtype.storm.state.StateFactory;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Wraps a {@link IStatefulBolt} and manages the state of the bolt.
 */
public class StatefulBoltExecutor<T extends State> extends CheckpointTupleForwarder {

    private static final long serialVersionUID = 119012272629901687L;

    private static final Logger LOG = LoggerFactory.getLogger(StatefulBoltExecutor.class);

    private final IStatefulBolt<T> bolt;
    private State state;
    private boolean boltInitialized = false;
    private List<Tuple> pendingTuples = new ArrayList<>();
    private List<Tuple> preparedTuples = new ArrayList<>();
    private AckTrackingOutputCollector collector;

    public StatefulBoltExecutor(IStatefulBolt<T> bolt) {
        super(bolt);
        this.bolt = bolt;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        // get the last successfully committed state from state store
        String namespace = context.getThisComponentId() + "-" + context.getThisTaskId();
        this.prepare(stormConf, context, collector, StateFactory.getState(namespace, stormConf, context));
    }

    // package access for unit tests
    void prepare(Map stormConf, TopologyContext context, OutputCollector collector, State state) {
        this.init(context, collector);
        this.collector = new AckTrackingOutputCollector(collector);
        bolt.prepare(stormConf, context, this.collector);
        this.state = state;
    }

    @Override
    protected void handleCheckpoint(Tuple checkpointTuple, Action action, long txid) {
        LOG.debug("handleCheckPoint with tuple {}, action {}, txid {}", checkpointTuple, action, txid);
        if (action == Action.PREPARE) {
            if (boltInitialized) {
                bolt.prePrepare(txid);
                state.prepareCommit(txid);
                preparedTuples.addAll(collector.ackedTuples());
            } else {
                /*
                 * May be the task restarted in the middle and the state needs be initialized.
                 * Fail fast and trigger recovery.
                 */
                LOG.debug("Failing checkpointTuple, PREPARE received when bolt state is not initialized.");
                collector.fail(checkpointTuple);
                return;
            }
        } else if (action == Action.COMMIT) {
            bolt.preCommit(txid);
            state.commit(txid);
            this.ack(preparedTuples);
        } else if (action == Action.ROLLBACK) {
            bolt.preRollback();
            state.rollback();
            this.fail(preparedTuples);
            this.fail(collector.ackedTuples());
        } else if (action == Action.INITSTATE) {
            if (!boltInitialized) {
                bolt.initState((T) state);
                boltInitialized = true;
                LOG.debug("{} pending tuples to process", pendingTuples.size());
                for (Tuple tuple : pendingTuples) {
                    this.doExecute(tuple);
                }
                pendingTuples.clear();
            } else {
                LOG.debug("Bolt state is already initialized, ignoring tuple {}, action {}, txid {}",
                        checkpointTuple, action, txid);
            }
        }
        collector.emit(CheckpointSpout.CHECKPOINT_STREAM_ID, checkpointTuple, new Values(txid, action));
        collector.delegate.ack(checkpointTuple);
    }

    @Override
    protected void handleTuple(Tuple input) {
        if (boltInitialized) {
            this.doExecute(input);
        } else {
            LOG.debug("Bolt state not initialized, adding tuple {} to pending tuples", input);
            pendingTuples.add(input);
        }
    }

    private void doExecute(Tuple tuple) {
        bolt.execute(tuple);
    }

    private void ack(List<Tuple> tuples) {
        if (!tuples.isEmpty()) {
            LOG.debug("Acking {} tuples", tuples.size());
            for (Tuple tuple : tuples) {
                collector.delegate.ack(tuple);
            }
            tuples.clear();
        }
    }

    private void fail(List<Tuple> tuples) {
        if (!tuples.isEmpty()) {
            LOG.debug("Failing {} tuples", tuples.size());
            for (Tuple tuple : tuples) {
                collector.fail(tuple);
            }
            tuples.clear();
        }
    }

    private static class AckTrackingOutputCollector extends AnchoringOutputCollector {
        private final OutputCollector delegate;
        private final Queue<Tuple> ackedTuples;

        AckTrackingOutputCollector(OutputCollector delegate) {
            super(delegate);
            this.delegate = delegate;
            this.ackedTuples = new ConcurrentLinkedQueue<>();
        }

        List<Tuple> ackedTuples() {
            List<Tuple> result = new ArrayList<>();
            Iterator<Tuple> it = ackedTuples.iterator();
            while (it.hasNext()) {
                result.add(it.next());
                it.remove();
            }
            return result;
        }

        @Override
        public void ack(Tuple input) {
            ackedTuples.add(input);
        }
    }
}
