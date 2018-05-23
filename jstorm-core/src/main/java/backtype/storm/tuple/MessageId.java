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

package backtype.storm.tuple;

import backtype.storm.utils.Utils;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

public class MessageId {

    /**
     * 存储的是anchor和anchor value。
     * 而anchor其实就是root_id，它在spout中生成，并且一路透传到所有的bolt中，
     * 属于同一个tuple tree中的消息都会有相同的root_id，它可以唯一标识spout发出来的这条消息（以及从下游bolt根据这个tuple衍生发出的消息）。
     */
    private Map<Long, Long> _anchorsToIds;

    public MessageId() {
    }

    @Deprecated
    public static long generateId() {
        return Utils.secureRandomLong();
    }

    public static long generateId(Random rand) {
        return rand.nextLong();
    }

    public static MessageId makeUnanchored() {
        return makeId(new HashMap<Long, Long>());
    }

    public static MessageId makeId(Map<Long, Long> anchorsToIds) {
        return new MessageId(anchorsToIds);
    }

    public static MessageId makeRootId(long id, long val) {
        Map<Long, Long> anchorsToIds = new HashMap<>();
        anchorsToIds.put(id, val);
        return new MessageId(anchorsToIds);
    }

    protected MessageId(Map<Long, Long> anchorsToIds) {
        _anchorsToIds = anchorsToIds;
    }

    public boolean isAnchored() {
        return _anchorsToIds != null && _anchorsToIds.size() > 0;
    }

    public Map<Long, Long> getAnchorsToIds() {
        return _anchorsToIds;
    }

    public Set<Long> getAnchors() {
        return _anchorsToIds.keySet();
    }

    @Override
    public int hashCode() {
        return _anchorsToIds.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof MessageId) {
            return _anchorsToIds.equals(((MessageId) other)._anchorsToIds);
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return _anchorsToIds.toString();
    }

    public void serialize(Output out) throws IOException {
        out.writeInt(_anchorsToIds.size(), true);
        for (Entry<Long, Long> anchorToId : _anchorsToIds.entrySet()) {
            out.writeLong(anchorToId.getKey());
            out.writeLong(anchorToId.getValue());
        }
    }

    public static MessageId deserialize(Input in) throws IOException {
        int numAnchors = in.readInt(true);
        if (numAnchors == 0) {
            return null;
        }
        Map<Long, Long> anchorsToIds = new HashMap<>();
        for (int i = 0; i < numAnchors; i++) {
            anchorsToIds.put(in.readLong(), in.readLong());
        }
        return new MessageId(anchorsToIds);
    }
}