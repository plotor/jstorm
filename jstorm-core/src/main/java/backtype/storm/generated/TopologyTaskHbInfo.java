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
 *
 * Autogenerated by Thrift Compiler (0.9.2)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *
 * @generated Autogenerated by Thrift Compiler (0.9.2)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 * @generated Autogenerated by Thrift Compiler (0.9.2)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 * @generated
 */
/**
 * Autogenerated by Thrift Compiler (0.9.2)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */

package backtype.storm.generated;

import org.apache.thrift.EncodingUtils;
import org.apache.thrift.protocol.TTupleProtocol;
import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;
import org.apache.thrift.scheme.TupleScheme;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Generated;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked"})
@Generated(value = "Autogenerated by Thrift Compiler (0.9.2)", date = "2017-2-22")
public class TopologyTaskHbInfo implements org.apache.thrift.TBase<TopologyTaskHbInfo, TopologyTaskHbInfo._Fields>, java.io.Serializable, Cloneable, Comparable<TopologyTaskHbInfo> {
    private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("TopologyTaskHbInfo");

    private static final org.apache.thrift.protocol.TField TOPOLOGY_ID_FIELD_DESC = new org.apache.thrift.protocol.TField("topologyId", org.apache.thrift.protocol.TType.STRING, (short) 1);
    private static final org.apache.thrift.protocol.TField TOPOLOGY_MASTER_ID_FIELD_DESC = new org.apache.thrift.protocol.TField("topologyMasterId", org.apache.thrift.protocol.TType.I32, (short) 2);
    private static final org.apache.thrift.protocol.TField TASK_HBS_FIELD_DESC = new org.apache.thrift.protocol.TField("taskHbs", org.apache.thrift.protocol.TType.MAP, (short) 3);

    private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();

    static {
        schemes.put(StandardScheme.class, new TopologyTaskHbInfoStandardSchemeFactory());
        schemes.put(TupleScheme.class, new TopologyTaskHbInfoTupleSchemeFactory());
    }

    private String topologyId; // required
    private int topologyMasterId; // required
    private Map<Integer, TaskHeartbeat> taskHbs; // optional

    /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
    public enum _Fields implements org.apache.thrift.TFieldIdEnum {
        TOPOLOGY_ID((short) 1, "topologyId"),
        TOPOLOGY_MASTER_ID((short) 2, "topologyMasterId"),
        TASK_HBS((short) 3, "taskHbs");

        private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

        static {
            for (_Fields field : EnumSet.allOf(_Fields.class)) {
                byName.put(field.getFieldName(), field);
            }
        }

        /**
         * Find the _Fields constant that matches fieldId, or null if its not found.
         */
        public static _Fields findByThriftId(int fieldId) {
            switch (fieldId) {
                case 1: // TOPOLOGY_ID
                    return TOPOLOGY_ID;
                case 2: // TOPOLOGY_MASTER_ID
                    return TOPOLOGY_MASTER_ID;
                case 3: // TASK_HBS
                    return TASK_HBS;
                default:
                    return null;
            }
        }

        /**
         * Find the _Fields constant that matches fieldId, throwing an exception
         * if it is not found.
         */
        public static _Fields findByThriftIdOrThrow(int fieldId) {
            _Fields fields = findByThriftId(fieldId);
            if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
            return fields;
        }

        /**
         * Find the _Fields constant that matches name, or null if its not found.
         */
        public static _Fields findByName(String name) {
            return byName.get(name);
        }

        private final short _thriftId;
        private final String _fieldName;

        _Fields(short thriftId, String fieldName) {
            _thriftId = thriftId;
            _fieldName = fieldName;
        }

        public short getThriftFieldId() {
            return _thriftId;
        }

        public String getFieldName() {
            return _fieldName;
        }
    }

    // isset id assignments
    private static final int __TOPOLOGYMASTERID_ISSET_ID = 0;
    private byte __isset_bitfield = 0;
    private static final _Fields optionals[] = {_Fields.TASK_HBS};
    public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;

    static {
        Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
        tmpMap.put(_Fields.TOPOLOGY_ID, new org.apache.thrift.meta_data.FieldMetaData("topologyId", org.apache.thrift.TFieldRequirementType.REQUIRED,
                new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
        tmpMap.put(_Fields.TOPOLOGY_MASTER_ID, new org.apache.thrift.meta_data.FieldMetaData("topologyMasterId", org.apache.thrift.TFieldRequirementType.REQUIRED,
                new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I32)));
        tmpMap.put(_Fields.TASK_HBS, new org.apache.thrift.meta_data.FieldMetaData("taskHbs", org.apache.thrift.TFieldRequirementType.OPTIONAL,
                new org.apache.thrift.meta_data.MapMetaData(org.apache.thrift.protocol.TType.MAP,
                        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I32),
                        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, TaskHeartbeat.class))));
        metaDataMap = Collections.unmodifiableMap(tmpMap);
        org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(TopologyTaskHbInfo.class, metaDataMap);
    }

    public TopologyTaskHbInfo() {
    }

    public TopologyTaskHbInfo(
            String topologyId,
            int topologyMasterId) {
        this();
        this.topologyId = topologyId;
        this.topologyMasterId = topologyMasterId;
        set_topologyMasterId_isSet(true);
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public TopologyTaskHbInfo(TopologyTaskHbInfo other) {
        __isset_bitfield = other.__isset_bitfield;
        if (other.is_set_topologyId()) {
            this.topologyId = other.topologyId;
        }
        this.topologyMasterId = other.topologyMasterId;
        if (other.is_set_taskHbs()) {
            Map<Integer, TaskHeartbeat> __this__taskHbs = new HashMap<Integer, TaskHeartbeat>(other.taskHbs.size());
            for (Map.Entry<Integer, TaskHeartbeat> other_element : other.taskHbs.entrySet()) {

                Integer other_element_key = other_element.getKey();
                TaskHeartbeat other_element_value = other_element.getValue();

                Integer __this__taskHbs_copy_key = other_element_key;

                TaskHeartbeat __this__taskHbs_copy_value = new TaskHeartbeat(other_element_value);

                __this__taskHbs.put(__this__taskHbs_copy_key, __this__taskHbs_copy_value);
            }
            this.taskHbs = __this__taskHbs;
        }
    }

    public TopologyTaskHbInfo deepCopy() {
        return new TopologyTaskHbInfo(this);
    }

    @Override
    public void clear() {
        this.topologyId = null;
        set_topologyMasterId_isSet(false);
        this.topologyMasterId = 0;
        this.taskHbs = null;
    }

    public String get_topologyId() {
        return this.topologyId;
    }

    public void set_topologyId(String topologyId) {
        this.topologyId = topologyId;
    }

    public void unset_topologyId() {
        this.topologyId = null;
    }

    /** Returns true if field topologyId is set (has been assigned a value) and false otherwise */
    public boolean is_set_topologyId() {
        return this.topologyId != null;
    }

    public void set_topologyId_isSet(boolean value) {
        if (!value) {
            this.topologyId = null;
        }
    }

    public int get_topologyMasterId() {
        return this.topologyMasterId;
    }

    public void set_topologyMasterId(int topologyMasterId) {
        this.topologyMasterId = topologyMasterId;
        set_topologyMasterId_isSet(true);
    }

    public void unset_topologyMasterId() {
        __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __TOPOLOGYMASTERID_ISSET_ID);
    }

    /** Returns true if field topologyMasterId is set (has been assigned a value) and false otherwise */
    public boolean is_set_topologyMasterId() {
        return EncodingUtils.testBit(__isset_bitfield, __TOPOLOGYMASTERID_ISSET_ID);
    }

    public void set_topologyMasterId_isSet(boolean value) {
        __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __TOPOLOGYMASTERID_ISSET_ID, value);
    }

    public int get_taskHbs_size() {
        return (this.taskHbs == null) ? 0 : this.taskHbs.size();
    }

    public void put_to_taskHbs(int key, TaskHeartbeat val) {
        if (this.taskHbs == null) {
            this.taskHbs = new HashMap<Integer, TaskHeartbeat>();
        }
        this.taskHbs.put(key, val);
    }

    public Map<Integer, TaskHeartbeat> get_taskHbs() {
        return this.taskHbs;
    }

    public void set_taskHbs(Map<Integer, TaskHeartbeat> taskHbs) {
        this.taskHbs = taskHbs;
    }

    public void unset_taskHbs() {
        this.taskHbs = null;
    }

    /** Returns true if field taskHbs is set (has been assigned a value) and false otherwise */
    public boolean is_set_taskHbs() {
        return this.taskHbs != null;
    }

    public void set_taskHbs_isSet(boolean value) {
        if (!value) {
            this.taskHbs = null;
        }
    }

    public void setFieldValue(_Fields field, Object value) {
        switch (field) {
            case TOPOLOGY_ID:
                if (value == null) {
                    unset_topologyId();
                } else {
                    set_topologyId((String) value);
                }
                break;

            case TOPOLOGY_MASTER_ID:
                if (value == null) {
                    unset_topologyMasterId();
                } else {
                    set_topologyMasterId((Integer) value);
                }
                break;

            case TASK_HBS:
                if (value == null) {
                    unset_taskHbs();
                } else {
                    set_taskHbs((Map<Integer, TaskHeartbeat>) value);
                }
                break;

        }
    }

    public Object getFieldValue(_Fields field) {
        switch (field) {
            case TOPOLOGY_ID:
                return get_topologyId();

            case TOPOLOGY_MASTER_ID:
                return Integer.valueOf(get_topologyMasterId());

            case TASK_HBS:
                return get_taskHbs();

        }
        throw new IllegalStateException();
    }

    /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
    public boolean isSet(_Fields field) {
        if (field == null) {
            throw new IllegalArgumentException();
        }

        switch (field) {
            case TOPOLOGY_ID:
                return is_set_topologyId();
            case TOPOLOGY_MASTER_ID:
                return is_set_topologyMasterId();
            case TASK_HBS:
                return is_set_taskHbs();
        }
        throw new IllegalStateException();
    }

    @Override
    public boolean equals(Object that) {
        if (that == null) {
            return false;
        }
        if (that instanceof TopologyTaskHbInfo) {
            return this.equals((TopologyTaskHbInfo) that);
        }
        return false;
    }

    public boolean equals(TopologyTaskHbInfo that) {
        if (that == null) {
            return false;
        }

        boolean this_present_topologyId = true && this.is_set_topologyId();
        boolean that_present_topologyId = true && that.is_set_topologyId();
        if (this_present_topologyId || that_present_topologyId) {
            if (!(this_present_topologyId && that_present_topologyId)) {
                return false;
            }
            if (!this.topologyId.equals(that.topologyId)) {
                return false;
            }
        }

        boolean this_present_topologyMasterId = true;
        boolean that_present_topologyMasterId = true;
        if (this_present_topologyMasterId || that_present_topologyMasterId) {
            if (!(this_present_topologyMasterId && that_present_topologyMasterId)) {
                return false;
            }
            if (this.topologyMasterId != that.topologyMasterId) {
                return false;
            }
        }

        boolean this_present_taskHbs = true && this.is_set_taskHbs();
        boolean that_present_taskHbs = true && that.is_set_taskHbs();
        if (this_present_taskHbs || that_present_taskHbs) {
            if (!(this_present_taskHbs && that_present_taskHbs)) {
                return false;
            }
            if (!this.taskHbs.equals(that.taskHbs)) {
                return false;
            }
        }

        return true;
    }

    @Override
    public int hashCode() {
        List<Object> list = new ArrayList<Object>();

        boolean present_topologyId = true && (is_set_topologyId());
        list.add(present_topologyId);
        if (present_topologyId) {
            list.add(topologyId);
        }

        boolean present_topologyMasterId = true;
        list.add(present_topologyMasterId);
        if (present_topologyMasterId) {
            list.add(topologyMasterId);
        }

        boolean present_taskHbs = true && (is_set_taskHbs());
        list.add(present_taskHbs);
        if (present_taskHbs) {
            list.add(taskHbs);
        }

        return list.hashCode();
    }

    @Override
    public int compareTo(TopologyTaskHbInfo other) {
        if (!getClass().equals(other.getClass())) {
            return getClass().getName().compareTo(other.getClass().getName());
        }

        int lastComparison = 0;

        lastComparison = Boolean.valueOf(is_set_topologyId()).compareTo(other.is_set_topologyId());
        if (lastComparison != 0) {
            return lastComparison;
        }
        if (is_set_topologyId()) {
            lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.topologyId, other.topologyId);
            if (lastComparison != 0) {
                return lastComparison;
            }
        }
        lastComparison = Boolean.valueOf(is_set_topologyMasterId()).compareTo(other.is_set_topologyMasterId());
        if (lastComparison != 0) {
            return lastComparison;
        }
        if (is_set_topologyMasterId()) {
            lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.topologyMasterId, other.topologyMasterId);
            if (lastComparison != 0) {
                return lastComparison;
            }
        }
        lastComparison = Boolean.valueOf(is_set_taskHbs()).compareTo(other.is_set_taskHbs());
        if (lastComparison != 0) {
            return lastComparison;
        }
        if (is_set_taskHbs()) {
            lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.taskHbs, other.taskHbs);
            if (lastComparison != 0) {
                return lastComparison;
            }
        }
        return 0;
    }

    public _Fields fieldForId(int fieldId) {
        return _Fields.findByThriftId(fieldId);
    }

    public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
        schemes.get(iprot.getScheme()).getScheme().read(iprot, this);
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
        schemes.get(oprot.getScheme()).getScheme().write(oprot, this);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("TopologyTaskHbInfo(");
        boolean first = true;

        sb.append("topologyId:");
        if (this.topologyId == null) {
            sb.append("null");
        } else {
            sb.append(this.topologyId);
        }
        first = false;
        if (!first) sb.append(", ");
        sb.append("topologyMasterId:");
        sb.append(this.topologyMasterId);
        first = false;
        if (is_set_taskHbs()) {
            if (!first) sb.append(", ");
            sb.append("taskHbs:");
            if (this.taskHbs == null) {
                sb.append("null");
            } else {
                sb.append(this.taskHbs);
            }
            first = false;
        }
        sb.append(")");
        return sb.toString();
    }

    public void validate() throws org.apache.thrift.TException {
        // check for required fields
        if (!is_set_topologyId()) {
            throw new org.apache.thrift.protocol.TProtocolException("Required field 'topologyId' is unset! Struct:" + toString());
        }

        if (!is_set_topologyMasterId()) {
            throw new org.apache.thrift.protocol.TProtocolException("Required field 'topologyMasterId' is unset! Struct:" + toString());
        }

        // check for sub-struct validity
    }

    private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
        try {
            write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
        } catch (org.apache.thrift.TException te) {
            throw new java.io.IOException(te);
        }
    }

    private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
        try {
            // it doesn't seem like you should have to do this, but java serialization is wacky, and doesn't call the default constructor.
            __isset_bitfield = 0;
            read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
        } catch (org.apache.thrift.TException te) {
            throw new java.io.IOException(te);
        }
    }

    private static class TopologyTaskHbInfoStandardSchemeFactory implements SchemeFactory {
        public TopologyTaskHbInfoStandardScheme getScheme() {
            return new TopologyTaskHbInfoStandardScheme();
        }
    }

    private static class TopologyTaskHbInfoStandardScheme extends StandardScheme<TopologyTaskHbInfo> {

        public void read(org.apache.thrift.protocol.TProtocol iprot, TopologyTaskHbInfo struct) throws org.apache.thrift.TException {
            org.apache.thrift.protocol.TField schemeField;
            iprot.readStructBegin();
            while (true) {
                schemeField = iprot.readFieldBegin();
                if (schemeField.type == org.apache.thrift.protocol.TType.STOP) {
                    break;
                }
                switch (schemeField.id) {
                    case 1: // TOPOLOGY_ID
                        if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
                            struct.topologyId = iprot.readString();
                            struct.set_topologyId_isSet(true);
                        } else {
                            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
                        }
                        break;
                    case 2: // TOPOLOGY_MASTER_ID
                        if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
                            struct.topologyMasterId = iprot.readI32();
                            struct.set_topologyMasterId_isSet(true);
                        } else {
                            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
                        }
                        break;
                    case 3: // TASK_HBS
                        if (schemeField.type == org.apache.thrift.protocol.TType.MAP) {
                            {
                                org.apache.thrift.protocol.TMap _map214 = iprot.readMapBegin();
                                struct.taskHbs = new HashMap<Integer, TaskHeartbeat>(2 * _map214.size);
                                int _key215;
                                TaskHeartbeat _val216;
                                for (int _i217 = 0; _i217 < _map214.size; ++_i217) {
                                    _key215 = iprot.readI32();
                                    _val216 = new TaskHeartbeat();
                                    _val216.read(iprot);
                                    struct.taskHbs.put(_key215, _val216);
                                }
                                iprot.readMapEnd();
                            }
                            struct.set_taskHbs_isSet(true);
                        } else {
                            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
                        }
                        break;
                    default:
                        org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
                }
                iprot.readFieldEnd();
            }
            iprot.readStructEnd();
            struct.validate();
        }

        public void write(org.apache.thrift.protocol.TProtocol oprot, TopologyTaskHbInfo struct) throws org.apache.thrift.TException {
            struct.validate();

            oprot.writeStructBegin(STRUCT_DESC);
            if (struct.topologyId != null) {
                oprot.writeFieldBegin(TOPOLOGY_ID_FIELD_DESC);
                oprot.writeString(struct.topologyId);
                oprot.writeFieldEnd();
            }
            oprot.writeFieldBegin(TOPOLOGY_MASTER_ID_FIELD_DESC);
            oprot.writeI32(struct.topologyMasterId);
            oprot.writeFieldEnd();
            if (struct.taskHbs != null) {
                if (struct.is_set_taskHbs()) {
                    oprot.writeFieldBegin(TASK_HBS_FIELD_DESC);
                    {
                        oprot.writeMapBegin(new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.I32, org.apache.thrift.protocol.TType.STRUCT, struct.taskHbs.size()));
                        for (Map.Entry<Integer, TaskHeartbeat> _iter218 : struct.taskHbs.entrySet()) {
                            oprot.writeI32(_iter218.getKey());
                            _iter218.getValue().write(oprot);
                        }
                        oprot.writeMapEnd();
                    }
                    oprot.writeFieldEnd();
                }
            }
            oprot.writeFieldStop();
            oprot.writeStructEnd();
        }

    }

    private static class TopologyTaskHbInfoTupleSchemeFactory implements SchemeFactory {
        public TopologyTaskHbInfoTupleScheme getScheme() {
            return new TopologyTaskHbInfoTupleScheme();
        }
    }

    private static class TopologyTaskHbInfoTupleScheme extends TupleScheme<TopologyTaskHbInfo> {

        @Override
        public void write(org.apache.thrift.protocol.TProtocol prot, TopologyTaskHbInfo struct) throws org.apache.thrift.TException {
            TTupleProtocol oprot = (TTupleProtocol) prot;
            oprot.writeString(struct.topologyId);
            oprot.writeI32(struct.topologyMasterId);
            BitSet optionals = new BitSet();
            if (struct.is_set_taskHbs()) {
                optionals.set(0);
            }
            oprot.writeBitSet(optionals, 1);
            if (struct.is_set_taskHbs()) {
                {
                    oprot.writeI32(struct.taskHbs.size());
                    for (Map.Entry<Integer, TaskHeartbeat> _iter219 : struct.taskHbs.entrySet()) {
                        oprot.writeI32(_iter219.getKey());
                        _iter219.getValue().write(oprot);
                    }
                }
            }
        }

        @Override
        public void read(org.apache.thrift.protocol.TProtocol prot, TopologyTaskHbInfo struct) throws org.apache.thrift.TException {
            TTupleProtocol iprot = (TTupleProtocol) prot;
            struct.topologyId = iprot.readString();
            struct.set_topologyId_isSet(true);
            struct.topologyMasterId = iprot.readI32();
            struct.set_topologyMasterId_isSet(true);
            BitSet incoming = iprot.readBitSet(1);
            if (incoming.get(0)) {
                {
                    org.apache.thrift.protocol.TMap _map220 = new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.I32, org.apache.thrift.protocol.TType.STRUCT, iprot.readI32());
                    struct.taskHbs = new HashMap<Integer, TaskHeartbeat>(2 * _map220.size);
                    int _key221;
                    TaskHeartbeat _val222;
                    for (int _i223 = 0; _i223 < _map220.size; ++_i223) {
                        _key221 = iprot.readI32();
                        _val222 = new TaskHeartbeat();
                        _val222.read(iprot);
                        struct.taskHbs.put(_key221, _val222);
                    }
                }
                struct.set_taskHbs_isSet(true);
            }
        }
    }

}

