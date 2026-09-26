/*
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

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.serialization.SerializerConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.StateSchemaEvolvingSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/**
 * An integer serializer that records whether it was armed for state schema evolution, so a test can
 * assert arming on the serializer object a state descriptor actually ends up holding.
 *
 * <p>Lives here because the production {@link StateSchemaEvolvingSerializer} implementation is a
 * table-module serializer that flink-runtime cannot see.
 */
public class StateSchemaEvolvingTestSerializer extends TypeSerializer<Integer>
        implements StateSchemaEvolvingSerializer<Integer> {

    private static final long serialVersionUID = 1L;

    private final boolean armed;

    public StateSchemaEvolvingTestSerializer() {
        this(false);
    }

    private StateSchemaEvolvingTestSerializer(boolean armed) {
        this.armed = armed;
    }

    public boolean isArmed() {
        return armed;
    }

    @Override
    public TypeSerializer<Integer> withStateSchemaEvolution() {
        return armed ? this : new StateSchemaEvolvingTestSerializer(true);
    }

    @Override
    public boolean isImmutableType() {
        return true;
    }

    @Override
    public TypeSerializer<Integer> duplicate() {
        return new StateSchemaEvolvingTestSerializer(armed);
    }

    @Override
    public Integer createInstance() {
        return 0;
    }

    @Override
    public Integer copy(Integer from) {
        return from;
    }

    @Override
    public Integer copy(Integer from, Integer reuse) {
        return from;
    }

    @Override
    public int getLength() {
        return Integer.BYTES;
    }

    @Override
    public void serialize(Integer record, DataOutputView target) throws IOException {
        target.writeInt(record);
    }

    @Override
    public Integer deserialize(DataInputView source) throws IOException {
        return source.readInt();
    }

    @Override
    public Integer deserialize(Integer reuse, DataInputView source) throws IOException {
        return source.readInt();
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        target.writeInt(source.readInt());
    }

    /** The armed flag is runtime-only state, so it must not perturb serializer equality. */
    @Override
    public boolean equals(Object obj) {
        return obj instanceof StateSchemaEvolvingTestSerializer;
    }

    @Override
    public int hashCode() {
        return StateSchemaEvolvingTestSerializer.class.hashCode();
    }

    @Override
    public TypeSerializerSnapshot<Integer> snapshotConfiguration() {
        return new StateSchemaEvolvingTestSerializerSnapshot();
    }

    /** Snapshot for {@link StateSchemaEvolvingTestSerializer}. */
    public static final class StateSchemaEvolvingTestSerializerSnapshot
            extends SimpleTypeSerializerSnapshot<Integer> {

        public StateSchemaEvolvingTestSerializerSnapshot() {
            super(StateSchemaEvolvingTestSerializer::new);
        }
    }

    /** Type information producing an unarmed {@link StateSchemaEvolvingTestSerializer}. */
    public static final class StateSchemaEvolvingTestTypeInfo extends TypeInformation<Integer> {

        private static final long serialVersionUID = 1L;

        @Override
        public boolean isBasicType() {
            return false;
        }

        @Override
        public boolean isTupleType() {
            return false;
        }

        @Override
        public int getArity() {
            return 1;
        }

        @Override
        public int getTotalFields() {
            return 1;
        }

        @Override
        public Class<Integer> getTypeClass() {
            return Integer.class;
        }

        @Override
        public boolean isKeyType() {
            return false;
        }

        @Override
        public TypeSerializer<Integer> createSerializer(SerializerConfig config) {
            return new StateSchemaEvolvingTestSerializer();
        }

        @Override
        public String toString() {
            return StateSchemaEvolvingTestTypeInfo.class.getSimpleName();
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof StateSchemaEvolvingTestTypeInfo;
        }

        @Override
        public int hashCode() {
            return StateSchemaEvolvingTestTypeInfo.class.hashCode();
        }

        @Override
        public boolean canEqual(Object obj) {
            return obj instanceof StateSchemaEvolvingTestTypeInfo;
        }
    }
}
