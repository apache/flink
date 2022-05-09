/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cep.nfa.sharedbuffer;

import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.cep.nfa.ComputationState;
import org.apache.flink.cep.nfa.NFAStateSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.StringValue;

import java.io.IOException;
import java.util.Objects;

/** Id for user-defined accumulator. */
public class UserAccumulatorId {

    private static final int VERSION_1 = 1;

    private final int version;
    private final ComputationState computationState;
    private final String accmulatorKey;

    public UserAccumulatorId(ComputationState computationState, String accmulatorKey) {
        this.version = VERSION_1;
        this.computationState = computationState;
        this.accmulatorKey = accmulatorKey;
    }

    public int getVersion() {
        return version;
    }

    public ComputationState getComputationState() {
        return computationState;
    }

    public String getAccmulatorKey() {
        return accmulatorKey;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        UserAccumulatorId that = (UserAccumulatorId) o;
        return version == that.version
                && Objects.equals(computationState, that.computationState)
                && Objects.equals(accmulatorKey, that.accmulatorKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(version, computationState, accmulatorKey);
    }

    @Override
    public String toString() {
        return "UserAccumulatorId{"
                + "version="
                + version
                + ", computationState="
                + computationState
                + ", accmulatorKey='"
                + accmulatorKey
                + '\''
                + '}';
    }

    /** Serializer for {@link UserAccumulatorId}. */
    public static class UserAccumulatorIdSerializer
            extends TypeSerializerSingleton<UserAccumulatorId> {

        public static final UserAccumulatorIdSerializer INSTANCE =
                new UserAccumulatorIdSerializer();

        public UserAccumulatorIdSerializer() {}

        @Override
        public boolean isImmutableType() {
            return true;
        }

        @Override
        public UserAccumulatorId createInstance() {
            return null;
        }

        @Override
        public UserAccumulatorId copy(UserAccumulatorId from) {
            return new UserAccumulatorId(from.computationState, from.accmulatorKey);
        }

        @Override
        public UserAccumulatorId copy(UserAccumulatorId from, UserAccumulatorId reuse) {
            return copy(from);
        }

        @Override
        public int getLength() {
            return -1;
        }

        @Override
        public void serialize(UserAccumulatorId userAccumulatorId, DataOutputView target)
                throws IOException {
            target.writeInt(userAccumulatorId.getVersion());
            NFAStateSerializer.INSTANCE.serializeSingleComputationState(
                    userAccumulatorId.getComputationState(), target);
            StringValue.writeString(userAccumulatorId.getAccmulatorKey(), target);
        }

        @Override
        public UserAccumulatorId deserialize(DataInputView source) throws IOException {
            int version = source.readInt();
            ComputationState computationState =
                    NFAStateSerializer.INSTANCE.deserializeSingleComputationState(source);
            String accumulatorKey = StringValue.readString(source);
            return new UserAccumulatorId(computationState, accumulatorKey);
        }

        @Override
        public UserAccumulatorId deserialize(UserAccumulatorId reuse, DataInputView source)
                throws IOException {
            return deserialize(source);
        }

        @Override
        public void copy(DataInputView source, DataOutputView target) throws IOException {
            serialize(deserialize(source), target);
        }

        @Override
        public TypeSerializerSnapshot<UserAccumulatorId> snapshotConfiguration() {
            return new UserAccumulatorIdSerializerSnapshot();
        }

        /** Serializer configuration snapshot for compatibility and format evolution. */
        @SuppressWarnings("WeakerAccess")
        public static class UserAccumulatorIdSerializerSnapshot
                extends SimpleTypeSerializerSnapshot<UserAccumulatorId> {

            public UserAccumulatorIdSerializerSnapshot() {
                super(UserAccumulatorIdSerializer::new);
            }
        }
    }
}
