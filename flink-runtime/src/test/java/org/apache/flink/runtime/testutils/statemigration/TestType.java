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

package org.apache.flink.runtime.testutils.statemigration;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.state.Keyed;
import org.apache.flink.runtime.state.PriorityComparable;
import org.apache.flink.runtime.state.heap.AbstractHeapPriorityQueueElement;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A data type used as state in state migration tests.
 *
 * <p>This is implemented so that the type can also be used as keyed priority queue state.
 */
public class TestType extends AbstractHeapPriorityQueueElement
        implements PriorityComparable<TestType>, Keyed<String> {

    private final int value;
    private final String key;

    public TestType(String key, int value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public String getKey() {
        return key;
    }

    public int getValue() {
        return value;
    }

    @Override
    public int comparePriorityTo(@Nonnull TestType other) {
        return Integer.compare(value, other.value);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof TestType)) {
            return false;
        }

        if (obj == this) {
            return true;
        }

        TestType other = (TestType) obj;
        return Objects.equals(key, other.key) && value == other.value;
    }

    @Override
    public int hashCode() {
        return 31 * key.hashCode() + value;
    }

    @Override
    public String toString() {
        return String.format("TestType(key='%s', value=%d)", key, value);
    }

    /** A serializer that read / writes {@link TestType} in schema version 1. */
    public static class V1TestTypeSerializer extends TestTypeSerializerBase {
        private static final long serialVersionUID = 5053346160938769779L;

        @Override
        public void serialize(TestType record, DataOutputView target) throws IOException {
            target.writeUTF(record.getKey());
            target.writeInt(record.getValue());
        }

        @Override
        public TestType deserialize(DataInputView source) throws IOException {
            return new TestType(source.readUTF(), source.readInt());
        }

        @Override
        public TypeSerializerSnapshot<TestType> snapshotConfiguration() {
            return new V1TestTypeSerializerSnapshot();
        }
    }

    /**
     * A serializer that read / writes {@link TestType} in schema version 2. Migration is required
     * if the state was previously written with {@link V1TestTypeSerializer}.
     */
    public static class V2TestTypeSerializer extends TestTypeSerializerBase {

        private static final long serialVersionUID = 7199590310936186578L;

        private static final String RANDOM_PAYLOAD = "random-payload";

        @Override
        public void serialize(TestType record, DataOutputView target) throws IOException {
            target.writeUTF(record.getKey());
            target.writeUTF(RANDOM_PAYLOAD);
            target.writeInt(record.getValue());
            target.writeBoolean(true);
        }

        @Override
        public TestType deserialize(DataInputView source) throws IOException {
            String key = source.readUTF();
            assertThat(source.readUTF()).isEqualTo(RANDOM_PAYLOAD);
            int value = source.readInt();
            assertThat(source.readBoolean()).isTrue();

            return new TestType(key, value);
        }

        @Override
        public TypeSerializerSnapshot<TestType> snapshotConfiguration() {
            return new V1TestTypeSerializerSnapshot();
        }
    }

    /**
     * A serializer that is meant to be compatible with any of the serializers only ofter being
     * reconfigured as a new instance.
     */
    public static class ReconfigurationRequiringTestTypeSerializer extends TestTypeSerializerBase {

        private static final long serialVersionUID = -7254527815207212324L;

        @Override
        public void serialize(TestType record, DataOutputView target) throws IOException {
            throw new UnsupportedOperationException(
                    "The serializer should have been reconfigured as a new instance; shouldn't be used.");
        }

        @Override
        public TestType deserialize(DataInputView source) throws IOException {
            throw new UnsupportedOperationException(
                    "The serializer should have been reconfigured as a new instance; shouldn't be used.");
        }

        @Override
        public TypeSerializerSnapshot<TestType> snapshotConfiguration() {
            throw new UnsupportedOperationException(
                    "The serializer should have been reconfigured as a new instance; shouldn't be used.");
        }
    }

    /** A serializer that is meant to be incompatible with any of the serializers. */
    public static class IncompatibleTestTypeSerializer extends TestTypeSerializerBase {

        private static final long serialVersionUID = -2959080770523247215L;

        @Override
        public void serialize(TestType record, DataOutputView target) throws IOException {
            throw new UnsupportedOperationException(
                    "This is an incompatible serializer; shouldn't be used.");
        }

        @Override
        public TestType deserialize(DataInputView source) throws IOException {
            throw new UnsupportedOperationException(
                    "This is an incompatible serializer; shouldn't be used.");
        }

        @Override
        public TypeSerializerSnapshot<TestType> snapshotConfiguration() {
            throw new UnsupportedOperationException(
                    "This is an incompatible serializer; shouldn't be used.");
        }
    }

    public abstract static class TestTypeSerializerBase extends TypeSerializer<TestType> {

        private static final long serialVersionUID = 256299937766275871L;

        // --------------------------------------------------------------------------------
        //  Miscellaneous serializer methods
        // --------------------------------------------------------------------------------

        @Override
        public TestType copy(TestType from) {
            return new TestType(from.getKey(), from.getValue());
        }

        @Override
        public void copy(DataInputView source, DataOutputView target) throws IOException {
            serialize(deserialize(source), target);
        }

        @Override
        public TestType deserialize(TestType reuse, DataInputView source) throws IOException {
            return deserialize(source);
        }

        @Override
        public TestType copy(TestType from, TestType reuse) {
            return copy(from);
        }

        @Override
        public TestType createInstance() {
            throw new UnsupportedOperationException();
        }

        @Override
        public TypeSerializer<TestType> duplicate() {
            return this;
        }

        @Override
        public boolean isImmutableType() {
            return false;
        }

        @Override
        public int getLength() {
            return -1;
        }

        @Override
        public int hashCode() {
            return getClass().hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            return obj == this;
        }
    }
}
