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

package org.apache.flink.streaming.tests.verify;

import org.apache.flink.api.common.typeutils.CompositeSerializer;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.util.FlinkRuntimeException;

import javax.annotation.Nonnull;

import java.io.Serializable;

/** User state value with timestamps before and after update. */
public class ValueWithTs<V> implements Serializable {

    private static final long serialVersionUID = -8941625260587401383L;

    private final V value;
    private final long timestamp;

    public ValueWithTs(V value, long timestamp) {
        this.value = value;
        this.timestamp = timestamp;
    }

    V getValue() {
        return value;
    }

    long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "ValueWithTs{" + "value=" + value + ", timestamp=" + timestamp + '}';
    }

    /** Serializer for Serializer. */
    public static class Serializer extends CompositeSerializer<ValueWithTs<?>> {

        private static final long serialVersionUID = -7300352863212438745L;

        public Serializer(
                TypeSerializer<?> valueSerializer, TypeSerializer<Long> timestampSerializer) {
            super(true, valueSerializer, timestampSerializer);
        }

        @SuppressWarnings("unchecked")
        Serializer(PrecomputedParameters precomputed, TypeSerializer<?>... fieldSerializers) {
            super(precomputed, fieldSerializers);
        }

        @Override
        public ValueWithTs<?> createInstance(@Nonnull Object... values) {
            return new ValueWithTs<>(values[0], (Long) values[1]);
        }

        @Override
        protected void setField(@Nonnull ValueWithTs<?> value, int index, Object fieldValue) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected Object getField(@Nonnull ValueWithTs<?> value, int index) {
            switch (index) {
                case 0:
                    return value.getValue();
                case 1:
                    return value.getTimestamp();
                default:
                    throw new FlinkRuntimeException("Unexpected field index for ValueWithTs");
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        protected CompositeSerializer<ValueWithTs<?>> createSerializerInstance(
                PrecomputedParameters precomputed, TypeSerializer<?>... originalSerializers) {

            return new Serializer(precomputed, originalSerializers[0], originalSerializers[1]);
        }

        TypeSerializer<?> getValueSerializer() {
            return fieldSerializers[0];
        }

        @SuppressWarnings("unchecked")
        TypeSerializer<Long> getTimestampSerializer() {
            TypeSerializer<?> fieldSerializer = fieldSerializers[1];
            return (TypeSerializer<Long>) fieldSerializer;
        }

        @Override
        public TypeSerializerSnapshot<ValueWithTs<?>> snapshotConfiguration() {
            return new ValueWithTsSerializerSnapshot(this);
        }
    }

    /** A {@link TypeSerializerSnapshot} for ValueWithTs Serializer. */
    public static final class ValueWithTsSerializerSnapshot
            extends CompositeTypeSerializerSnapshot<ValueWithTs<?>, Serializer> {

        private static final int VERSION = 2;

        @SuppressWarnings("unused")
        public ValueWithTsSerializerSnapshot() {
            super(Serializer.class);
        }

        ValueWithTsSerializerSnapshot(Serializer serializerInstance) {
            super(serializerInstance);
        }

        @Override
        protected int getCurrentOuterSnapshotVersion() {
            return VERSION;
        }

        @Override
        protected TypeSerializer<?>[] getNestedSerializers(Serializer outerSerializer) {
            return new TypeSerializer[] {
                outerSerializer.getValueSerializer(), outerSerializer.getTimestampSerializer()
            };
        }

        @SuppressWarnings("unchecked")
        @Override
        protected Serializer createOuterSerializerWithNestedSerializers(
                TypeSerializer<?>[] nestedSerializers) {
            TypeSerializer<?> valueSerializer = nestedSerializers[0];
            TypeSerializer<Long> timestampSerializer = (TypeSerializer<Long>) nestedSerializers[1];
            return new Serializer(valueSerializer, timestampSerializer);
        }
    }
}
