/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.kafka.table;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableList;
import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableMap;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.binaryrow;
import static org.junit.Assert.assertEquals;

/** Tests for {@link ReducingUpsertWriterStateSerializer}. */
public class ReducingUpsertWriterStateSerializerTest {

    private static final int VERSION = 1;

    @ParameterizedTest
    @MethodSource("getArguments")
    public void testSerDe(
            List<Integer> wrappedState, SimpleVersionedSerializer<Integer> wrappedStateSerializer)
            throws IOException {
        final ReducingUpsertWriterState<Integer> state =
                new ReducingUpsertWriterState<>(
                        ImmutableMap.of(
                                binaryrow("row1", 1), Tuple2.of(binaryrow("row1", 2), 42L),
                                binaryrow("row2", 2), Tuple2.of(binaryrow("row2", 3), 43L)),
                        wrappedState);
        final ReducingUpsertWriterStateSerializer<Integer> serializer =
                new ReducingUpsertWriterStateSerializer<>(
                        new RowDataSerializer(
                                DataTypes.STRING().getLogicalType(),
                                DataTypes.INT().getLogicalType()),
                        wrappedStateSerializer);
        final ReducingUpsertWriterState<Integer> actual =
                serializer.deserialize(VERSION, serializer.serialize(state));
        assertEquals(state, actual);
    }

    private static List<Arguments> getArguments() {
        return ImmutableList.of(
                Arguments.of(null, null),
                Arguments.of(ImmutableList.of(5, 4), new WrappedSerializer()));
    }

    private static class WrappedSerializer implements SimpleVersionedSerializer<Integer> {

        @Override
        public int getVersion() {
            return 1;
        }

        @Override
        public byte[] serialize(Integer obj) throws IOException {
            try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    final DataOutputStream out = new DataOutputStream(baos)) {
                out.writeInt(obj);
                return baos.toByteArray();
            }
        }

        @Override
        public Integer deserialize(int version, byte[] serialized) throws IOException {
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                    final DataInputStream in = new DataInputStream(bais)) {
                return in.readInt();
            }
        }
    }
}
