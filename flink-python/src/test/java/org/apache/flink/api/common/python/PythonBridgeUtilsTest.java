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

package org.apache.flink.api.common.python;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import net.razorvine.pickle.Unpickler;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class PythonBridgeUtilsTest {

    @Test
    void testGetPickledBytesFromRowWithLocalZonedTimestamp() throws IOException {
        Instant instant = Instant.parse("2026-11-01T05:30:00.123456789Z");
        Row row = Row.of(instant, new Object[] {instant});
        DataType timestampType = DataTypes.TIMESTAMP_LTZ(9);

        Object serialized =
                PythonBridgeUtils.getPickledBytesFromRow(
                        row, new DataType[] {timestampType, DataTypes.ARRAY(timestampType)});

        assertThat(serialized).isInstanceOf(List.class);
        List<?> fields = (List<?>) serialized;
        assertSerializedInstant(fields.get(1), instant);

        Object serializedArray = new Unpickler().loads((byte[]) fields.get(2));
        assertThat(serializedArray).isInstanceOf(List.class);
        List<?> array = (List<?>) serializedArray;
        assertSerializedInstant(array.get(0), instant);
    }

    private static void assertSerializedInstant(Object serialized, Instant expected)
            throws IOException {
        Object deserialized = new Unpickler().loads((byte[]) serialized);
        assertThat(deserialized).isInstanceOf(List.class);
        List<?> instant = (List<?>) deserialized;
        assertThat(((Number) instant.get(0)).longValue()).isEqualTo(expected.getEpochSecond());
        assertThat(((Number) instant.get(1)).intValue()).isEqualTo(expected.getNano());
    }
}
