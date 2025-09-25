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

package org.apache.flink.table.runtime.arrow.writers;

import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.arrow.ArrowUtils;

import org.apache.flink.shaded.guava33.com.google.common.collect.ImmutableMap;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/** Tests for {@link MapWriter}. */
class MapWriterTest {

    private BufferAllocator allocator;

    @BeforeEach
    void setUp() {
        allocator = ArrowUtils.getRootAllocator().newChildAllocator("test", 0, Long.MAX_VALUE);
    }

    @AfterEach
    void tearDown() {
        if (allocator != null) {
            allocator.close();
        }
    }

    @Test
    void testMapWriterMultipleWritesAndReset() {
        // Create Map<int, int> vector
        Field keyField = new Field("key", FieldType.notNullable(new ArrowType.Int(32, true)), null);
        Field valueField =
                new Field("value", FieldType.nullable(new ArrowType.Int(32, true)), null);
        FieldType mapType = new FieldType(true, new ArrowType.Map(false), null);
        Field mapField =
                new Field(
                        "myMap",
                        mapType,
                        List.of(
                                new Field(
                                        "entries",
                                        FieldType.notNullable(ArrowType.Struct.INSTANCE),
                                        Arrays.asList(keyField, valueField))));
        // Create arrow writer
        try (MapVector mapVector = (MapVector) mapField.createVector(allocator)) {
            StructVector structVector = (StructVector) mapVector.getDataVector();
            IntVector keyVector = (IntVector) structVector.getChild(MapVector.KEY_NAME);
            IntVector valueVector = (IntVector) structVector.getChild(MapVector.VALUE_NAME);
            MapWriter<RowData> mapWriter =
                    MapWriter.forRow(
                            mapVector,
                            IntWriter.forArray(keyVector),
                            IntWriter.forArray(valueVector));
            // Write once
            mapWriter.write(createRowData(ImmutableMap.of(1, 1, 2, 2)), 0);
            mapWriter.write(createRowData(ImmutableMap.of(1, 1)), 0);
            mapWriter.finish();
            assertEquals(2, mapVector.getValueCount());
            checkMapVector(mapVector, keyVector, valueVector, 0, ImmutableMap.of(1, 1, 2, 2));
            checkMapVector(mapVector, keyVector, valueVector, 1, ImmutableMap.of(1, 1));
            // Reset and write again
            mapWriter.reset();
            mapWriter.write(createRowData(ImmutableMap.of(1, 1, 2, 2)), 0);
            mapWriter.write(createRowData(ImmutableMap.of(1, 1)), 0);
            mapWriter.finish();
            assertEquals(2, mapVector.getValueCount());
            checkMapVector(mapVector, keyVector, valueVector, 0, ImmutableMap.of(1, 1, 2, 2));
            checkMapVector(mapVector, keyVector, valueVector, 1, ImmutableMap.of(1, 1));
        }
    }

    private RowData createRowData(Map<Integer, Integer> map) {
        MapData mapData = new GenericMapData(map);
        return GenericRowData.of(mapData);
    }

    private void checkMapVector(
            MapVector mapVector,
            IntVector keyVector,
            IntVector valueVector,
            int rowIndex,
            Map<Integer, Integer> expected) {
        if (mapVector.isNull(rowIndex)) {
            Assertions.assertNull(expected);
        }

        int start = mapVector.getOffsetBuffer().getInt(rowIndex * MapVector.OFFSET_WIDTH);
        int end = mapVector.getOffsetBuffer().getInt((rowIndex + 1) * MapVector.OFFSET_WIDTH);

        Map<Integer, Integer> result = new HashMap<>();
        for (int i = start; i < end; i++) {
            assertFalse(keyVector.isNull(i));
            int key = keyVector.get(i);
            Integer value = valueVector.isNull(i) ? null : valueVector.get(i);
            result.put(key, value);
        }
        assertEquals(expected, result);
    }
}
