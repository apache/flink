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

package org.apache.flink.table.runtime.util.collections.binary;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.table.runtime.operators.sort.IntRecordComparator;
import org.apache.flink.table.runtime.typeutils.WindowKeySerializer;
import org.apache.flink.table.runtime.util.WindowKey;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;

/** Tests for {@link WindowBytesSortedArrayMultiMap}. */
public class WindowBytesSortedArrayMultiMapTest extends BytesMultiMapTestBase<WindowKey> {

    private static final LogicalType[] VALUE_TYPES =
            new LogicalType[] {new IntType(), new VarCharType(VarCharType.MAX_LENGTH)};

    private final MapValidator<WindowKey> validator =
            (keys, values, actual) -> {
                while (actual.advanceNext()) {
                    int i = 0;
                    Iterator<RowData> valueIter = actual.getValue();
                    while (valueIter.hasNext()) {
                        Assert.assertEquals(valueIter.next(), values[i++]);
                    }
                }
            };

    private final MapValidator<WindowKey> validatorInDescendingOrder =
            (keys, values, actual) -> {
                while (actual.advanceNext()) {
                    int i = 0;
                    Iterator<RowData> valueIter = actual.getValue();
                    while (valueIter.hasNext()) {
                        Assert.assertEquals(valueIter.next(), values[NUM_VALUE_PER_KEY - 1 - i++]);
                    }
                }
            };

    public WindowBytesSortedArrayMultiMapTest() {
        super(
                new WindowKeySerializer(KEY_TYPES.length),
                RandomKeyGeneratorFactory.createWindowKeyGenerator());
    }

    @Test
    public void testBuildAndRetrieve() throws Exception {
        int heapCapacity = NUM_VALUE_PER_KEY - 10;
        MapCreator<WindowKey> creator =
                (memoryManager, memorySize) ->
                        new WindowBytesSortedArrayMultiMap(
                                this,
                                memoryManager,
                                memorySize,
                                KEY_TYPES,
                                VALUE_TYPES,
                                heapCapacity,
                                IntRecordComparator.INSTANCE);

        innerBuildAndRetrieve(creator, validator);
    }

    @Test
    public void testBuildAndRetrieveWithLargeCapacity() throws Exception {
        int heapCapacity = NUM_VALUE_PER_KEY + 10;
        MapCreator<WindowKey> creator =
                (memoryManager, memorySize) ->
                        new WindowBytesSortedArrayMultiMap(
                                this,
                                memoryManager,
                                memorySize,
                                KEY_TYPES,
                                VALUE_TYPES,
                                heapCapacity,
                                IntRecordComparator.INSTANCE);

        innerBuildAndRetrieve(creator, validator);
    }

    @Test
    public void testBuildAndRetrieveDescendingOrder() throws Exception {
        int heapCapacity = NUM_VALUE_PER_KEY - 10;
        MapCreator<WindowKey> creator =
                (memoryManager, memorySize) ->
                        new WindowBytesSortedArrayMultiMap(
                                this,
                                memoryManager,
                                memorySize,
                                KEY_TYPES,
                                VALUE_TYPES,
                                heapCapacity,
                                new IntRecordComparator(true));

        innerBuildAndRetrieve(creator, validatorInDescendingOrder);
    }

    @Test
    public void testBuildAndRetrieveDescendingOrderWithLargeCapacity() throws Exception {
        int heapCapacity = NUM_VALUE_PER_KEY + 10;
        MapCreator<WindowKey> creator =
                (memoryManager, memorySize) ->
                        new WindowBytesSortedArrayMultiMap(
                                this,
                                memoryManager,
                                memorySize,
                                KEY_TYPES,
                                VALUE_TYPES,
                                heapCapacity,
                                new IntRecordComparator(true));

        innerBuildAndRetrieve(creator, validatorInDescendingOrder);
    }

    @Override
    BinaryRowData[] genValues(int num) {
        BinaryRowData[] values = new BinaryRowData[num];
        for (int i = 0; i < num; i++) {
            values[i] = new BinaryRowData(2);
            BinaryRowWriter writer = new BinaryRowWriter(values[i]);
            writer.writeInt(0, i);
            writer.writeString(1, StringData.fromString("String " + i));
            writer.complete();
        }
        return values;
    }
}
