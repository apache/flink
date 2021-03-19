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
import org.apache.flink.table.runtime.typeutils.PagedTypeSerializer;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;
import java.util.Random;

/** Base test class for {@link BytesMultiMap} and {@link WindowBytesMultiMap}. */
public abstract class BytesMultiLinkedMapTestBase<K> extends BytesMultiMapTestBase<K> {

    protected static final LogicalType[] VALUE_TYPES =
            new LogicalType[] {new VarCharType(), new IntType()};

    public BytesMultiLinkedMapTestBase(
            PagedTypeSerializer<K> keySerializer, RandomKeyGenerator<K> generator) {
        super(keySerializer, generator);
    }

    @Override
    BinaryRowData[] genValues(int num) {
        BinaryRowData[] values = new BinaryRowData[num];
        final Random rnd = new Random(RANDOM_SEED);
        for (int i = 0; i < num; i++) {
            values[i] = new BinaryRowData(2);
            BinaryRowWriter writer = new BinaryRowWriter(values[i]);
            writer.writeString(0, StringData.fromString("string" + rnd.nextInt()));
            writer.writeInt(1, rnd.nextInt());
            writer.complete();
        }
        return values;
    }

    abstract MapCreator<K> buildCreator();

    // ------------------------------------------------------------------------------------------
    // Tests
    // ------------------------------------------------------------------------------------------

    @Test
    public void buildAndRetrieve() throws Exception {
        innerBuildAndRetrieve(
                buildCreator(),
                (keys, values, actual) -> {
                    while (actual.advanceNext()) {
                        int i = 0;
                        Iterator<RowData> valueIter = actual.getValue();
                        while (valueIter.hasNext()) {
                            Assert.assertEquals(valueIter.next(), values[i++]);
                        }
                    }
                });
    }
}
