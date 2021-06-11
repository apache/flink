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

package org.apache.flink.table.planner.codegen;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.table.runtime.generated.RecordEqualiser;
import org.apache.flink.table.runtime.typeutils.RawValueDataSerializer;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TypeInformationRawType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.Assert;
import org.junit.Test;

import java.util.function.Function;
import java.util.stream.IntStream;

import static org.apache.flink.table.data.TimestampData.fromEpochMillis;
import static org.junit.Assert.assertTrue;

/** Test for {@link EqualiserCodeGenerator}. */
public class EqualiserCodeGeneratorTest {

    @Test
    public void testRaw() {
        RecordEqualiser equaliser =
                new EqualiserCodeGenerator(
                                new LogicalType[] {new TypeInformationRawType<>(Types.INT)})
                        .generateRecordEqualiser("RAW")
                        .newInstance(Thread.currentThread().getContextClassLoader());
        Function<RawValueData<?>, BinaryRowData> func =
                o -> {
                    BinaryRowData row = new BinaryRowData(1);
                    BinaryRowWriter writer = new BinaryRowWriter(row);
                    writer.writeRawValue(
                            0, o, new RawValueDataSerializer<>(IntSerializer.INSTANCE));
                    writer.complete();
                    return row;
                };
        assertBoolean(
                equaliser, func, RawValueData.fromObject(1), RawValueData.fromObject(1), true);
        assertBoolean(
                equaliser, func, RawValueData.fromObject(1), RawValueData.fromObject(2), false);
    }

    @Test
    public void testTimestamp() {
        RecordEqualiser equaliser =
                new EqualiserCodeGenerator(new LogicalType[] {new TimestampType()})
                        .generateRecordEqualiser("TIMESTAMP")
                        .newInstance(Thread.currentThread().getContextClassLoader());
        Function<TimestampData, BinaryRowData> func =
                o -> {
                    BinaryRowData row = new BinaryRowData(1);
                    BinaryRowWriter writer = new BinaryRowWriter(row);
                    writer.writeTimestamp(0, o, 9);
                    writer.complete();
                    return row;
                };
        assertBoolean(equaliser, func, fromEpochMillis(1024), fromEpochMillis(1024), true);
        assertBoolean(equaliser, func, fromEpochMillis(1024), fromEpochMillis(1025), false);
    }

    @Test
    public void testManyFields() {
        final LogicalType[] fieldTypes =
                IntStream.range(0, 999)
                        .mapToObj(i -> new VarCharType())
                        .toArray(LogicalType[]::new);

        RecordEqualiser equaliser;
        try {
            equaliser =
                    new EqualiserCodeGenerator(fieldTypes)
                            .generateRecordEqualiser("ManyFields")
                            .newInstance(Thread.currentThread().getContextClassLoader());
        } catch (Exception e) {
            Assert.fail("Expected compilation to succeed");

            // Unreachable
            throw e;
        }

        final StringData[] fields =
                IntStream.range(0, 999)
                        .mapToObj(i -> StringData.fromString("Entry " + i))
                        .toArray(StringData[]::new);
        assertTrue(
                equaliser.equals(
                        GenericRowData.of((Object[]) fields),
                        GenericRowData.of((Object[]) fields)));
    }

    private static <T> void assertBoolean(
            RecordEqualiser equaliser,
            Function<T, BinaryRowData> toBinaryRow,
            T o1,
            T o2,
            boolean bool) {
        Assert.assertEquals(bool, equaliser.equals(GenericRowData.of(o1), GenericRowData.of(o2)));
        Assert.assertEquals(bool, equaliser.equals(toBinaryRow.apply(o1), GenericRowData.of(o2)));
        Assert.assertEquals(bool, equaliser.equals(GenericRowData.of(o1), toBinaryRow.apply(o2)));
        Assert.assertEquals(bool, equaliser.equals(toBinaryRow.apply(o1), toBinaryRow.apply(o2)));
    }
}
