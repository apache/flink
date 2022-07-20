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

package org.apache.flink.table.runtime.typeutils;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.SerializerTestInstance;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryArrayData;
import org.apache.flink.table.data.binary.BinaryMapData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.writer.BinaryArrayWriter;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RawType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.testutils.DeeplyEqualsChecker;

import org.junit.Test;

import java.util.Objects;

import static org.apache.flink.table.data.StringData.fromString;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link RowDataSerializer}. */
abstract class RowDataSerializerTest extends SerializerTestInstance<RowData> {

    private final RowDataSerializer serializer;
    private final RowData[] testData;

    RowDataSerializerTest(RowDataSerializer serializer, RowData[] testData) {
        super(
                new DeeplyEqualsChecker()
                        .withCustomCheck(
                                (o1, o2) -> o1 instanceof RowData && o2 instanceof RowData,
                                (o1, o2, checker) ->
                                        deepEqualsRowData(
                                                (RowData) o1,
                                                (RowData) o2,
                                                (RowDataSerializer) serializer.duplicate(),
                                                (RowDataSerializer) serializer.duplicate())),
                serializer,
                RowData.class,
                -1,
                testData);
        this.serializer = serializer;
        this.testData = testData;
    }

    // ----------------------------------------------------------------------------------------------

    private static BinaryArrayData createArray(int... ints) {
        BinaryArrayData array = new BinaryArrayData();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, ints.length, 4);
        for (int i = 0; i < ints.length; i++) {
            writer.writeInt(i, ints[i]);
        }
        writer.complete();
        return array;
    }

    private static BinaryMapData createMap(int[] keys, int[] values) {
        return BinaryMapData.valueOf(createArray(keys), createArray(values));
    }

    private static GenericRowData createRow(Object f0, Object f1, Object f2, Object f3, Object f4) {
        GenericRowData row = new GenericRowData(5);
        row.setField(0, f0);
        row.setField(1, f1);
        row.setField(2, f2);
        row.setField(3, f3);
        row.setField(4, f4);
        return row;
    }

    private static boolean deepEqualsRowData(
            RowData should,
            RowData is,
            RowDataSerializer serializer1,
            RowDataSerializer serializer2) {
        return deepEqualsRowData(should, is, serializer1, serializer2, false);
    }

    private static boolean deepEqualsRowData(
            RowData should,
            RowData is,
            RowDataSerializer serializer1,
            RowDataSerializer serializer2,
            boolean checkClass) {
        if (should.getArity() != is.getArity()) {
            return false;
        }
        if (checkClass && (should.getClass() != is.getClass() || !should.equals(is))) {
            return false;
        }

        BinaryRowData row1 = serializer1.toBinaryRow(should);
        BinaryRowData row2 = serializer2.toBinaryRow(is);

        return Objects.equals(row1, row2);
    }

    private void checkDeepEquals(RowData should, RowData is, boolean checkClass) {
        boolean equals =
                deepEqualsRowData(
                        should,
                        is,
                        (RowDataSerializer) serializer.duplicate(),
                        (RowDataSerializer) serializer.duplicate(),
                        checkClass);
        assertThat(equals).isTrue();
    }

    @Test
    protected void testCopy() {
        for (RowData row : testData) {
            checkDeepEquals(row, serializer.copy(row), true);
        }

        for (RowData row : testData) {
            checkDeepEquals(row, serializer.copy(row, new GenericRowData(row.getArity())), true);
        }

        for (RowData row : testData) {
            checkDeepEquals(
                    row,
                    serializer.copy(
                            serializer.toBinaryRow(row), new GenericRowData(row.getArity())),
                    false);
        }

        for (RowData row : testData) {
            checkDeepEquals(row, serializer.copy(serializer.toBinaryRow(row)), false);
        }

        for (RowData row : testData) {
            checkDeepEquals(
                    row,
                    serializer.copy(serializer.toBinaryRow(row), new BinaryRowData(row.getArity())),
                    false);
        }
    }

    @Test
    void testWrongCopy() {
        assertThatThrownBy(() -> serializer.copy(new GenericRowData(serializer.getArity() + 1)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testWrongCopyReuse() {
        for (RowData row : testData) {
            assertThatThrownBy(
                            () ->
                                    checkDeepEquals(
                                            row,
                                            serializer.copy(
                                                    row, new GenericRowData(row.getArity() + 1)),
                                            false))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    /** Class used for concurrent testing with KryoSerializer. */
    private static class WrappedString {

        private final String content;

        WrappedString(String content) {
            this.content = content;
        }
    }

    static final class SimpleRowDataSerializerTest extends RowDataSerializerTest {
        public SimpleRowDataSerializerTest() {
            super(getRowSerializer(), getData());
        }

        private static RowData[] getData() {
            GenericRowData row1 = new GenericRowData(2);
            row1.setField(0, 1);
            row1.setField(1, fromString("a"));

            GenericRowData row2 = new GenericRowData(2);
            row2.setField(0, 2);
            row2.setField(1, null);

            return new RowData[] {row1, row2};
        }

        private static RowDataSerializer getRowSerializer() {
            InternalTypeInfo<RowData> typeInfo =
                    InternalTypeInfo.ofFields(new IntType(), VarCharType.STRING_TYPE);

            return typeInfo.toRowSerializer();
        }
    }

    static final class LargeRowDataSerializerTest extends RowDataSerializerTest {
        public LargeRowDataSerializerTest() {
            super(getRowSerializer(), getData());
        }

        private static RowData[] getData() {
            GenericRowData row = new GenericRowData(13);
            row.setField(0, 2);
            row.setField(1, null);
            row.setField(3, null);
            row.setField(4, null);
            row.setField(5, null);
            row.setField(6, null);
            row.setField(7, null);
            row.setField(8, null);
            row.setField(9, null);
            row.setField(10, null);
            row.setField(11, null);
            row.setField(12, fromString("Test"));

            return new RowData[] {row};
        }

        private static RowDataSerializer getRowSerializer() {
            InternalTypeInfo<RowData> typeInfo =
                    InternalTypeInfo.ofFields(
                            new IntType(),
                            new IntType(),
                            new IntType(),
                            new IntType(),
                            new IntType(),
                            new IntType(),
                            new IntType(),
                            new IntType(),
                            new IntType(),
                            new IntType(),
                            new IntType(),
                            new IntType(),
                            VarCharType.STRING_TYPE);

            return typeInfo.toRowSerializer();
        }
    }

    static final class RowDataSerializerWithComplexTypesTest extends RowDataSerializerTest {
        public RowDataSerializerWithComplexTypesTest() {
            super(getRowSerializer(), getData());
        }

        private static RowData[] getData() {
            return new GenericRowData[] {
                createRow(null, null, null, null, null),
                createRow(0, null, null, null, null),
                createRow(0, 0.0, null, null, null),
                createRow(0, 0.0, fromString("a"), null, null),
                createRow(1, 0.0, fromString("a"), null, null),
                createRow(1, 1.0, fromString("a"), null, null),
                createRow(1, 1.0, fromString("b"), null, null),
                createRow(
                        1,
                        1.0,
                        fromString("b"),
                        createArray(1),
                        createMap(new int[] {1}, new int[] {1})),
                createRow(
                        1,
                        1.0,
                        fromString("b"),
                        createArray(1, 2),
                        createMap(new int[] {1, 4}, new int[] {1, 2})),
                createRow(
                        1,
                        1.0,
                        fromString("b"),
                        createArray(1, 2, 3),
                        createMap(new int[] {1, 5}, new int[] {1, 3})),
                createRow(
                        1,
                        1.0,
                        fromString("b"),
                        createArray(1, 2, 3, 4),
                        createMap(new int[] {1, 6}, new int[] {1, 4})),
                createRow(
                        1,
                        1.0,
                        fromString("b"),
                        createArray(1, 2, 3, 4, 5),
                        createMap(new int[] {1, 7}, new int[] {1, 5})),
                createRow(
                        1,
                        1.0,
                        fromString("b"),
                        createArray(1, 2, 3, 4, 5, 6),
                        createMap(new int[] {1, 8}, new int[] {1, 6}))
            };
        }

        private static RowDataSerializer getRowSerializer() {
            InternalTypeInfo<RowData> typeInfo =
                    InternalTypeInfo.ofFields(
                            new IntType(),
                            new DoubleType(),
                            VarCharType.STRING_TYPE,
                            new ArrayType(new IntType()),
                            new MapType(new IntType(), new IntType()));

            return typeInfo.toRowSerializer();
        }
    }

    static final class RowDataSerializerWithKryoTest extends RowDataSerializerTest {
        public RowDataSerializerWithKryoTest() {
            super(getRowSerializer(), getData());
        }

        private static RowData[] getData() {
            GenericRowData row = new GenericRowData(1);
            row.setField(0, RawValueData.fromObject(new WrappedString("a")));

            return new RowData[] {row};
        }

        private static RowDataSerializer getRowSerializer() {
            RawValueDataSerializer<WrappedString> rawValueSerializer =
                    new RawValueDataSerializer<>(
                            new KryoSerializer<>(WrappedString.class, new ExecutionConfig()));
            return new RowDataSerializer(
                    new LogicalType[] {new RawType(RawValueData.class, rawValueSerializer)},
                    new TypeSerializer[] {rawValueSerializer});
        }
    }

    static final class RowDataSerializerWithNestedRowTest extends RowDataSerializerTest {

        private static final DataType NESTED_DATA_TYPE =
                DataTypes.ROW(
                        DataTypes.FIELD("ri", DataTypes.INT()),
                        DataTypes.FIELD("rs", DataTypes.STRING()),
                        DataTypes.FIELD("rb", DataTypes.BIGINT()));

        public RowDataSerializerWithNestedRowTest() {
            super(getRowSerializer(), getData());
        }

        private static RowData[] getData() {
            final DataType outerDataType =
                    DataTypes.ROW(
                            DataTypes.FIELD("i", DataTypes.INT()),
                            DataTypes.FIELD("r", NESTED_DATA_TYPE),
                            DataTypes.FIELD("s", DataTypes.STRING()));

            final RowDataSerializer outerSerializer =
                    (RowDataSerializer)
                            InternalSerializers.<RowData>create(outerDataType.getLogicalType());

            final GenericRowData outerRow1 =
                    GenericRowData.of(
                            12,
                            GenericRowData.of(34, StringData.fromString("56"), 78L),
                            StringData.fromString("910"));
            final RowData nestedRow1 = outerSerializer.toBinaryRow(outerRow1).getRow(1, 3);

            final GenericRowData outerRow2 =
                    GenericRowData.of(
                            12, GenericRowData.of(null, StringData.fromString("56"), 78L), null);
            final RowData nestedRow2 = outerSerializer.toBinaryRow(outerRow2).getRow(1, 3);

            return new RowData[] {nestedRow1, nestedRow2};
        }

        private static RowDataSerializer getRowSerializer() {
            return (RowDataSerializer)
                    InternalSerializers.<RowData>create(NESTED_DATA_TYPE.getLogicalType());
        }
    }
}
