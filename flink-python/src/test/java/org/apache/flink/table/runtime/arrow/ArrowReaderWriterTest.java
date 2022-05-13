/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.arrow;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.typeutils.ArrayDataSerializer;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.util.StreamRecordUtils;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.testutils.DeeplyEqualsChecker;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.junit.BeforeClass;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Tests for {@link ArrowReader} and {@link ArrowWriter} of RowData. */
public class ArrowReaderWriterTest extends ArrowReaderWriterTestBase<RowData> {
    private static List<LogicalType> fieldTypes = new ArrayList<>();
    private static RowType rowType;
    private static RowType rowFieldType;
    private static BufferAllocator allocator;

    public ArrowReaderWriterTest() {
        super(
                new DeeplyEqualsChecker()
                        .withCustomCheck(
                                (o1, o2) -> o1 instanceof RowData && o2 instanceof RowData,
                                (o1, o2, checker) -> {
                                    RowDataSerializer serializer =
                                            new RowDataSerializer(
                                                    fieldTypes.toArray(new LogicalType[0]));
                                    return deepEqualsRowData(
                                            (RowData) o1,
                                            (RowData) o2,
                                            (RowDataSerializer) serializer.duplicate(),
                                            (RowDataSerializer) serializer.duplicate());
                                }));
    }

    private static boolean deepEqualsRowData(
            RowData should,
            RowData is,
            RowDataSerializer serializer1,
            RowDataSerializer serializer2) {
        if (should.getArity() != is.getArity()) {
            return false;
        }
        BinaryRowData row1 = serializer1.toBinaryRow(should);
        BinaryRowData row2 = serializer2.toBinaryRow(is);

        return Objects.equals(row1, row2);
    }

    @BeforeClass
    public static void init() {
        fieldTypes.add(new TinyIntType());
        fieldTypes.add(new SmallIntType());
        fieldTypes.add(new IntType());
        fieldTypes.add(new BigIntType());
        fieldTypes.add(new BooleanType());
        fieldTypes.add(new FloatType());
        fieldTypes.add(new DoubleType());
        fieldTypes.add(new VarCharType());
        fieldTypes.add(new VarBinaryType());
        fieldTypes.add(new DecimalType(10, 3));
        fieldTypes.add(new DateType());
        fieldTypes.add(new TimeType(0));
        fieldTypes.add(new TimeType(2));
        fieldTypes.add(new TimeType(4));
        fieldTypes.add(new TimeType(8));
        fieldTypes.add(new LocalZonedTimestampType(0));
        fieldTypes.add(new LocalZonedTimestampType(2));
        fieldTypes.add(new LocalZonedTimestampType(4));
        fieldTypes.add(new LocalZonedTimestampType(8));
        fieldTypes.add(new TimestampType(0));
        fieldTypes.add(new TimestampType(2));
        fieldTypes.add(new TimestampType(4));
        fieldTypes.add(new TimestampType(8));
        fieldTypes.add(new ArrayType(new VarCharType()));
        rowFieldType =
                new RowType(
                        Arrays.asList(
                                new RowType.RowField("a", new IntType()),
                                new RowType.RowField("b", new VarCharType()),
                                new RowType.RowField("c", new ArrayType(new VarCharType())),
                                new RowType.RowField("d", new TimestampType(2)),
                                new RowType.RowField(
                                        "e",
                                        new RowType(
                                                Arrays.asList(
                                                        new RowType.RowField("e1", new IntType()),
                                                        new RowType.RowField(
                                                                "e2", new VarCharType()))))));
        fieldTypes.add(rowFieldType);

        List<RowType.RowField> rowFields = new ArrayList<>();
        for (int i = 0; i < fieldTypes.size(); i++) {
            rowFields.add(new RowType.RowField("f" + i, fieldTypes.get(i)));
        }
        rowType = new RowType(rowFields);
        allocator = ArrowUtils.getRootAllocator().newChildAllocator("stdout", 0, Long.MAX_VALUE);
    }

    @Override
    public ArrowReader createArrowReader(InputStream inputStream) throws IOException {
        ArrowStreamReader reader = new ArrowStreamReader(inputStream, allocator);
        reader.loadNextBatch();
        return ArrowUtils.createArrowReader(reader.getVectorSchemaRoot(), rowType);
    }

    @Override
    public Tuple2<ArrowWriter<RowData>, ArrowStreamWriter> createArrowWriter(
            OutputStream outputStream) throws IOException {
        VectorSchemaRoot root =
                VectorSchemaRoot.create(ArrowUtils.toArrowSchema(rowType), allocator);
        ArrowWriter<RowData> arrowWriter = ArrowUtils.createRowDataArrowWriter(root, rowType);
        ArrowStreamWriter arrowStreamWriter = new ArrowStreamWriter(root, null, outputStream);
        arrowStreamWriter.start();
        return Tuple2.of(arrowWriter, arrowStreamWriter);
    }

    @Override
    public RowData[] getTestData() {
        RowData row1 =
                StreamRecordUtils.row(
                        (byte) 1,
                        (short) 2,
                        3,
                        4L,
                        true,
                        1.0f,
                        1.0,
                        "hello",
                        "hello".getBytes(),
                        DecimalData.fromUnscaledLong(1, 10, 3),
                        100,
                        3600000,
                        3600000,
                        3600000,
                        3600000,
                        TimestampData.fromEpochMillis(3600000),
                        TimestampData.fromEpochMillis(3600000),
                        TimestampData.fromEpochMillis(3600000, 100000),
                        TimestampData.fromEpochMillis(3600000, 100000),
                        TimestampData.fromEpochMillis(3600000),
                        TimestampData.fromEpochMillis(3600000),
                        TimestampData.fromEpochMillis(3600000, 100000),
                        TimestampData.fromEpochMillis(3600000, 100000),
                        new GenericArrayData(
                                new StringData[] {
                                    StringData.fromString("hello"),
                                    StringData.fromString("中文"),
                                    null
                                }),
                        GenericRowData.of(
                                1,
                                StringData.fromString("hello"),
                                new GenericArrayData(
                                        new StringData[] {StringData.fromString("hello")}),
                                TimestampData.fromEpochMillis(3600000),
                                GenericRowData.of(1, StringData.fromString("hello"))));
        BinaryRowData row2 =
                StreamRecordUtils.binaryrow(
                        (byte) 1,
                        (short) 2,
                        3,
                        4L,
                        false,
                        1.0f,
                        1.0,
                        "中文",
                        "中文".getBytes(),
                        DecimalData.fromUnscaledLong(1, 10, 3),
                        100,
                        3600000,
                        3600000,
                        3600000,
                        3600000,
                        Tuple2.of(TimestampData.fromEpochMillis(3600000), 0),
                        Tuple2.of(TimestampData.fromEpochMillis(3600000), 2),
                        Tuple2.of(TimestampData.fromEpochMillis(3600000, 100000), 4),
                        Tuple2.of(TimestampData.fromEpochMillis(3600000, 100000), 8),
                        Tuple2.of(TimestampData.fromEpochMillis(3600000), 0),
                        Tuple2.of(TimestampData.fromEpochMillis(3600000), 2),
                        Tuple2.of(TimestampData.fromEpochMillis(3600000, 100000), 4),
                        Tuple2.of(TimestampData.fromEpochMillis(3600000, 100000), 8),
                        Tuple2.of(
                                new GenericArrayData(new String[] {null, null, null}),
                                new ArrayDataSerializer(new VarCharType())),
                        Tuple2.of(
                                GenericRowData.of(
                                        1,
                                        null,
                                        new GenericArrayData(
                                                new StringData[] {StringData.fromString("hello")}),
                                        null,
                                        GenericRowData.of(1, StringData.fromString("hello"))),
                                new RowDataSerializer(rowFieldType)));
        RowData row3 =
                StreamRecordUtils.row(
                        null,
                        (short) 2,
                        3,
                        4L,
                        false,
                        1.0f,
                        1.0,
                        "中文",
                        "中文".getBytes(),
                        DecimalData.fromUnscaledLong(1, 10, 3),
                        100,
                        3600000,
                        3600000,
                        3600000,
                        3600000,
                        TimestampData.fromEpochMillis(3600000),
                        TimestampData.fromEpochMillis(3600000),
                        TimestampData.fromEpochMillis(3600000, 100000),
                        TimestampData.fromEpochMillis(3600000, 100000),
                        TimestampData.fromEpochMillis(3600000),
                        TimestampData.fromEpochMillis(3600000),
                        TimestampData.fromEpochMillis(3600000, 100000),
                        TimestampData.fromEpochMillis(3600000, 100000),
                        new GenericArrayData(new String[] {null, null, null}),
                        GenericRowData.of(
                                1,
                                null,
                                new GenericArrayData(
                                        new StringData[] {StringData.fromString("hello")}),
                                null,
                                null));
        BinaryRowData row4 =
                StreamRecordUtils.binaryrow(
                        (byte) 1,
                        null,
                        3,
                        4L,
                        true,
                        1.0f,
                        1.0,
                        "hello",
                        "hello".getBytes(),
                        DecimalData.fromUnscaledLong(1, 10, 3),
                        100,
                        3600000,
                        3600000,
                        3600000,
                        3600000,
                        Tuple2.of(TimestampData.fromEpochMillis(3600000), 0),
                        Tuple2.of(TimestampData.fromEpochMillis(3600000), 2),
                        Tuple2.of(TimestampData.fromEpochMillis(3600000, 100000), 4),
                        Tuple2.of(TimestampData.fromEpochMillis(3600000, 100000), 8),
                        Tuple2.of(TimestampData.fromEpochMillis(3600000), 0),
                        Tuple2.of(TimestampData.fromEpochMillis(3600000), 2),
                        Tuple2.of(TimestampData.fromEpochMillis(3600000, 100000), 4),
                        Tuple2.of(TimestampData.fromEpochMillis(3600000, 100000), 8),
                        Tuple2.of(
                                new GenericArrayData(
                                        new StringData[] {
                                            StringData.fromString("hello"),
                                            StringData.fromString("中文"),
                                            null
                                        }),
                                new ArrayDataSerializer(new VarCharType())),
                        Tuple2.of(
                                GenericRowData.of(
                                        1,
                                        null,
                                        new GenericArrayData(
                                                new StringData[] {StringData.fromString("hello")}),
                                        null,
                                        null),
                                new RowDataSerializer(rowFieldType)));
        RowData row5 = StreamRecordUtils.row(new Object[fieldTypes.size()]);
        BinaryRowData row6 = StreamRecordUtils.binaryrow(new Object[fieldTypes.size()]);
        return new RowData[] {row1, row2, row3, row4, row5, row6};
    }
}
