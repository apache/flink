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

package org.apache.flink.formats.parquet.vector;

import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.flink.formats.parquet.row.ParquetRowDataBuilder;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.columnar.ColumnarRowData;
import org.apache.flink.table.data.columnar.vector.VectorizedColumnBatch;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.table.utils.DateTimeUtils;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link ParquetColumnarRowSplitReader}. */
class ParquetColumnarRowSplitReaderTest {

    private static final int FIELD_NUMBER = 34;
    private static final LocalDateTime BASE_TIME = LocalDateTime.now();

    private static final RowType ROW_TYPE =
            RowType.of(
                    new VarCharType(VarCharType.MAX_LENGTH),
                    new BooleanType(),
                    new TinyIntType(),
                    new SmallIntType(),
                    new IntType(),
                    new BigIntType(),
                    new FloatType(),
                    new DoubleType(),
                    new TimestampType(9),
                    new DecimalType(5, 0),
                    new DecimalType(15, 0),
                    new DecimalType(20, 0),
                    new DecimalType(5, 0),
                    new DecimalType(15, 0),
                    new DecimalType(20, 0),
                    new ArrayType(new VarCharType(VarCharType.MAX_LENGTH)),
                    new ArrayType(new BooleanType()),
                    new ArrayType(new TinyIntType()),
                    new ArrayType(new SmallIntType()),
                    new ArrayType(new IntType()),
                    new ArrayType(new BigIntType()),
                    new ArrayType(new FloatType()),
                    new ArrayType(new DoubleType()),
                    new ArrayType(new TimestampType(9)),
                    new ArrayType(new DecimalType(5, 0)),
                    new ArrayType(new DecimalType(15, 0)),
                    new ArrayType(new DecimalType(20, 0)),
                    new ArrayType(new DecimalType(5, 0)),
                    new ArrayType(new DecimalType(15, 0)),
                    new ArrayType(new DecimalType(20, 0)),
                    new MapType(
                            new VarCharType(VarCharType.MAX_LENGTH),
                            new VarCharType(VarCharType.MAX_LENGTH)),
                    new MapType(new IntType(), new BooleanType()),
                    new MultisetType(new VarCharType(VarCharType.MAX_LENGTH)),
                    RowType.of(new VarCharType(VarCharType.MAX_LENGTH), new IntType()));

    @TempDir File tmpDir;

    public static Collection<Integer> parameters() {
        return Arrays.asList(10, 1000);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void testNormalTypesReadWithSplits(int rowGroupSize) throws IOException {
        // prepare parquet file
        int number = 10000;
        List<RowData> records = new ArrayList<>(number);
        List<Integer> values = new ArrayList<>(number);
        Random random = new Random();
        for (int i = 0; i < number; i++) {
            Integer v = random.nextInt(number / 2);
            if (v % 10 == 0) {
                values.add(null);
                records.add(new GenericRowData(FIELD_NUMBER));
            } else {
                values.add(v);
                records.add(newRow(v));
            }
        }

        testNormalTypes(number, records, values, rowGroupSize);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void testReachEnd(int rowGroupSize) throws Exception {
        // prepare parquet file
        int number = 5;
        List<RowData> records = new ArrayList<>(number);
        Random random = new Random();
        for (int i = 0; i < number; i++) {
            Integer v = random.nextInt(number / 2);
            if (v % 10 == 0) {
                records.add(new GenericRowData(FIELD_NUMBER));
            } else {
                records.add(newRow(v));
            }
        }
        Path testPath = createTempParquetFile(tmpDir, records, rowGroupSize);
        ParquetColumnarRowSplitReader reader =
                createReader(
                        testPath, 0, testPath.getFileSystem().getFileStatus(testPath).getLen());
        while (!reader.reachedEnd()) {
            reader.nextRecord();
        }
        assertThat(reader.reachedEnd()).isTrue();
    }

    private Path createTempParquetFile(File folder, List<RowData> rows, int rowGroupSize)
            throws IOException {
        // write data
        Path path = new Path(folder.getPath(), UUID.randomUUID().toString());
        Configuration conf = new Configuration();
        conf.setInt("parquet.block.size", rowGroupSize);
        ParquetWriterFactory<RowData> factory =
                ParquetRowDataBuilder.createWriterFactory(ROW_TYPE, conf, false);
        BulkWriter<RowData> writer =
                factory.create(path.getFileSystem().create(path, FileSystem.WriteMode.OVERWRITE));
        for (int i = 0; i < rows.size(); i++) {
            writer.addElement(rows.get(i));
        }

        writer.flush();
        writer.finish();
        return path;
    }

    private void testNormalTypes(
            int number, List<RowData> records, List<Integer> values, int rowGroupSize)
            throws IOException {
        Path testPath = createTempParquetFile(tmpDir, records, rowGroupSize);

        // test reading and splitting
        long fileLen = testPath.getFileSystem().getFileStatus(testPath).getLen();
        int len1 = readSplitAndCheck(0, 0, testPath, 0, fileLen / 3, values);
        int len2 = readSplitAndCheck(len1, 0, testPath, fileLen / 3, fileLen * 2 / 3, values);
        int len3 =
                readSplitAndCheck(
                        len1 + len2, 0, testPath, fileLen * 2 / 3, Long.MAX_VALUE, values);
        assertThat(len1 + len2 + len3).isEqualTo(number);

        // test seek
        assertThat(readSplitAndCheck(number / 2, number / 2, testPath, 0, fileLen, values))
                .isEqualTo(number - number / 2);
    }

    private ParquetColumnarRowSplitReader createReader(
            Path testPath, long splitStart, long splitLength) throws IOException {
        return new ParquetColumnarRowSplitReader(
                false,
                true,
                new Configuration(),
                ROW_TYPE.getChildren().toArray(new LogicalType[] {}),
                new String[] {
                    "f0", "f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8", "f9", "f10", "f11", "f12",
                    "f13", "f14", "f15", "f16", "f17", "f18", "f19", "f20", "f21", "f22", "f23",
                    "f24", "f25", "f26", "f27", "f28", "f29", "f30", "f31", "f32", "f33"
                },
                VectorizedColumnBatch::new,
                500,
                new org.apache.hadoop.fs.Path(testPath.getPath()),
                splitStart,
                splitLength);
    }

    private int readSplitAndCheck(
            int start,
            long seekToRow,
            Path testPath,
            long splitStart,
            long splitLength,
            List<Integer> values)
            throws IOException {
        ParquetColumnarRowSplitReader reader = createReader(testPath, splitStart, splitLength);
        reader.seekToRow(seekToRow);

        int i = start;
        while (!reader.reachedEnd()) {
            ColumnarRowData row = reader.nextRecord();
            Integer v = values.get(i);
            if (v == null) {
                assertThat(row.isNullAt(0)).isTrue();
                assertThat(row.isNullAt(1)).isTrue();
                assertThat(row.isNullAt(2)).isTrue();
                assertThat(row.isNullAt(3)).isTrue();
                assertThat(row.isNullAt(4)).isTrue();
                assertThat(row.isNullAt(5)).isTrue();
                assertThat(row.isNullAt(6)).isTrue();
                assertThat(row.isNullAt(7)).isTrue();
                assertThat(row.isNullAt(8)).isTrue();
                assertThat(row.isNullAt(9)).isTrue();
                assertThat(row.isNullAt(10)).isTrue();
                assertThat(row.isNullAt(11)).isTrue();
                assertThat(row.isNullAt(12)).isTrue();
                assertThat(row.isNullAt(13)).isTrue();
                assertThat(row.isNullAt(14)).isTrue();
                assertThat(row.isNullAt(15)).isTrue();
                assertThat(row.isNullAt(16)).isTrue();
                assertThat(row.isNullAt(17)).isTrue();
                assertThat(row.isNullAt(18)).isTrue();
                assertThat(row.isNullAt(19)).isTrue();
                assertThat(row.isNullAt(20)).isTrue();
                assertThat(row.isNullAt(21)).isTrue();
                assertThat(row.isNullAt(22)).isTrue();
                assertThat(row.isNullAt(23)).isTrue();
                assertThat(row.isNullAt(24)).isTrue();
                assertThat(row.isNullAt(25)).isTrue();
                assertThat(row.isNullAt(26)).isTrue();
                assertThat(row.isNullAt(27)).isTrue();
                assertThat(row.isNullAt(28)).isTrue();
                assertThat(row.isNullAt(29)).isTrue();
                assertThat(row.isNullAt(30)).isTrue();
                assertThat(row.isNullAt(31)).isTrue();
                assertThat(row.isNullAt(32)).isTrue();
                assertThat(row.isNullAt(33)).isTrue();
            } else {
                assertThat(row.getString(0)).hasToString("" + v);
                assertThat(row.getBoolean(1)).isEqualTo(v % 2 == 0);
                assertThat(row.getByte(2)).isEqualTo(v.byteValue());
                assertThat(row.getShort(3)).isEqualTo(v.shortValue());
                assertThat(row.getInt(4)).isEqualTo(v.intValue());
                assertThat(row.getLong(5)).isEqualTo(v.longValue());
                assertThat(row.getFloat(6)).isEqualTo(v.floatValue());
                assertThat(row.getDouble(7)).isEqualTo(v.doubleValue());
                assertThat(row.getTimestamp(8, 9).toLocalDateTime()).isEqualTo(toDateTime(v));
                if (DecimalData.fromBigDecimal(BigDecimal.valueOf(v), 5, 0) == null) {
                    assertThat(row.isNullAt(9)).isTrue();
                    assertThat(row.isNullAt(12)).isTrue();
                    assertThat(row.isNullAt(24)).isTrue();
                    assertThat(row.isNullAt(27)).isTrue();
                } else {
                    assertThat(row.getDecimal(9, 5, 0))
                            .isEqualTo(DecimalData.fromBigDecimal(BigDecimal.valueOf(v), 5, 0));
                    assertThat(row.getDecimal(12, 5, 0))
                            .isEqualTo(DecimalData.fromBigDecimal(BigDecimal.valueOf(v), 5, 0));
                    assertThat(row.getArray(24).getDecimal(0, 5, 0))
                            .isEqualTo(DecimalData.fromBigDecimal(BigDecimal.valueOf(v), 5, 0));
                    assertThat(row.getArray(27).getDecimal(0, 5, 0))
                            .isEqualTo(DecimalData.fromBigDecimal(BigDecimal.valueOf(v), 5, 0));
                }
                assertThat(row.getDecimal(10, 15, 0))
                        .isEqualTo(DecimalData.fromBigDecimal(BigDecimal.valueOf(v), 15, 0));
                assertThat(row.getDecimal(11, 20, 0))
                        .isEqualTo(DecimalData.fromBigDecimal(BigDecimal.valueOf(v), 20, 0));
                assertThat(row.getDecimal(13, 15, 0))
                        .isEqualTo(DecimalData.fromBigDecimal(BigDecimal.valueOf(v), 15, 0));
                assertThat(row.getDecimal(14, 20, 0))
                        .isEqualTo(DecimalData.fromBigDecimal(BigDecimal.valueOf(v), 20, 0));
                assertThat(row.getArray(15).getString(0)).hasToString("" + v);
                assertThat(row.getArray(16).getBoolean(0)).isEqualTo(v % 2 == 0);
                assertThat(row.getArray(17).getByte(0)).isEqualTo(v.byteValue());
                assertThat(row.getArray(18).getShort(0)).isEqualTo(v.shortValue());
                assertThat(row.getArray(19).getInt(0)).isEqualTo(v.intValue());
                assertThat(row.getArray(20).getLong(0)).isEqualTo(v.longValue());
                assertThat(row.getArray(21).getFloat(0)).isEqualTo(v.floatValue());
                assertThat(row.getArray(22).getDouble(0)).isEqualTo(v.doubleValue());
                assertThat(row.getArray(23).getTimestamp(0, 9).toLocalDateTime())
                        .isEqualTo(toDateTime(v));

                assertThat(row.getArray(25).getDecimal(0, 15, 0))
                        .isEqualTo(DecimalData.fromBigDecimal(BigDecimal.valueOf(v), 15, 0));
                assertThat(row.getArray(26).getDecimal(0, 20, 0))
                        .isEqualTo(DecimalData.fromBigDecimal(BigDecimal.valueOf(v), 20, 0));
                assertThat(row.getArray(28).getDecimal(0, 15, 0))
                        .isEqualTo(DecimalData.fromBigDecimal(BigDecimal.valueOf(v), 15, 0));
                assertThat(row.getArray(29).getDecimal(0, 20, 0))
                        .isEqualTo(DecimalData.fromBigDecimal(BigDecimal.valueOf(v), 20, 0));
                assertThat(row.getMap(30).valueArray().getString(0)).hasToString("" + v);
                assertThat(row.getMap(31).valueArray().getBoolean(0)).isEqualTo(v % 2 == 0);
                assertThat(row.getMap(32).keyArray().getString(0)).hasToString("" + v);
                assertThat(row.getRow(33, 2).getString(0)).hasToString("" + v);
                assertThat(row.getRow(33, 2).getInt(1)).isEqualTo(v.intValue());
            }
            i++;
        }
        reader.close();
        return i - start;
    }

    private RowData newRow(Integer v) {
        Map<StringData, StringData> f30 = new HashMap<>();
        f30.put(StringData.fromString("" + v), StringData.fromString("" + v));

        Map<Integer, Boolean> f31 = new HashMap<>();
        f31.put(v, v % 2 == 0);

        Map<StringData, Integer> f32 = new HashMap<>();
        f32.put(StringData.fromString("" + v), v);

        return GenericRowData.of(
                StringData.fromString("" + v),
                v % 2 == 0,
                v.byteValue(),
                v.shortValue(),
                v,
                v.longValue(),
                v.floatValue(),
                v.doubleValue(),
                TimestampData.fromLocalDateTime(toDateTime(v)),
                DecimalData.fromBigDecimal(BigDecimal.valueOf(v), 5, 0),
                DecimalData.fromBigDecimal(BigDecimal.valueOf(v), 15, 0),
                DecimalData.fromBigDecimal(BigDecimal.valueOf(v), 20, 0),
                DecimalData.fromBigDecimal(BigDecimal.valueOf(v), 5, 0),
                DecimalData.fromBigDecimal(BigDecimal.valueOf(v), 15, 0),
                DecimalData.fromBigDecimal(BigDecimal.valueOf(v), 20, 0),
                new GenericArrayData(new Object[] {StringData.fromString("" + v), null}),
                new GenericArrayData(new Object[] {v % 2 == 0, null}),
                new GenericArrayData(new Object[] {v.byteValue(), null}),
                new GenericArrayData(new Object[] {v.shortValue(), null}),
                new GenericArrayData(new Object[] {v, null}),
                new GenericArrayData(new Object[] {v.longValue(), null}),
                new GenericArrayData(new Object[] {v.floatValue(), null}),
                new GenericArrayData(new Object[] {v.doubleValue(), null}),
                new GenericArrayData(
                        new Object[] {TimestampData.fromLocalDateTime(toDateTime(v)), null}),
                DecimalData.fromBigDecimal(BigDecimal.valueOf(v), 5, 0) == null
                        ? null
                        : new GenericArrayData(
                                new Object[] {
                                    DecimalData.fromBigDecimal(BigDecimal.valueOf(v), 5, 0), null
                                }),
                new GenericArrayData(
                        new Object[] {
                            DecimalData.fromBigDecimal(BigDecimal.valueOf(v), 15, 0), null
                        }),
                new GenericArrayData(
                        new Object[] {
                            DecimalData.fromBigDecimal(BigDecimal.valueOf(v), 20, 0), null
                        }),
                DecimalData.fromBigDecimal(BigDecimal.valueOf(v), 5, 0) == null
                        ? null
                        : new GenericArrayData(
                                new Object[] {
                                    DecimalData.fromBigDecimal(BigDecimal.valueOf(v), 5, 0), null
                                }),
                new GenericArrayData(
                        new Object[] {
                            DecimalData.fromBigDecimal(BigDecimal.valueOf(v), 15, 0), null
                        }),
                new GenericArrayData(
                        new Object[] {
                            DecimalData.fromBigDecimal(BigDecimal.valueOf(v), 20, 0), null
                        }),
                new GenericMapData(f30),
                new GenericMapData(f31),
                new GenericMapData(f32),
                GenericRowData.of(StringData.fromString("" + v), v));
    }

    private LocalDateTime toDateTime(Integer v) {
        v = (v > 0 ? v : -v) % 10000;
        return BASE_TIME.plusNanos(v).plusSeconds(v);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void testDictionary(int rowGroupSize) throws IOException {
        // prepare parquet file
        int number = 10000;
        List<RowData> records = new ArrayList<>(number);
        List<Integer> values = new ArrayList<>(number);
        Random random = new Random();
        int[] intValues = new int[10];
        // test large values in dictionary
        for (int i = 0; i < intValues.length; i++) {
            intValues[i] = random.nextInt();
        }
        for (int i = 0; i < number; i++) {
            Integer v = intValues[random.nextInt(10)];
            if (v == 0) {
                values.add(null);
                records.add(new GenericRowData(FIELD_NUMBER));
            } else {
                values.add(v);
                records.add(newRow(v));
            }
        }

        testNormalTypes(number, records, values, rowGroupSize);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void testPartialDictionary(int rowGroupSize) throws IOException {
        // prepare parquet file
        int number = 10000;
        List<RowData> records = new ArrayList<>(number);
        List<Integer> values = new ArrayList<>(number);
        Random random = new Random();
        int[] intValues = new int[10];
        // test large values in dictionary
        for (int i = 0; i < intValues.length; i++) {
            intValues[i] = random.nextInt();
        }
        for (int i = 0; i < number; i++) {
            Integer v = i < 5000 ? intValues[random.nextInt(10)] : i;
            if (v == 0) {
                values.add(null);
                records.add(new GenericRowData(FIELD_NUMBER));
            } else {
                values.add(v);
                records.add(newRow(v));
            }
        }

        testNormalTypes(number, records, values, rowGroupSize);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void testContinuousRepetition(int rowGroupSize) throws IOException {
        // prepare parquet file
        int number = 10000;
        List<RowData> records = new ArrayList<>(number);
        List<Integer> values = new ArrayList<>(number);
        Random random = new Random();
        for (int i = 0; i < 100; i++) {
            Integer v = random.nextInt(10);
            for (int j = 0; j < 100; j++) {
                if (v == 0) {
                    values.add(null);
                    records.add(new GenericRowData(FIELD_NUMBER));
                } else {
                    values.add(v);
                    records.add(newRow(v));
                }
            }
        }

        testNormalTypes(number, records, values, rowGroupSize);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void testLargeValue(int rowGroupSize) throws IOException {
        // prepare parquet file
        int number = 10000;
        List<RowData> records = new ArrayList<>(number);
        List<Integer> values = new ArrayList<>(number);
        Random random = new Random();
        for (int i = 0; i < number; i++) {
            Integer v = random.nextInt();
            if (v % 10 == 0) {
                values.add(null);
                records.add(new GenericRowData(FIELD_NUMBER));
            } else {
                values.add(v);
                records.add(newRow(v));
            }
        }

        testNormalTypes(number, records, values, rowGroupSize);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void testProject(int rowGroupSize) throws IOException {
        // prepare parquet file
        int number = 1000;
        List<RowData> records = new ArrayList<>(number);
        for (int i = 0; i < number; i++) {
            Integer v = i;
            records.add(newRow(v));
        }
        Path testPath = createTempParquetFile(tmpDir, records, rowGroupSize);
        RowType rowType = RowType.of(new DoubleType(), new TinyIntType(), new IntType());
        // test reader
        ParquetColumnarRowSplitReader reader =
                new ParquetColumnarRowSplitReader(
                        false,
                        true,
                        new Configuration(),
                        rowType.getChildren().toArray(new LogicalType[] {}),
                        new String[] {"f7", "f2", "f4"},
                        VectorizedColumnBatch::new,
                        500,
                        new org.apache.hadoop.fs.Path(testPath.getPath()),
                        0,
                        Long.MAX_VALUE);
        int i = 0;
        while (!reader.reachedEnd()) {
            ColumnarRowData row = reader.nextRecord();
            assertThat(row.getDouble(0)).isEqualTo(i);
            assertThat(row.getByte(1)).isEqualTo((byte) i);
            assertThat(row.getInt(2)).isEqualTo(i);
            i++;
        }
        reader.close();
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void testPartitionValues(int rowGroupSize) throws IOException {
        // prepare parquet file
        int number = 1000;
        List<RowData> records = new ArrayList<>(number);
        for (int i = 0; i < number; i++) {
            Integer v = i;
            records.add(newRow(v));
        }

        Path testPath = createTempParquetFile(tmpDir, records, rowGroupSize);

        // test reader
        Map<String, Object> partSpec = new HashMap<>();
        partSpec.put("f33", true);
        partSpec.put("f34", Date.valueOf("2020-11-23"));
        partSpec.put("f35", LocalDateTime.of(1999, 1, 1, 1, 1));
        partSpec.put("f36", 6.6);
        partSpec.put("f37", (byte) 9);
        partSpec.put("f38", (short) 10);
        partSpec.put("f39", 11);
        partSpec.put("f40", 12L);
        partSpec.put("f41", 13f);
        partSpec.put("f42", new BigDecimal(42));
        partSpec.put("f43", new BigDecimal(43));
        partSpec.put("f44", new BigDecimal(44));
        partSpec.put("f45", "f45");

        innerTestPartitionValues(testPath, partSpec, false, rowGroupSize);

        for (String k : new ArrayList<>(partSpec.keySet())) {
            partSpec.put(k, null);
        }

        innerTestPartitionValues(testPath, partSpec, true, rowGroupSize);
    }

    private void innerTestPartitionValues(
            Path testPath, Map<String, Object> partSpec, boolean nullPartValue, int rowGroupSize)
            throws IOException {
        LogicalType[] fieldTypes =
                new LogicalType[] {
                    new VarCharType(VarCharType.MAX_LENGTH),
                    new BooleanType(),
                    new TinyIntType(),
                    new SmallIntType(),
                    new IntType(),
                    new BigIntType(),
                    new FloatType(),
                    new DoubleType(),
                    new TimestampType(9),
                    new DecimalType(5, 0),
                    new DecimalType(15, 0),
                    new DecimalType(20, 0),
                    new DecimalType(5, 0),
                    new DecimalType(15, 0),
                    new DecimalType(20, 0),
                    new ArrayType(new VarCharType(VarCharType.MAX_LENGTH)),
                    new ArrayType(new BooleanType()),
                    new ArrayType(new TinyIntType()),
                    new ArrayType(new SmallIntType()),
                    new ArrayType(new IntType()),
                    new ArrayType(new BigIntType()),
                    new ArrayType(new FloatType()),
                    new ArrayType(new DoubleType()),
                    new ArrayType(new TimestampType(9)),
                    new ArrayType(new DecimalType(5, 0)),
                    new ArrayType(new DecimalType(15, 0)),
                    new ArrayType(new DecimalType(20, 0)),
                    new ArrayType(new DecimalType(5, 0)),
                    new ArrayType(new DecimalType(15, 0)),
                    new ArrayType(new DecimalType(20, 0)),
                    new MapType(
                            new VarCharType(VarCharType.MAX_LENGTH),
                            new VarCharType(VarCharType.MAX_LENGTH)),
                    new MapType(new IntType(), new BooleanType()),
                    RowType.of(new VarCharType(VarCharType.MAX_LENGTH), new IntType()),
                    new BooleanType(),
                    new DateType(),
                    new TimestampType(9),
                    new DoubleType(),
                    new TinyIntType(),
                    new SmallIntType(),
                    new IntType(),
                    new BigIntType(),
                    new FloatType(),
                    new DecimalType(5, 0),
                    new DecimalType(15, 0),
                    new DecimalType(20, 0),
                    new VarCharType(VarCharType.MAX_LENGTH)
                };
        ParquetColumnarRowSplitReader reader =
                ParquetSplitReaderUtil.genPartColumnarRowReader(
                        false,
                        true,
                        new Configuration(),
                        IntStream.range(0, 46).mapToObj(i -> "f" + i).toArray(String[]::new),
                        Arrays.stream(fieldTypes)
                                .map(TypeConversions::fromLogicalToDataType)
                                .toArray(DataType[]::new),
                        partSpec,
                        new int[] {7, 2, 4, 33, 37, 38, 39, 40, 41, 36, 34, 35, 42, 43, 44, 45},
                        rowGroupSize,
                        new Path(testPath.getPath()),
                        0,
                        Long.MAX_VALUE);
        int i = 0;
        while (!reader.reachedEnd()) {
            ColumnarRowData row = reader.nextRecord();

            // common values
            assertThat(row.getDouble(0)).isEqualTo(i);
            assertThat(row.getByte(1)).isEqualTo((byte) i);
            assertThat(row.getInt(2)).isEqualTo(i);

            // partition values
            if (nullPartValue) {
                for (int j = 3; j < 16; j++) {
                    assertThat(row.isNullAt(j)).isTrue();
                }
            } else {
                assertThat(row.getBoolean(3)).isTrue();
                assertThat(row.getByte(4)).isEqualTo((byte) 9);
                assertThat(row.getShort(5)).isEqualTo((short) 10);
                assertThat(row.getInt(6)).isEqualTo(11);
                assertThat(row.getLong(7)).isEqualTo(12);
                assertThat(row.getFloat(8)).isEqualTo(13);
                assertThat(row.getDouble(9)).isEqualTo(6.6);
                assertThat(row.getInt(10))
                        .isEqualTo(DateTimeUtils.toInternal(Date.valueOf("2020-11-23")));
                assertThat(row.getTimestamp(11, 9).toLocalDateTime())
                        .isEqualTo(LocalDateTime.of(1999, 1, 1, 1, 1));
                assertThat(row.getDecimal(12, 5, 0))
                        .isEqualTo(DecimalData.fromBigDecimal(new BigDecimal(42), 5, 0));
                assertThat(row.getDecimal(13, 15, 0))
                        .isEqualTo(DecimalData.fromBigDecimal(new BigDecimal(43), 15, 0));
                assertThat(row.getDecimal(14, 20, 0))
                        .isEqualTo(DecimalData.fromBigDecimal(new BigDecimal(44), 20, 0));
                assertThat(row.getString(15)).hasToString("f45");
            }

            i++;
        }
        reader.close();
    }
}
