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

package org.apache.flink.formats.parquet;

import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.src.util.CheckpointedPosition;
import org.apache.flink.connector.file.table.PartitionFieldExtractor;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.row.ParquetRowDataBuilder;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
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
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.utils.DateTimeUtils;
import org.apache.flink.util.InstantiationUtil;

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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.connector.file.src.util.Utils.forEachRemaining;
import static org.apache.flink.table.utils.PartitionPathUtils.generatePartitionPath;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link ParquetColumnarRowInputFormat}. */
class ParquetColumnarRowInputFormatTest {

    private static final LocalDateTime BASE_TIME = LocalDateTime.now();
    private static final org.apache.flink.configuration.Configuration EMPTY_CONF =
            new org.apache.flink.configuration.Configuration();

    private static final LogicalType[] FIELD_TYPES =
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
                new DecimalType(15, 2),
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
                RowType.of(new VarCharType(VarCharType.MAX_LENGTH), new IntType())
            };

    private static final RowType ROW_TYPE =
            RowType.of(
                    FIELD_TYPES,
                    IntStream.range(0, FIELD_TYPES.length)
                            .mapToObj(i -> "f" + i)
                            .toArray(String[]::new));

    @TempDir private File folder;

    public static Collection<Integer> parameters() {
        return Arrays.asList(10, 1000);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void testTypesReadWithSplits(int rowGroupSize) throws IOException {
        int number = 10000;
        List<Integer> values = new ArrayList<>(number);
        Random random = new Random();
        for (int i = 0; i < number; i++) {
            int v = random.nextInt(number / 2);
            values.add(v % 10 == 0 ? null : v);
        }

        innerTestTypes(folder, values, rowGroupSize);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void testDictionary(int rowGroupSize) throws IOException {
        int number = 10000;
        List<Integer> values = new ArrayList<>(number);
        Random random = new Random();
        int[] intValues = new int[10];
        // test large values in dictionary
        for (int i = 0; i < intValues.length; i++) {
            intValues[i] = random.nextInt();
        }
        for (int i = 0; i < number; i++) {
            int v = intValues[random.nextInt(10)];
            values.add(v == 0 ? null : v);
        }

        innerTestTypes(folder, values, rowGroupSize);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void testPartialDictionary(int rowGroupSize) throws IOException {
        // prepare parquet file
        int number = 10000;
        List<Integer> values = new ArrayList<>(number);
        Random random = new Random();
        int[] intValues = new int[10];
        // test large values in dictionary
        for (int i = 0; i < intValues.length; i++) {
            intValues[i] = random.nextInt();
        }
        for (int i = 0; i < number; i++) {
            int v = i < 5000 ? intValues[random.nextInt(10)] : i;
            values.add(v == 0 ? null : v);
        }

        innerTestTypes(folder, values, rowGroupSize);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void testContinuousRepetition(int rowGroupSize) throws IOException {
        int number = 10000;
        List<Integer> values = new ArrayList<>(number);
        Random random = new Random();
        for (int i = 0; i < 100; i++) {
            int v = random.nextInt(10);
            for (int j = 0; j < 100; j++) {
                values.add(v == 0 ? null : v);
            }
        }

        innerTestTypes(folder, values, rowGroupSize);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void testLargeValue(int rowGroupSize) throws IOException {
        int number = 10000;
        List<Integer> values = new ArrayList<>(number);
        Random random = new Random();
        for (int i = 0; i < number; i++) {
            int v = random.nextInt();
            values.add(v % 10 == 0 ? null : v);
        }

        innerTestTypes(folder, values, rowGroupSize);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void testProjection(int rowGroupSize) throws IOException {
        int number = 100;
        final List<RowData> records =
                IntStream.range(0, number).mapToObj(this::newRow).collect(Collectors.toList());
        Path testPath = createTempParquetFile(folder, records, rowGroupSize);
        // test reader
        String[] readerColumnNames = new String[] {"f7", "f2", "f4"};
        final LogicalType[] fieldTypes =
                Arrays.stream(readerColumnNames)
                        .map(ROW_TYPE::getFieldIndex)
                        .map(ROW_TYPE::getTypeAt)
                        .toArray(LogicalType[]::new);
        ParquetColumnarRowInputFormat<FileSourceSplit> format =
                new ParquetColumnarRowInputFormat(
                        new Configuration(),
                        RowType.of(fieldTypes, readerColumnNames),
                        null,
                        500,
                        false,
                        true);

        AtomicInteger cnt = new AtomicInteger(0);
        forEachRemaining(
                format.createReader(
                        EMPTY_CONF,
                        new FileSourceSplit("id", testPath, 0, Long.MAX_VALUE, 0, Long.MAX_VALUE)),
                row -> {
                    int i = cnt.get();
                    assertThat(row.getDouble(0)).isEqualTo(i);
                    assertThat(row.getByte(1)).isEqualTo((byte) i);
                    assertThat(row.getInt(2)).isEqualTo(i);
                    cnt.incrementAndGet();
                });
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void testProjectionReadUnknownField(int rowGroupSize) throws IOException {
        int number = 100;
        final List<RowData> records =
                IntStream.range(0, number).mapToObj(this::newRow).collect(Collectors.toList());
        Path testPath = createTempParquetFile(folder, records, rowGroupSize);

        // test reader
        // f99 not exist in parquet file.
        String[] readerColumnNames = new String[] {"f7", "f2", "f4", "f99"};
        final LogicalType[] fieldTypes =
                Arrays.stream(readerColumnNames)
                        .map(ROW_TYPE::getFieldIndex)
                        .map(index -> (index != -1) ? ROW_TYPE.getTypeAt(index) : new VarCharType())
                        .toArray(LogicalType[]::new);

        ParquetColumnarRowInputFormat<FileSourceSplit> format =
                new ParquetColumnarRowInputFormat<>(
                        new Configuration(),
                        RowType.of(fieldTypes, readerColumnNames),
                        null,
                        500,
                        false,
                        true);

        AtomicInteger cnt = new AtomicInteger(0);
        forEachRemaining(
                format.createReader(
                        EMPTY_CONF,
                        new FileSourceSplit("id", testPath, 0, Long.MAX_VALUE, 0, Long.MAX_VALUE)),
                row -> {
                    int i = cnt.get();
                    assertThat(row.getDouble(0)).isEqualTo(i);
                    assertThat(row.getByte(1)).isEqualTo((byte) i);
                    assertThat(row.getInt(2)).isEqualTo(i);
                    assertThat(row.isNullAt(3)).isTrue();
                    cnt.incrementAndGet();
                });
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void testProjectionReadUnknownFieldAcrossFiles(int rowGroupSize) throws IOException {
        int number = 100;
        final List<RowData> records =
                IntStream.range(0, number).mapToObj(this::newRow).collect(Collectors.toList());
        // create parquet files for both legacy and new
        // f7 and f99 don’t exist in legacy parquet file
        // f99 doesn't exist in new parquet file
        // create legacy parquet file
        // assume f7 and following columns do not exist in legacy parquet file.
        RowType legacyParquetType =
                new RowType(
                        IntStream.range(0, 6)
                                .mapToObj(i -> ROW_TYPE.getFields().get(i))
                                .collect(Collectors.toList()));
        Path legacyParquetPath =
                createTempParquetFile(
                        new File(folder, "legacy"), legacyParquetType, records, rowGroupSize);
        // create new parquet file
        Path newParquetPath = createTempParquetFile(new File(folder, "new"), records, rowGroupSize);

        // test reader
        // f7 and f99 do not exist in parquet file.
        String[] readerColumnNames = new String[] {"f7", "f2", "f4", "f99"};
        final LogicalType[] fieldTypes =
                Arrays.stream(readerColumnNames)
                        .map(ROW_TYPE::getFieldIndex)
                        .map(index -> (index != -1) ? ROW_TYPE.getTypeAt(index) : new VarCharType())
                        .toArray(LogicalType[]::new);

        ParquetColumnarRowInputFormat<FileSourceSplit> format =
                new ParquetColumnarRowInputFormat<>(
                        new Configuration(),
                        RowType.of(fieldTypes, readerColumnNames),
                        null,
                        500,
                        false,
                        true);

        AtomicInteger cnt = new AtomicInteger(0);
        // iterate data in both legacy and new parquet file separately
        // equivalent to the call function: FileSource.forBulkFileFormat(BulkFormat,Path...);
        // f7 is expected to be null in the legacy parquet file
        forEachRemaining(
                format.createReader(
                        EMPTY_CONF,
                        new FileSourceSplit(
                                "id", legacyParquetPath, 0, Long.MAX_VALUE, 0, Long.MAX_VALUE)),
                row -> {
                    int i = cnt.get();
                    assertThat(row.isNullAt(0)).isTrue();
                    assertThat(row.getByte(1)).isEqualTo((byte) i);
                    assertThat(row.getInt(2)).isEqualTo(i);
                    assertThat(row.isNullAt(3)).isTrue();
                    cnt.incrementAndGet();
                });
        // f7 is expected to exist in the new parquet file
        cnt.set(0);
        forEachRemaining(
                format.createReader(
                        EMPTY_CONF,
                        new FileSourceSplit(
                                "id", newParquetPath, 0, Long.MAX_VALUE, 0, Long.MAX_VALUE)),
                row -> {
                    int i = cnt.get();
                    assertThat(row.getDouble(0)).isEqualTo(i);
                    assertThat(row.getByte(1)).isEqualTo((byte) i);
                    assertThat(row.getInt(2)).isEqualTo(i);
                    assertThat(row.isNullAt(3)).isTrue();
                    cnt.incrementAndGet();
                });
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void testPartitionValues(int rowGroupSize) throws IOException {
        // prepare parquet file
        int number = 1000;
        final List<RowData> records =
                IntStream.range(0, number).mapToObj(this::newRow).collect(Collectors.toList());
        List<String> partitionKeys =
                Arrays.asList(
                        "f33", "f34", "f35", "f36", "f37", "f38", "f39", "f40", "f41", "f42", "f43",
                        "f44", "f45");

        // test partition values

        LinkedHashMap<String, String> partSpec = new LinkedHashMap<>();
        partSpec.put("f33", "true");
        partSpec.put("f34", Date.valueOf("2020-11-23").toString());
        partSpec.put("f35", LocalDateTime.of(1999, 1, 1, 1, 1).toString());
        partSpec.put("f36", "6.6");
        partSpec.put("f37", "9");
        partSpec.put("f38", "10");
        partSpec.put("f39", "11");
        partSpec.put("f40", "12");
        partSpec.put("f41", "13");
        partSpec.put("f42", "24");
        partSpec.put("f43", "25");
        partSpec.put("f44", "26");
        partSpec.put("f45", "f45");

        String partPath = generatePartitionPath(partSpec);
        Path testPath = createTempParquetFile(new File(folder, partPath), records, rowGroupSize);

        innerTestPartitionValues(testPath, partitionKeys, false);

        // test null partition values

        for (String k : new ArrayList<>(partSpec.keySet())) {
            partSpec.put(k, "my_default_value");
        }

        partPath = generatePartitionPath(partSpec);
        testPath = createTempParquetFile(new File(folder, partPath), records, rowGroupSize);

        innerTestPartitionValues(testPath, partitionKeys, true);
    }

    private void innerTestTypes(File folder, List<Integer> records, int rowGroupSize)
            throws IOException {
        List<RowData> rows = records.stream().map(this::newRow).collect(Collectors.toList());
        Path testPath = createTempParquetFile(folder, rows, rowGroupSize);

        // test reading and splitting
        long fileLen = testPath.getFileSystem().getFileStatus(testPath).getLen();
        int len1 = testReadingSplit(subList(records, 0), testPath, 0, fileLen / 3);
        int len2 = testReadingSplit(subList(records, len1), testPath, fileLen / 3, fileLen * 2 / 3);
        int len3 =
                testReadingSplit(
                        subList(records, len1 + len2), testPath, fileLen * 2 / 3, Long.MAX_VALUE);
        assertThat(len1 + len2 + len3).isEqualTo(records.size());

        // test seek
        int cntAfterSeeking =
                testReadingSplit(
                        subList(records, records.size() / 2),
                        testPath,
                        0,
                        fileLen,
                        records.size() / 2);
        assertThat(cntAfterSeeking).isEqualTo(records.size() - records.size() / 2);
    }

    private Path createTempParquetFile(File folder, List<RowData> rows, int rowGroupSize)
            throws IOException {
        return this.createTempParquetFile(folder, ROW_TYPE, rows, rowGroupSize);
    }

    private Path createTempParquetFile(
            File folder, RowType rowType, List<RowData> rows, int rowGroupSize) throws IOException {
        // write data
        Path path = new Path(folder.getPath(), UUID.randomUUID().toString());
        Configuration conf = new Configuration();
        conf.setInt("parquet.block.size", rowGroupSize);
        ParquetWriterFactory<RowData> factory =
                ParquetRowDataBuilder.createWriterFactory(rowType, conf, false);
        BulkWriter<RowData> writer =
                factory.create(path.getFileSystem().create(path, FileSystem.WriteMode.OVERWRITE));
        for (int i = 0; i < rows.size(); i++) {
            writer.addElement(rows.get(i));
        }

        writer.flush();
        writer.finish();
        return path;
    }

    private int testReadingSplit(
            List<Integer> expected, Path path, long splitStart, long splitLength)
            throws IOException {
        return testReadingSplit(expected, path, splitStart, splitLength, 0);
    }

    private int testReadingSplit(
            List<Integer> expected, Path path, long splitStart, long splitLength, long seekToRow)
            throws IOException {

        ParquetColumnarRowInputFormat format =
                new ParquetColumnarRowInputFormat(
                        new Configuration(), ROW_TYPE, null, 500, false, true);

        // validate java serialization
        try {
            InstantiationUtil.clone(format);
        } catch (ClassNotFoundException e) {
            throw new IOException(e);
        }

        FileStatus fileStatus = path.getFileSystem().getFileStatus(path);

        BulkFormat.Reader<RowData> reader =
                format.restoreReader(
                        EMPTY_CONF,
                        new FileSourceSplit(
                                "id",
                                path,
                                splitStart,
                                splitLength,
                                fileStatus.getModificationTime(),
                                fileStatus.getLen(),
                                new String[0],
                                new CheckpointedPosition(
                                        CheckpointedPosition.NO_OFFSET, seekToRow)));

        AtomicInteger cnt = new AtomicInteger(0);
        final AtomicReference<RowData> previousRow = new AtomicReference<>();
        forEachRemaining(
                reader,
                row -> {
                    if (previousRow.get() == null) {
                        previousRow.set(row);
                    } else {
                        // ParquetColumnarRowInputFormat should only have one row instance.
                        assertThat(row).isSameAs(previousRow.get());
                    }
                    Integer v = expected.get(cnt.get());
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
                    } else {
                        assertThat(row.getString(0)).hasToString("" + v);
                        assertThat(row.getBoolean(1)).isEqualTo(v % 2 == 0);
                        assertThat(row.getByte(2)).isEqualTo(v.byteValue());
                        assertThat(row.getShort(3)).isEqualTo(v.shortValue());
                        assertThat(row.getInt(4)).isEqualTo(v.intValue());
                        assertThat(row.getLong(5)).isEqualTo(v.longValue());
                        assertThat(row.getFloat(6)).isEqualTo(v.floatValue());
                        assertThat(row.getDouble(7)).isEqualTo(v.doubleValue());
                        assertThat(row.getTimestamp(8, 9).toLocalDateTime())
                                .isEqualTo(toDateTime(v));
                        if (DecimalData.fromBigDecimal(BigDecimal.valueOf(v), 5, 0) == null) {
                            assertThat(row.isNullAt(9)).isTrue();
                            assertThat(row.isNullAt(12)).isTrue();
                            assertThat(row.isNullAt(24)).isTrue();
                            assertThat(row.isNullAt(27)).isTrue();
                        } else {
                            assertThat(row.getDecimal(9, 5, 0))
                                    .isEqualTo(
                                            DecimalData.fromBigDecimal(
                                                    BigDecimal.valueOf(v), 5, 0));
                            assertThat(row.getDecimal(12, 5, 0))
                                    .isEqualTo(
                                            DecimalData.fromBigDecimal(
                                                    BigDecimal.valueOf(v), 5, 0));
                            assertThat(row.getArray(24).getDecimal(0, 5, 0))
                                    .isEqualTo(
                                            DecimalData.fromBigDecimal(
                                                    BigDecimal.valueOf(v), 5, 0));
                            assertThat(row.getArray(27).getDecimal(0, 5, 0))
                                    .isEqualTo(
                                            DecimalData.fromBigDecimal(
                                                    BigDecimal.valueOf(v), 5, 0));
                        }
                        assertThat(row.getDecimal(10, 15, 2))
                                .isEqualTo(DecimalData.fromUnscaledLong(v.longValue(), 15, 2));
                        assertThat(row.getDecimal(11, 20, 0).toBigDecimal())
                                .isEqualTo(BigDecimal.valueOf(v));
                        assertThat(row.getDecimal(13, 15, 0).toBigDecimal())
                                .isEqualTo(BigDecimal.valueOf(v));
                        assertThat(row.getDecimal(14, 20, 0).toBigDecimal())
                                .isEqualTo(BigDecimal.valueOf(v));
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
                                .isEqualTo(
                                        DecimalData.fromBigDecimal(BigDecimal.valueOf(v), 15, 0));
                        assertThat(row.getArray(26).getDecimal(0, 20, 0))
                                .isEqualTo(
                                        DecimalData.fromBigDecimal(BigDecimal.valueOf(v), 20, 0));
                        assertThat(row.getArray(28).getDecimal(0, 15, 0))
                                .isEqualTo(
                                        DecimalData.fromBigDecimal(BigDecimal.valueOf(v), 15, 0));
                        assertThat(row.getArray(29).getDecimal(0, 20, 0))
                                .isEqualTo(
                                        DecimalData.fromBigDecimal(BigDecimal.valueOf(v), 20, 0));
                        assertThat(row.getMap(30).valueArray().getString(0)).hasToString("" + v);
                        assertThat(row.getMap(31).valueArray().getBoolean(0)).isEqualTo(v % 2 == 0);
                        assertThat(row.getRow(32, 2).getString(0)).hasToString("" + v);
                        assertThat(row.getRow(32, 2).getInt(1)).isEqualTo(v.intValue());
                    }
                    cnt.incrementAndGet();
                });

        return cnt.get();
    }

    private RowData newRow(Integer v) {
        if (v == null) {
            return new GenericRowData(ROW_TYPE.getFieldCount());
        }

        Map<StringData, StringData> f30 = new HashMap<>();
        f30.put(StringData.fromString("" + v), StringData.fromString("" + v));

        Map<Integer, Boolean> f31 = new HashMap<>();
        f31.put(v, v % 2 == 0);

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
                DecimalData.fromUnscaledLong(v.longValue(), 15, 2),
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
                GenericRowData.of(StringData.fromString("" + v), v));
    }

    private LocalDateTime toDateTime(Integer v) {
        v = (v > 0 ? v : -v) % 10000;
        return BASE_TIME.plusNanos(v).plusSeconds(v);
    }

    private void innerTestPartitionValues(
            Path testPath, List<String> partitionKeys, boolean nullPartValue) throws IOException {
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
                    new DecimalType(15, 2),
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

        RowType rowType =
                RowType.of(
                        fieldTypes,
                        IntStream.range(0, fieldTypes.length)
                                .mapToObj(i -> "f" + i)
                                .toArray(String[]::new));

        int[] projected = new int[] {7, 2, 4, 33, 37, 38, 39, 40, 41, 36, 34, 35, 42, 43, 44, 45};

        RowType producedType =
                new RowType(
                        Arrays.stream(projected)
                                .mapToObj(i -> rowType.getFields().get(i))
                                .collect(Collectors.toList()));

        ParquetColumnarRowInputFormat<FileSourceSplit> format =
                ParquetColumnarRowInputFormat.createPartitionedFormat(
                        new Configuration(),
                        producedType,
                        InternalTypeInfo.of(producedType),
                        partitionKeys,
                        PartitionFieldExtractor.forFileSystem("my_default_value"),
                        500,
                        false,
                        true);

        FileStatus fileStatus = testPath.getFileSystem().getFileStatus(testPath);

        AtomicInteger cnt = new AtomicInteger(0);
        forEachRemaining(
                format.createReader(
                        EMPTY_CONF,
                        new FileSourceSplit(
                                "id",
                                testPath,
                                0,
                                Long.MAX_VALUE,
                                fileStatus.getModificationTime(),
                                fileStatus.getLen())),
                row -> {
                    int i = cnt.get();
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
                        assertThat(row.getFloat(8)).isEqualTo((float) 13);
                        assertThat(row.getDouble(9)).isEqualTo(6.6);
                        assertThat(row.getInt(10))
                                .isEqualTo(DateTimeUtils.toInternal(Date.valueOf("2020-11-23")));
                        assertThat(row.getTimestamp(11, 9).toLocalDateTime())
                                .isEqualTo(LocalDateTime.of(1999, 1, 1, 1, 1));
                        assertThat(row.getDecimal(12, 5, 0))
                                .isEqualTo(DecimalData.fromBigDecimal(new BigDecimal(24), 5, 0));
                        assertThat(row.getDecimal(13, 15, 0))
                                .isEqualTo(DecimalData.fromBigDecimal(new BigDecimal(25), 15, 0));
                        assertThat(row.getDecimal(14, 20, 0))
                                .isEqualTo(DecimalData.fromBigDecimal(new BigDecimal(26), 20, 0));
                        assertThat(row.getString(15)).hasToString("f45");
                    }
                    cnt.incrementAndGet();
                });
    }

    private static <T> List<T> subList(List<T> list, int i) {
        return list.subList(i, list.size());
    }
}
