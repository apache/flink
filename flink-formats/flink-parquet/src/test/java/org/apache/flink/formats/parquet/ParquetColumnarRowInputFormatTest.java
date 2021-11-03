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
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

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
import static org.apache.parquet.schema.Types.list;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/** Test for {@link ParquetColumnarRowInputFormat}. */
@RunWith(Parameterized.class)
public class ParquetColumnarRowInputFormatTest {

    private static final LocalDateTime BASE_TIME = LocalDateTime.now();
    private static final org.apache.flink.configuration.Configuration EMPTY_CONF =
            new org.apache.flink.configuration.Configuration();

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
                    RowType.of(new VarCharType(VarCharType.MAX_LENGTH), new IntType()));

    @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    private final int rowGroupSize;

    @Parameterized.Parameters(name = "rowGroupSize-{0}")
    public static Collection<Integer> parameters() {
        return Arrays.asList(10, 1000);
    }

    public ParquetColumnarRowInputFormatTest(int rowGroupSize) {
        this.rowGroupSize = rowGroupSize;
    }

    @Test
    public void testTypesReadWithSplits() throws IOException {
        int number = 10000;
        List<Integer> values = new ArrayList<>(number);
        Random random = new Random();
        for (int i = 0; i < number; i++) {
            int v = random.nextInt(number / 2);
            values.add(v % 10 == 0 ? null : v);
        }

        innerTestTypes(values);
    }

    @Test
    public void testDictionary() throws IOException {
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

        innerTestTypes(values);
    }

    @Test
    public void testPartialDictionary() throws IOException {
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

        innerTestTypes(values);
    }

    @Test
    public void testContinuousRepetition() throws IOException {
        int number = 10000;
        List<Integer> values = new ArrayList<>(number);
        Random random = new Random();
        for (int i = 0; i < 100; i++) {
            int v = random.nextInt(10);
            for (int j = 0; j < 100; j++) {
                values.add(v == 0 ? null : v);
            }
        }

        innerTestTypes(values);
    }

    @Test
    public void testLargeValue() throws IOException {
        int number = 10000;
        List<Integer> values = new ArrayList<>(number);
        Random random = new Random();
        for (int i = 0; i < number; i++) {
            int v = random.nextInt();
            values.add(v % 10 == 0 ? null : v);
        }

        innerTestTypes(values);
    }

    @Test
    public void testProjection() throws IOException {
        int number = 1000;
        List<RowData> records = new ArrayList<>(number);
        for (int i = 0; i < number; i++) {
            Integer v = i;
            records.add(newRow(v));
        }

        Path testPath = createTempParquetFile(TEMPORARY_FOLDER.newFolder(), records, rowGroupSize);
        // test reader
        LogicalType[] fieldTypes =
                new LogicalType[] {new DoubleType(), new TinyIntType(), new IntType()};
        ParquetColumnarRowInputFormat<FileSourceSplit> format =
                new ParquetColumnarRowInputFormat(
                        new Configuration(),
                        RowType.of(fieldTypes, new String[] {"f7", "f2", "f4"}),
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
                    assertEquals(i, row.getDouble(0), 0);
                    assertEquals((byte) i, row.getByte(1));
                    assertEquals(i, row.getInt(2));
                    cnt.incrementAndGet();
                });
    }

    @Test
    public void testProjectionReadUnknownField() throws IOException {
        int number = 1000;
        List<RowData> records = new ArrayList<>(number);
        for (int i = 0; i < number; i++) {
            Integer v = i;
            records.add(newRow(v));
        }

        Path testPath = createTempParquetFile(TEMPORARY_FOLDER.newFolder(), records, rowGroupSize);

        // test reader
        LogicalType[] fieldTypes =
                new LogicalType[] {
                    new DoubleType(), new TinyIntType(), new IntType(), new VarCharType()
                };
        ParquetColumnarRowInputFormat<FileSourceSplit> format =
                new ParquetColumnarRowInputFormat<>(
                        new Configuration(),
                        // f99 not exist in parquet file.
                        RowType.of(fieldTypes, new String[] {"f7", "f2", "f4", "f99"}),
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
                    assertEquals(i, row.getDouble(0), 0);
                    assertEquals((byte) i, row.getByte(1));
                    assertEquals(i, row.getInt(2));
                    assertTrue(row.isNullAt(3));
                    cnt.incrementAndGet();
                });
    }

    @Test
    public void testPartitionValues() throws IOException {
        // prepare parquet file
        int number = 1000;
        List<RowData> records = new ArrayList<>(number);
        for (int i = 0; i < number; i++) {
            Integer v = i;
            records.add(newRow(v));
        }

        File root = TEMPORARY_FOLDER.newFolder();

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
        Path testPath = createTempParquetFile(new File(root, partPath), records, rowGroupSize);

        innerTestPartitionValues(testPath, partitionKeys, false);

        // test null partition values

        for (String k : new ArrayList<>(partSpec.keySet())) {
            partSpec.put(k, "my_default_value");
        }

        partPath = generatePartitionPath(partSpec);
        testPath = createTempParquetFile(new File(root, partPath), records, rowGroupSize);

        innerTestPartitionValues(testPath, partitionKeys, true);
    }

    private void innerTestTypes(List<Integer> records) throws IOException {
        List<RowData> rows = records.stream().map(this::newRow).collect(Collectors.toList());
        Path testPath = createTempParquetFile(TEMPORARY_FOLDER.newFolder(), rows, rowGroupSize);

        // test reading and splitting
        long fileLen = testPath.getFileSystem().getFileStatus(testPath).getLen();
        int len1 = testReadingSplit(subList(records, 0), testPath, 0, fileLen / 3);
        int len2 = testReadingSplit(subList(records, len1), testPath, fileLen / 3, fileLen * 2 / 3);
        int len3 =
                testReadingSplit(
                        subList(records, len1 + len2), testPath, fileLen * 2 / 3, Long.MAX_VALUE);
        assertEquals(records.size(), len1 + len2 + len3);

        // test seek
        int cntAfterSeeking =
                testReadingSplit(
                        subList(records, records.size() / 2),
                        testPath,
                        0,
                        fileLen,
                        records.size() / 2);
        assertEquals(records.size() - records.size() / 2, cntAfterSeeking);
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
                        assertSame(previousRow.get(), row);
                    }

                    Integer v = expected.get(cnt.get());
                    if (v == null) {
                        assertTrue(row.isNullAt(0));
                        assertTrue(row.isNullAt(1));
                        assertTrue(row.isNullAt(2));
                        assertTrue(row.isNullAt(3));
                        assertTrue(row.isNullAt(4));
                        assertTrue(row.isNullAt(5));
                        assertTrue(row.isNullAt(6));
                        assertTrue(row.isNullAt(7));
                        assertTrue(row.isNullAt(8));
                        assertTrue(row.isNullAt(9));
                        assertTrue(row.isNullAt(10));
                        assertTrue(row.isNullAt(11));
                        assertTrue(row.isNullAt(12));
                        assertTrue(row.isNullAt(13));
                        assertTrue(row.isNullAt(14));
                        assertTrue(row.isNullAt(15));
                        assertTrue(row.isNullAt(16));
                        assertTrue(row.isNullAt(17));
                        assertTrue(row.isNullAt(18));
                        assertTrue(row.isNullAt(19));
                        assertTrue(row.isNullAt(20));
                        assertTrue(row.isNullAt(21));
                        assertTrue(row.isNullAt(22));
                        assertTrue(row.isNullAt(23));
                        assertTrue(row.isNullAt(24));
                        assertTrue(row.isNullAt(25));
                        assertTrue(row.isNullAt(26));
                        assertTrue(row.isNullAt(27));
                        assertTrue(row.isNullAt(28));
                        assertTrue(row.isNullAt(29));
                        assertTrue(row.isNullAt(30));
                        assertTrue(row.isNullAt(31));
                        assertTrue(row.isNullAt(32));
                    } else {
                        assertEquals("" + v, row.getString(0).toString());
                        assertEquals(v % 2 == 0, row.getBoolean(1));
                        assertEquals(v.byteValue(), row.getByte(2));
                        assertEquals(v.shortValue(), row.getShort(3));
                        assertEquals(v.intValue(), row.getInt(4));
                        assertEquals(v.longValue(), row.getLong(5));
                        assertEquals(v.floatValue(), row.getFloat(6), 0);
                        assertEquals(v.doubleValue(), row.getDouble(7), 0);
                        assertEquals(toDateTime(v), row.getTimestamp(8, 9).toLocalDateTime());
                        if (DecimalData.fromBigDecimal(BigDecimal.valueOf(v), 5, 0) == null) {
                            assertTrue(row.isNullAt(9));
                            assertTrue(row.isNullAt(12));
                            assertTrue(row.isNullAt(24));
                            assertTrue(row.isNullAt(27));
                        } else {
                            assertEquals(
                                    DecimalData.fromBigDecimal(BigDecimal.valueOf(v), 5, 0),
                                    row.getDecimal(9, 5, 0));
                            assertEquals(
                                    DecimalData.fromBigDecimal(BigDecimal.valueOf(v), 5, 0),
                                    row.getDecimal(12, 5, 0));
                            assertEquals(
                                    DecimalData.fromBigDecimal(BigDecimal.valueOf(v), 5, 0),
                                    row.getArray(24).getDecimal(0, 5, 0));
                            assertEquals(
                                    DecimalData.fromBigDecimal(BigDecimal.valueOf(v), 5, 0),
                                    row.getArray(27).getDecimal(0, 5, 0));
                        }
                        assertEquals(
                                BigDecimal.valueOf(v), row.getDecimal(10, 15, 0).toBigDecimal());
                        assertEquals(
                                BigDecimal.valueOf(v), row.getDecimal(11, 20, 0).toBigDecimal());
                        assertEquals(
                                BigDecimal.valueOf(v), row.getDecimal(13, 15, 0).toBigDecimal());
                        assertEquals(
                                BigDecimal.valueOf(v), row.getDecimal(14, 20, 0).toBigDecimal());
                        assertEquals("" + v, row.getArray(15).getString(0).toString());
                        assertEquals(v % 2 == 0, row.getArray(16).getBoolean(0));
                        assertEquals(v.byteValue(), row.getArray(17).getByte(0));
                        assertEquals(v.shortValue(), row.getArray(18).getShort(0));
                        assertEquals(v.intValue(), row.getArray(19).getInt(0));
                        assertEquals(v.longValue(), row.getArray(20).getLong(0));
                        assertEquals(v.floatValue(), row.getArray(21).getFloat(0), 0);
                        assertEquals(v.doubleValue(), row.getArray(22).getDouble(0), 0);
                        assertEquals(
                                toDateTime(v),
                                row.getArray(23).getTimestamp(0, 9).toLocalDateTime());

                        assertEquals(
                                DecimalData.fromBigDecimal(BigDecimal.valueOf(v), 15, 0),
                                row.getArray(25).getDecimal(0, 15, 0));
                        assertEquals(
                                DecimalData.fromBigDecimal(BigDecimal.valueOf(v), 20, 0),
                                row.getArray(26).getDecimal(0, 20, 0));
                        assertEquals(
                                DecimalData.fromBigDecimal(BigDecimal.valueOf(v), 15, 0),
                                row.getArray(28).getDecimal(0, 15, 0));
                        assertEquals(
                                DecimalData.fromBigDecimal(BigDecimal.valueOf(v), 20, 0),
                                row.getArray(29).getDecimal(0, 20, 0));
                        assertEquals("" + v, row.getMap(30).valueArray().getString(0).toString());
                        assertEquals(v % 2 == 0, row.getMap(31).valueArray().getBoolean(0));
                        assertEquals("" + v, row.getRow(32, 2).getString(0).toString());
                        assertEquals(v.intValue(), row.getRow(32, 2).getInt(1));
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
                GenericRowData.of(StringData.fromString("" + v), v));
        // new String[]{"" + v});
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

        RowType rowType =
                RowType.of(
                        fieldTypes,
                        IntStream.range(0, 46).mapToObj(i -> "f" + i).toArray(String[]::new));

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
                    assertEquals(i, row.getDouble(0), 0);
                    assertEquals((byte) i, row.getByte(1));
                    assertEquals(i, row.getInt(2));

                    // partition values
                    if (nullPartValue) {
                        for (int j = 3; j < 16; j++) {
                            assertTrue(row.isNullAt(j));
                        }
                    } else {
                        assertTrue(row.getBoolean(3));
                        assertEquals(9, row.getByte(4));
                        assertEquals(10, row.getShort(5));
                        assertEquals(11, row.getInt(6));
                        assertEquals(12, row.getLong(7));
                        assertEquals(13, row.getFloat(8), 0);
                        assertEquals(6.6, row.getDouble(9), 0);
                        assertEquals(
                                DateTimeUtils.toInternal(Date.valueOf("2020-11-23")),
                                row.getInt(10));
                        assertEquals(
                                LocalDateTime.of(1999, 1, 1, 1, 1),
                                row.getTimestamp(11, 9).toLocalDateTime());
                        assertEquals(
                                DecimalData.fromBigDecimal(new BigDecimal(24), 5, 0),
                                row.getDecimal(12, 5, 0));
                        assertEquals(
                                DecimalData.fromBigDecimal(new BigDecimal(25), 15, 0),
                                row.getDecimal(13, 15, 0));
                        assertEquals(
                                DecimalData.fromBigDecimal(new BigDecimal(26), 20, 0),
                                row.getDecimal(14, 20, 0));
                        assertEquals("f45", row.getString(15).toString());
                    }
                    cnt.incrementAndGet();
                });
    }

    private static <T> List<T> subList(List<T> list, int i) {
        return list.subList(i, list.size());
    }
}
