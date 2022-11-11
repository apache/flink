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

package org.apache.flink.orc;

import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.DecimalDataUtils;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

import static org.apache.flink.orc.OrcColumnarRowInputFormatTest.copyFileFromResource;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link OrcColumnarRowSplitReader}. */
public class OrcColumnarRowSplitReaderTest {

    protected static final int BATCH_SIZE = 10;

    private final DataType[] testSchemaFlat =
            new DataType[] {
                DataTypes.INT(),
                DataTypes.STRING(),
                DataTypes.STRING(),
                DataTypes.STRING(),
                DataTypes.INT(),
                DataTypes.STRING(),
                DataTypes.INT(),
                DataTypes.INT(),
                DataTypes.INT()
            };

    private final DataType[] testSchemaDecimal = new DataType[] {DataTypes.DECIMAL(10, 5)};

    private static Path testFileFlat;
    private static Path testFileDecimal;

    @BeforeAll
    static void setupFiles(@TempDir java.nio.file.Path tmpDir) {
        testFileFlat =
                copyFileFromResource("test-data-flat.orc", tmpDir.resolve("test-data-flat.orc"));
        testFileDecimal =
                copyFileFromResource(
                        "test-data-decimal.orc", tmpDir.resolve("test-data-decimal.orc"));
    }

    @Test
    void testReadFileInSplits() throws IOException {
        FileInputSplit[] splits = createSplits(testFileFlat, 4);

        long cnt = 0;
        long totalF0 = 0;
        // read all splits
        for (FileInputSplit split : splits) {

            try (OrcColumnarRowSplitReader reader =
                    createReader(new int[] {0, 1}, testSchemaFlat, new HashMap<>(), split)) {
                // read and count all rows
                while (!reader.reachedEnd()) {
                    RowData row = reader.nextRecord(null);
                    assertThat(row.isNullAt(0)).isFalse();
                    assertThat(row.isNullAt(1)).isFalse();
                    totalF0 += row.getInt(0);
                    assertThat(row.getString(1).toString()).isNotNull();
                    cnt++;
                }
            }
        }
        // check that all rows have been read
        assertThat(cnt).isEqualTo(1920800);
        assertThat(totalF0).isEqualTo(1844737280400L);
    }

    @Test
    void testReadDecimalTypeFile() throws IOException {
        FileInputSplit[] splits = createSplits(testFileDecimal, 1);

        try (OrcColumnarRowSplitReader reader =
                createReader(new int[] {0}, testSchemaDecimal, new HashMap<>(), splits[0])) {
            assertThat(reader.reachedEnd()).isFalse();
            RowData row = reader.nextRecord(null);

            // validate first row
            assertThat(row).isNotNull();
            assertThat(row.getArity()).isEqualTo(1);
            assertThat(row.getDecimal(0, 10, 5))
                    .isEqualTo(DecimalDataUtils.castFrom(-1000.5d, 10, 5));

            // check correct number of rows
            long cnt = 1;
            long nullCount = 0;
            while (!reader.reachedEnd()) {
                row = reader.nextRecord(null);
                if (!row.isNullAt(0)) {
                    assertThat(row.getDecimal(0, 10, 5)).isNotNull();
                } else {
                    nullCount++;
                }
                cnt++;
            }
            assertThat(cnt).isEqualTo(6000);
            assertThat(nullCount).isEqualTo(2000);
        }
    }

    @Test
    void testReadFileWithSelectFields() throws IOException {
        FileInputSplit[] splits = createSplits(testFileFlat, 4);

        long cnt = 0;
        long totalF0 = 0;

        Map<String, Object> partSpec = new HashMap<>();
        partSpec.put("f1", 1);
        partSpec.put("f3", 3L);
        partSpec.put("f5", "f5");

        // read all splits
        for (FileInputSplit split : splits) {
            try (OrcColumnarRowSplitReader<VectorizedRowBatch> reader =
                    createReader(
                            new int[] {1, 3, 2, 5, 0},
                            new DataType[] {
                                /* 0 */ DataTypes.INT(),
                                /* 1 */ DataTypes.INT(), // part-1
                                /* 2 */ DataTypes.STRING(),
                                /* 3 */ DataTypes.BIGINT(), // part-2
                                /* 4 */ DataTypes.STRING(),
                                /* 5 */ DataTypes.STRING(), // part-3
                                /* 6 */ DataTypes.STRING(),
                                /* 7 */ DataTypes.INT(),
                                /* 8 */ DataTypes.STRING(),
                                /* 9 */ DataTypes.INT(),
                                /* 10*/ DataTypes.INT(),
                                /* 11*/ DataTypes.INT()
                            },
                            partSpec,
                            split)) {
                // read and count all rows
                while (!reader.reachedEnd()) {
                    RowData row = reader.nextRecord(null);

                    assertThat(row.getArity()).isEqualTo(2);
                    // data values
                    assertThat(row.isNullAt(1)).isFalse();
                    assertThat(row.isNullAt(0)).isFalse();
                    totalF0 += row.getInt(1);
                    assertThat(row.getString(0).toString()).isNotNull();

                    cnt++;
                }
            }
        }
        // check that all rows have been read
        assertThat(cnt).isEqualTo(1920800);
        assertThat(totalF0).isEqualTo(1844737280400L);
    }

    @Test
    void testReadFileWithPartitionValues() throws IOException {
        FileInputSplit[] splits = createSplits(testFileFlat, 4);

        long cnt = 0;
        long totalF0 = 0;
        // read all splits
        for (FileInputSplit split : splits) {

            try (OrcColumnarRowSplitReader reader =
                    createReader(new int[] {2, 0, 1}, testSchemaFlat, new HashMap<>(), split)) {
                // read and count all rows
                while (!reader.reachedEnd()) {
                    RowData row = reader.nextRecord(null);
                    assertThat(row.isNullAt(0)).isFalse();
                    assertThat(row.isNullAt(1)).isFalse();
                    assertThat(row.isNullAt(2)).isFalse();
                    assertThat(row.getString(0).toString()).isNotNull();
                    totalF0 += row.getInt(1);
                    assertThat(row.getString(2).toString()).isNotNull();
                    cnt++;
                }
            }
        }
        // check that all rows have been read
        assertThat(cnt).isEqualTo(1920800);
        assertThat(totalF0).isEqualTo(1844737280400L);
    }

    protected void prepareReadFileWithTypes(String file, int rowSize) throws IOException {
        // NOTE: orc has field name information, so name should be same as orc
        TypeDescription schema =
                TypeDescription.fromString(
                        "struct<"
                                + "f0:float,"
                                + "f1:double,"
                                + "f2:timestamp,"
                                + "f3:tinyint,"
                                + "f4:smallint"
                                + ">");

        org.apache.hadoop.fs.Path filePath = new org.apache.hadoop.fs.Path(file);
        Configuration conf = new Configuration();

        Writer writer =
                OrcFile.createWriter(filePath, OrcFile.writerOptions(conf).setSchema(schema));

        VectorizedRowBatch batch = schema.createRowBatch(rowSize);
        DoubleColumnVector col0 = (DoubleColumnVector) batch.cols[0];
        DoubleColumnVector col1 = (DoubleColumnVector) batch.cols[1];
        TimestampColumnVector col2 = (TimestampColumnVector) batch.cols[2];
        LongColumnVector col3 = (LongColumnVector) batch.cols[3];
        LongColumnVector col4 = (LongColumnVector) batch.cols[4];

        col0.noNulls = false;
        col1.noNulls = false;
        col2.noNulls = false;
        col3.noNulls = false;
        col4.noNulls = false;
        for (int i = 0; i < rowSize - 1; i++) {
            col0.vector[i] = i;
            col1.vector[i] = i;

            Timestamp timestamp = toTimestamp(i);
            col2.time[i] = timestamp.getTime();
            col2.nanos[i] = timestamp.getNanos();

            col3.vector[i] = i;
            col4.vector[i] = i;
        }

        col0.isNull[rowSize - 1] = true;
        col1.isNull[rowSize - 1] = true;
        col2.isNull[rowSize - 1] = true;
        col3.isNull[rowSize - 1] = true;
        col4.isNull[rowSize - 1] = true;

        batch.size = rowSize;
        writer.addRowBatch(batch);
        batch.reset();
        writer.close();
    }

    @Test
    void testReadFileWithTypes(@TempDir File folder) throws IOException {
        String file = new File(folder, "testOrc").getPath();
        int rowSize = 1024;

        prepareReadFileWithTypes(file, rowSize);

        // second test read.
        FileInputSplit split = createSplits(new Path(file), 1)[0];

        int cnt = 0;
        Map<String, Object> partSpec = new HashMap<>();
        partSpec.put("f5", true);
        partSpec.put("f6", new Date(562423));
        try (OrcColumnarRowSplitReader<VectorizedRowBatch> reader =
                createReader(
                        new int[] {2, 0, 1, 3, 4, 5, 6},
                        new DataType[] {
                            DataTypes.FLOAT(),
                            DataTypes.DOUBLE(),
                            DataTypes.TIMESTAMP(),
                            DataTypes.TINYINT(),
                            DataTypes.SMALLINT(),
                            DataTypes.BOOLEAN(),
                            DataTypes.DATE(),
                        },
                        partSpec,
                        split)) {
            // read and count all rows
            while (!reader.reachedEnd()) {
                RowData row = reader.nextRecord(null);

                // only physical field consumed
                assertThat(row.getArity()).isEqualTo(5);
                if (cnt == rowSize - 1) {
                    assertThat(row.isNullAt(0)).isTrue();
                    assertThat(row.isNullAt(1)).isTrue();
                    assertThat(row.isNullAt(2)).isTrue();
                    assertThat(row.isNullAt(3)).isTrue();
                    assertThat(row.isNullAt(4)).isTrue();
                } else {
                    assertThat(row.isNullAt(0)).isFalse();
                    assertThat(row.isNullAt(1)).isFalse();
                    assertThat(row.isNullAt(2)).isFalse();
                    assertThat(row.isNullAt(3)).isFalse();
                    assertThat(row.isNullAt(4)).isFalse();
                    assertThat(row.getTimestamp(0, 9))
                            .isEqualTo(TimestampData.fromTimestamp(toTimestamp(cnt)));
                    assertThat(row.getFloat(1)).isEqualTo((float) cnt);
                    assertThat(row.getDouble(2)).isEqualTo(cnt);
                    assertThat(row.getByte(3)).isEqualTo((byte) cnt);
                    assertThat(row.getShort(4)).isEqualTo((short) cnt);
                }
                cnt++;
            }
        }
        // check that all rows have been read
        assertThat(cnt).isEqualTo(rowSize);
    }

    @Test
    void testReachEnd() throws Exception {
        FileInputSplit[] splits = createSplits(testFileFlat, 1);
        try (OrcColumnarRowSplitReader reader =
                createReader(new int[] {0, 1}, testSchemaFlat, new HashMap<>(), splits[0])) {
            while (!reader.reachedEnd()) {
                reader.nextRecord(null);
            }
            assertThat(reader.reachedEnd()).isTrue();
        }
    }

    protected static Timestamp toTimestamp(int i) {
        return new Timestamp(
                i + 1000, (i % 12) + 1, (i % 28) + 1, i % 24, i % 60, i % 60, i * 1_000 + i);
    }

    protected OrcColumnarRowSplitReader<VectorizedRowBatch> createReader(
            int[] selectedFields,
            DataType[] fullTypes,
            Map<String, Object> partitionSpec,
            FileInputSplit split)
            throws IOException {
        return OrcSplitReaderUtil.genPartColumnarRowReader(
                "2.3.0",
                new Configuration(),
                IntStream.range(0, fullTypes.length).mapToObj(i -> "f" + i).toArray(String[]::new),
                fullTypes,
                partitionSpec,
                selectedFields,
                new ArrayList<>(),
                BATCH_SIZE,
                split.getPath(),
                split.getStart(),
                split.getLength());
    }

    private static FileInputSplit[] createSplits(Path path, int minNumSplits) throws IOException {
        return new DummyFileInputFormat(path).createInputSplits(minNumSplits);
    }

    private static class DummyFileInputFormat extends FileInputFormat<Row> {

        private static final long serialVersionUID = 1L;

        private DummyFileInputFormat(Path path) {
            super(path);
        }

        @Override
        public boolean reachedEnd() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Row nextRecord(Row reuse) {
            throw new UnsupportedOperationException();
        }
    }
}
