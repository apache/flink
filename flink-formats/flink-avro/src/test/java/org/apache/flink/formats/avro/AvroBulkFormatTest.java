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

package org.apache.flink.formats.avro;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.src.util.CheckpointedPosition;
import org.apache.flink.connector.file.src.util.RecordAndPosition;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.StringUtils;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileConstants;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.apache.flink.formats.avro.AvroBulkFormatTestUtils.ROW_TYPE;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link AbstractAvroBulkFormat}. */
class AvroBulkFormatTest {

    private static final List<RowData> TEST_DATA =
            Arrays.asList(
                    // -------- batch 0, block start 186 --------
                    GenericRowData.of(
                            StringData.fromString("AvroBulk"), StringData.fromString("FormatTest")),
                    GenericRowData.of(
                            StringData.fromString("Apache"), StringData.fromString("Flink")),
                    GenericRowData.of(
                            StringData.fromString(
                                    "永和九年，岁在癸丑，暮春之初，会于会稽山阴之兰亭，修禊事也。群贤毕至，少"
                                            + "长咸集。此地有崇山峻岭，茂林修竹，又有清流激湍，映带左右。引"
                                            + "以为流觞曲水，列坐其次。虽无丝竹管弦之盛，一觞一咏，亦足以畅"
                                            + "叙幽情。"),
                            StringData.fromString("")),
                    // -------- batch 1, block start 547 --------
                    GenericRowData.of(
                            StringData.fromString("File"), StringData.fromString("Format")),
                    GenericRowData.of(
                            null,
                            StringData.fromString(
                                    "This is a string with English, 中文 and even 🍎🍌🍑🥝🍍🥭🍐")),
                    // -------- batch 2, block start 659 --------
                    GenericRowData.of(
                            StringData.fromString("block with"),
                            StringData.fromString("only one record"))
                    // -------- file length 706 --------
                    );
    private static final List<Integer> BLOCK_STARTS = Arrays.asList(186, 547, 659);

    private File tmpFile;

    @BeforeEach
    public void before() throws IOException {
        tmpFile = Files.createTempFile("avro-bulk-format-test", ".avro").toFile();
        tmpFile.createNewFile();
        FileOutputStream out = new FileOutputStream(tmpFile);

        Schema schema = AvroSchemaConverter.convertToSchema(ROW_TYPE);
        RowDataToAvroConverters.RowDataToAvroConverter converter =
                RowDataToAvroConverters.createConverter(ROW_TYPE);

        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.create(schema, out);
        dataFileWriter.setSyncInterval(64);

        for (RowData rowData : TEST_DATA) {
            dataFileWriter.append((GenericRecord) converter.convert(schema, rowData));
        }

        dataFileWriter.close();
    }

    @AfterEach
    public void after() throws IOException {
        FileUtils.deleteFileOrDirectory(tmpFile);
    }

    @Test
    void testReadWholeFileWithOneSplit() throws IOException {
        AvroBulkFormatTestUtils.TestingAvroBulkFormat bulkFormat =
                new AvroBulkFormatTestUtils.TestingAvroBulkFormat();
        assertSplit(
                bulkFormat,
                Collections.singletonList(
                        new SplitInfo(
                                0,
                                tmpFile.length(),
                                Arrays.asList(
                                        new BatchInfo(0, 3),
                                        new BatchInfo(3, 5),
                                        new BatchInfo(5, 6)))));
    }

    @Test
    void testReadWholeFileWithMultipleSplits() throws IOException {
        AvroBulkFormatTestUtils.TestingAvroBulkFormat bulkFormat =
                new AvroBulkFormatTestUtils.TestingAvroBulkFormat();
        long splitLength = tmpFile.length() / 3;
        assertSplit(
                bulkFormat,
                Arrays.asList(
                        new SplitInfo(
                                0, splitLength, Collections.singletonList(new BatchInfo(0, 3))),
                        new SplitInfo(splitLength, splitLength * 2, Collections.emptyList()),
                        new SplitInfo(
                                splitLength * 2,
                                tmpFile.length(),
                                Arrays.asList(new BatchInfo(3, 5), new BatchInfo(5, 6)))));
    }

    @Test
    void testSplitsAtCriticalLocations() throws IOException {
        AvroBulkFormatTestUtils.TestingAvroBulkFormat bulkFormat =
                new AvroBulkFormatTestUtils.TestingAvroBulkFormat();
        assertSplit(
                bulkFormat,
                Arrays.asList(
                        // ends just before the new block
                        new SplitInfo(
                                BLOCK_STARTS.get(0) - DataFileConstants.SYNC_SIZE,
                                BLOCK_STARTS.get(1) - DataFileConstants.SYNC_SIZE,
                                Collections.singletonList(new BatchInfo(0, 3))),
                        // ends just at the beginning of new block
                        new SplitInfo(
                                BLOCK_STARTS.get(1) - DataFileConstants.SYNC_SIZE,
                                BLOCK_STARTS.get(2) - DataFileConstants.SYNC_SIZE + 1,
                                Arrays.asList(new BatchInfo(3, 5), new BatchInfo(5, 6)))));
    }

    @Test
    void testRestoreReader() throws IOException {
        AvroBulkFormatTestUtils.TestingAvroBulkFormat bulkFormat =
                new AvroBulkFormatTestUtils.TestingAvroBulkFormat();
        long splitLength = tmpFile.length() / 3;
        String splitId = UUID.randomUUID().toString();

        FileSourceSplit split =
                new FileSourceSplit(
                        splitId, new Path(tmpFile.toString()), splitLength * 2, tmpFile.length());
        BulkFormat.Reader<RowData> reader = bulkFormat.createReader(new Configuration(), split);
        long offset1 = assertBatch(reader, new BatchInfo(3, 5));
        assertBatch(reader, new BatchInfo(5, 6));
        assertThat(reader.readBatch()).isNull();
        reader.close();

        split =
                new FileSourceSplit(
                        splitId,
                        new Path(tmpFile.toString()),
                        splitLength * 2,
                        tmpFile.length(),
                        StringUtils.EMPTY_STRING_ARRAY,
                        new CheckpointedPosition(offset1, 1));
        reader = bulkFormat.restoreReader(new Configuration(), split);
        long offset2 = assertBatch(reader, new BatchInfo(3, 5), 1);
        assertBatch(reader, new BatchInfo(5, 6));
        assertThat(reader.readBatch()).isNull();
        reader.close();

        assertThat(offset2).isEqualTo(offset1);
    }

    private void assertSplit(
            AvroBulkFormatTestUtils.TestingAvroBulkFormat bulkFormat, List<SplitInfo> splitInfos)
            throws IOException {
        for (SplitInfo splitInfo : splitInfos) {
            FileSourceSplit split =
                    new FileSourceSplit(
                            UUID.randomUUID().toString(),
                            new Path(tmpFile.toString()),
                            splitInfo.start,
                            splitInfo.end - splitInfo.start);
            BulkFormat.Reader<RowData> reader = bulkFormat.createReader(new Configuration(), split);
            List<Long> offsets = new ArrayList<>();
            for (BatchInfo batch : splitInfo.batches) {
                offsets.add(assertBatch(reader, batch));
            }
            assertThat(reader.readBatch()).isNull();
            for (int j = 1; j < offsets.size(); j++) {
                assertThat(offsets.get(j - 1) < offsets.get(j)).isTrue();
            }
            reader.close();
        }
    }

    private long assertBatch(BulkFormat.Reader<RowData> reader, BatchInfo batchInfo)
            throws IOException {
        return assertBatch(reader, batchInfo, 0);
    }

    private long assertBatch(
            BulkFormat.Reader<RowData> reader, BatchInfo batchInfo, int initialSkipCount)
            throws IOException {
        long ret = -1;
        int skipCount = initialSkipCount;
        BulkFormat.RecordIterator<RowData> iterator = reader.readBatch();
        for (RecordAndPosition<RowData> recordAndPos = iterator.next();
                recordAndPos != null;
                recordAndPos = iterator.next()) {
            if (ret == -1) {
                ret = recordAndPos.getOffset();
            }
            assertThat(recordAndPos.getRecord())
                    .isEqualTo(TEST_DATA.get(batchInfo.start + skipCount));
            assertThat(recordAndPos.getOffset()).isEqualTo(ret);
            skipCount++;
            assertThat(recordAndPos.getRecordSkipCount()).isEqualTo(skipCount);
        }
        assertThat(skipCount).isEqualTo(batchInfo.end - batchInfo.start);
        iterator.releaseBatch();
        return ret;
    }

    private static class SplitInfo {
        private final long start;
        private final long end;
        private final List<BatchInfo> batches;

        private SplitInfo(long start, long end, List<BatchInfo> batches) {
            this.start = start;
            this.end = end;
            this.batches = batches;
        }
    }

    private static class BatchInfo {
        private final int start;
        private final int end;

        private BatchInfo(int start, int end) {
            this.start = start;
            this.end = end;
        }
    }
}
