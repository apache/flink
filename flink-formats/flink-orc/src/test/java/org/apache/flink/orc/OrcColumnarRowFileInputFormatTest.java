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

import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.src.util.CheckpointedPosition;
import org.apache.flink.connector.file.src.util.RecordAndPosition;
import org.apache.flink.connector.file.src.util.Utils;
import org.apache.flink.core.fs.Path;
import org.apache.flink.orc.OrcFilters.Between;
import org.apache.flink.orc.OrcFilters.Equals;
import org.apache.flink.orc.OrcFilters.Or;
import org.apache.flink.orc.OrcFilters.Predicate;
import org.apache.flink.orc.shim.OrcShim;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.DecimalDataUtils;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.filesystem.PartitionFieldExtractor;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.IOUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static org.apache.flink.table.utils.PartitionPathUtils.generatePartitionPath;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/** Test for {@link OrcColumnarRowFileInputFormat}. */
public class OrcColumnarRowFileInputFormatTest {

    /** Small batch size for test more boundary conditions. */
    protected static final int BATCH_SIZE = 9;

    private static final RowType FLAT_FILE_TYPE =
            RowType.of(
                    new LogicalType[] {
                        DataTypes.INT().getLogicalType(),
                        DataTypes.STRING().getLogicalType(),
                        DataTypes.STRING().getLogicalType(),
                        DataTypes.STRING().getLogicalType(),
                        DataTypes.INT().getLogicalType(),
                        DataTypes.STRING().getLogicalType(),
                        DataTypes.INT().getLogicalType(),
                        DataTypes.INT().getLogicalType(),
                        DataTypes.INT().getLogicalType()
                    },
                    new String[] {
                        "_col0", "_col1", "_col2", "_col3", "_col4", "_col5", "_col6", "_col7",
                        "_col8"
                    });

    private static final RowType DECIMAL_FILE_TYPE =
            RowType.of(new LogicalType[] {new DecimalType(10, 5)}, new String[] {"_col0"});

    private final Path flatFile = copyFileFromResource("test-data-flat.orc");

    private final Path decimalFile = copyFileFromResource("test-data-decimal.orc");

    @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    @Test
    public void testReadFileInSplits() throws IOException {
        OrcColumnarRowFileInputFormat<?, FileSourceSplit> format =
                createFormat(FLAT_FILE_TYPE, new int[] {0, 1});

        AtomicInteger cnt = new AtomicInteger(0);
        AtomicLong totalF0 = new AtomicLong(0);

        // read all splits
        for (FileSourceSplit split : createSplits(flatFile, 4)) {
            forEach(
                    format,
                    split,
                    row -> {
                        Assert.assertFalse(row.isNullAt(0));
                        Assert.assertFalse(row.isNullAt(1));
                        totalF0.addAndGet(row.getInt(0));
                        Assert.assertNotNull(row.getString(1).toString());
                        cnt.incrementAndGet();
                    });
        }

        // check that all rows have been read
        assertEquals(1920800, cnt.get());
        assertEquals(1844737280400L, totalF0.get());
    }

    @Test
    public void testReadFileWithSelectFields() throws IOException {
        OrcColumnarRowFileInputFormat<?, FileSourceSplit> format =
                createFormat(FLAT_FILE_TYPE, new int[] {2, 0, 1});

        AtomicInteger cnt = new AtomicInteger(0);
        AtomicLong totalF0 = new AtomicLong(0);

        // read all splits
        for (FileSourceSplit split : createSplits(flatFile, 4)) {
            forEach(
                    format,
                    split,
                    row -> {
                        Assert.assertFalse(row.isNullAt(0));
                        Assert.assertFalse(row.isNullAt(1));
                        Assert.assertFalse(row.isNullAt(2));
                        Assert.assertNotNull(row.getString(0).toString());
                        totalF0.addAndGet(row.getInt(1));
                        Assert.assertNotNull(row.getString(2).toString());
                        cnt.incrementAndGet();
                    });
        }

        // check that all rows have been read
        assertEquals(1920800, cnt.get());
        assertEquals(1844737280400L, totalF0.get());
    }

    @Test
    public void testReadDecimalTypeFile() throws IOException {
        OrcColumnarRowFileInputFormat<?, FileSourceSplit> format =
                createFormat(DECIMAL_FILE_TYPE, new int[] {0});

        AtomicInteger cnt = new AtomicInteger(0);
        AtomicInteger nullCount = new AtomicInteger(0);

        // read all splits
        for (FileSourceSplit split : createSplits(decimalFile, 4)) {
            forEach(
                    format,
                    split,
                    row -> {
                        if (cnt.get() == 0) {
                            // validate first row
                            assertNotNull(row);
                            assertEquals(1, row.getArity());
                            assertEquals(
                                    DecimalDataUtils.castFrom(-1000.5d, 10, 5),
                                    row.getDecimal(0, 10, 5));
                        } else {
                            if (!row.isNullAt(0)) {
                                assertNotNull(row.getDecimal(0, 10, 5));
                            } else {
                                nullCount.incrementAndGet();
                            }
                        }
                        cnt.incrementAndGet();
                    });
        }

        assertEquals(6000, cnt.get());
        assertEquals(2000, nullCount.get());
    }

    @Test
    public void testReadFileWithPartitionFields() throws IOException {
        LinkedHashMap<String, String> partSpec = new LinkedHashMap<>();
        partSpec.put("f1", "1");
        partSpec.put("f3", "3");
        partSpec.put("f5", "f5");
        partSpec.put("f8", BigDecimal.valueOf(5.333).toString());
        partSpec.put("f13", "f13");

        final Path flatFile = copyFileFromResource("test-data-flat.orc", partSpec);

        RowType tableType =
                RowType.of(
                        /* 0 */ DataTypes.INT().getLogicalType(),
                        /* 1 */ DataTypes.INT().getLogicalType(), // part-1
                        /* 2 */ DataTypes.STRING().getLogicalType(),
                        /* 3 */ DataTypes.BIGINT().getLogicalType(), // part-2
                        /* 4 */ DataTypes.STRING().getLogicalType(),
                        /* 5 */ DataTypes.STRING().getLogicalType(), // part-3
                        /* 6 */ DataTypes.STRING().getLogicalType(),
                        /* 7 */ DataTypes.INT().getLogicalType(),
                        /* 8 */ DataTypes.DECIMAL(10, 5).getLogicalType(), // part-4
                        /* 9 */ DataTypes.STRING().getLogicalType(),
                        /* 11*/ DataTypes.INT().getLogicalType(),
                        /* 12*/ DataTypes.INT().getLogicalType(),
                        /* 13*/ DataTypes.STRING().getLogicalType(), // part-5
                        /* 14*/ DataTypes.INT().getLogicalType());

        int[] projectedFields = {8, 1, 3, 0, 5, 2};

        OrcColumnarRowFileInputFormat<?, FileSourceSplit> format =
                createPartitionFormat(
                        tableType, new ArrayList<>(partSpec.keySet()), projectedFields);

        AtomicInteger cnt = new AtomicInteger(0);
        AtomicLong totalF0 = new AtomicLong(0);

        // read all splits
        for (FileSourceSplit split : createSplits(flatFile, 4)) {
            forEach(
                    format,
                    split,
                    row -> {
                        // data values
                        Assert.assertFalse(row.isNullAt(3));
                        Assert.assertFalse(row.isNullAt(5));
                        totalF0.addAndGet(row.getInt(3));
                        Assert.assertNotNull(row.getString(5).toString());

                        // part values
                        Assert.assertFalse(row.isNullAt(0));
                        Assert.assertFalse(row.isNullAt(1));
                        Assert.assertFalse(row.isNullAt(2));
                        Assert.assertFalse(row.isNullAt(4));
                        Assert.assertEquals(
                                DecimalDataUtils.castFrom(5.333, 10, 5), row.getDecimal(0, 10, 5));
                        Assert.assertEquals(1, row.getInt(1));
                        Assert.assertEquals(3, row.getLong(2));
                        Assert.assertEquals("f5", row.getString(4).toString());
                        cnt.incrementAndGet();
                    });
        }

        // check that all rows have been read
        assertEquals(1920800, cnt.get());
        assertEquals(1844737280400L, totalF0.get());
    }

    @Test
    public void testReadFileAndRestore() throws IOException {
        OrcColumnarRowFileInputFormat<?, FileSourceSplit> format =
                createFormat(FLAT_FILE_TYPE, new int[] {0, 1});

        // pick a middle split
        FileSourceSplit split = createSplits(flatFile, 3).get(1);

        int expectedCnt = 660000;

        innerTestRestore(format, split, expectedCnt / 2, expectedCnt, 656700330000L);
    }

    @Test
    public void testReadFileAndRestoreWithFilter() throws IOException {
        List<Predicate> filter =
                Collections.singletonList(
                        new Or(
                                new Between("_col0", PredicateLeaf.Type.LONG, 0L, 975000L),
                                new Equals("_col0", PredicateLeaf.Type.LONG, 980001L),
                                new Between("_col0", PredicateLeaf.Type.LONG, 990000L, 1800000L)));
        OrcColumnarRowFileInputFormat<?, FileSourceSplit> format =
                createFormat(FLAT_FILE_TYPE, new int[] {0, 1}, filter);

        // pick a middle split
        FileSourceSplit split = createSplits(flatFile, 1).get(0);

        int breakCnt = 975001;
        int expectedCnt = 1795000;
        long expectedTotalF0 = 1615113397500L;

        innerTestRestore(format, split, breakCnt, expectedCnt, expectedTotalF0);
    }

    private void innerTestRestore(
            OrcColumnarRowFileInputFormat<?, FileSourceSplit> format,
            FileSourceSplit split,
            int breakCnt,
            int expectedCnt,
            long expectedTotalF0)
            throws IOException {
        AtomicInteger cnt = new AtomicInteger(0);
        AtomicLong totalF0 = new AtomicLong(0);

        Consumer<RowData> consumer =
                row -> {
                    Assert.assertFalse(row.isNullAt(0));
                    Assert.assertFalse(row.isNullAt(1));
                    totalF0.addAndGet(row.getInt(0));
                    assertNotNull(row.getString(1).toString());
                    cnt.incrementAndGet();
                };

        // ---------- restore reading ---------------

        long offset = -1;
        long recordSkipCount = -1;
        try (BulkFormat.Reader<RowData> reader = createReader(format, split)) {
            while (cnt.get() < breakCnt) {
                BulkFormat.RecordIterator<RowData> batch = reader.readBatch();
                Assert.assertNotNull(batch);

                RecordAndPosition<RowData> record;
                while ((record = batch.next()) != null && cnt.get() < breakCnt) {
                    consumer.accept(record.getRecord());
                    offset = record.getOffset();
                    recordSkipCount = record.getRecordSkipCount();
                }
                batch.releaseBatch();
            }
        }

        Utils.forEachRemaining(restoreReader(format, split, offset, recordSkipCount), consumer);

        // ---------- end restore reading ---------------
        // the results should be the same as:
        // forEach(format, split, consumer);

        // check that all rows have been read
        assertEquals(expectedCnt, cnt.get());
        assertEquals(expectedTotalF0, totalF0.get());
    }

    protected OrcColumnarRowFileInputFormat<?, FileSourceSplit> createFormat(
            RowType formatType, int[] selectedFields) {
        return createFormat(formatType, selectedFields, new ArrayList<>());
    }

    protected OrcColumnarRowFileInputFormat<?, FileSourceSplit> createFormat(
            RowType formatType, int[] selectedFields, List<Predicate> conjunctPredicates) {
        return OrcColumnarRowFileInputFormat.createPartitionedFormat(
                OrcShim.defaultShim(),
                new Configuration(),
                formatType,
                new ArrayList<>(),
                PartitionFieldExtractor.forFileSystem(""),
                selectedFields,
                conjunctPredicates,
                BATCH_SIZE);
    }

    protected OrcColumnarRowFileInputFormat<?, FileSourceSplit> createPartitionFormat(
            RowType tableType, List<String> partitionKeys, int[] selectedFields) {
        return OrcColumnarRowFileInputFormat.createPartitionedFormat(
                OrcShim.defaultShim(),
                new Configuration(),
                tableType,
                partitionKeys,
                PartitionFieldExtractor.forFileSystem(""),
                selectedFields,
                new ArrayList<>(),
                BATCH_SIZE);
    }

    private BulkFormat.Reader<RowData> createReader(
            OrcColumnarRowFileInputFormat<?, FileSourceSplit> format, FileSourceSplit split)
            throws IOException {
        return format.createReader(new org.apache.flink.configuration.Configuration(), split);
    }

    private BulkFormat.Reader<RowData> restoreReader(
            OrcColumnarRowFileInputFormat<?, FileSourceSplit> format,
            FileSourceSplit split,
            long offset,
            long recordSkipCount)
            throws IOException {
        FileSourceSplit restoreSplit =
                split.updateWithCheckpointedPosition(
                        new CheckpointedPosition(offset, recordSkipCount));
        return format.restoreReader(
                new org.apache.flink.configuration.Configuration(), restoreSplit);
    }

    private void forEach(
            OrcColumnarRowFileInputFormat<?, FileSourceSplit> format,
            FileSourceSplit split,
            Consumer<RowData> action)
            throws IOException {
        Utils.forEachRemaining(createReader(format, split), action);
    }

    private Path copyFileFromResource(String fileName) {
        try {
            File file = TEMPORARY_FOLDER.newFile();
            return copyFileFromResource(fileName, file);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Path copyFileFromResource(String fileName, LinkedHashMap<String, String> partSpec) {
        try {
            File folder = TEMPORARY_FOLDER.newFolder();
            folder = new File(folder, generatePartitionPath(partSpec));
            return copyFileFromResource(fileName, new File(folder, UUID.randomUUID().toString()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Path copyFileFromResource(String fileName, File file) {
        try {
            file.getParentFile().mkdirs();
            file.delete();
            file.createNewFile();
            IOUtils.copyBytes(
                    getClass().getClassLoader().getResource(fileName).openStream(),
                    new FileOutputStream(file),
                    true);
            return new Path(file.getPath());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static List<FileSourceSplit> createSplits(Path path, int minNumSplits)
            throws IOException {
        List<FileSourceSplit> splits = new ArrayList<>(minNumSplits);
        long len = path.getFileSystem().getFileStatus(path).getLen();
        long preferSplitSize = len / minNumSplits + (len % minNumSplits == 0 ? 0 : 1);
        int splitNum = 0;
        long position = 0;
        while (position < len) {
            long splitLen = Math.min(preferSplitSize, len - position);
            splits.add(new FileSourceSplit(String.valueOf(splitNum++), path, position, splitLen));
            position += splitLen;
        }
        return splits;
    }
}
