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

package org.apache.flink.formats.parquet.utils;

import org.apache.flink.api.java.tuple.Tuple2;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.compat.FilterCompat.Filter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.io.api.RecordMaterializer.RecordMaterializationException;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.CheckReturnValue;
import javax.annotation.meta.When;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Customized {@link org.apache.parquet.hadoop.ParquetRecordReader} that support start read from
 * particular position.
 */
public class ParquetRecordReader<T> {
    private static final Logger LOG = LoggerFactory.getLogger(ParquetRecordReader.class);

    private ColumnIOFactory columnIOFactory;
    private Filter filter;
    private MessageType readSchema;
    private MessageType fileSchema;
    private ReadSupport<T> readSupport;

    private RecordMaterializer<T> recordMaterializer;
    private ParquetFileReader reader;
    private RecordReader<T> recordReader;
    private boolean skipCorruptedRecord = true;

    private T readRecord;
    private boolean readRecordReturned = true;

    // number of records in file
    private long numTotalRecords;
    // number of records that were read from file
    private long numReadRecords = 0;

    // id of current block
    private int currentBlock = -1;
    private long numRecordsUpToPreviousBlock = 0;
    private long numRecordsUpToCurrentBlock = 0;

    public ParquetRecordReader(ReadSupport<T> readSupport, MessageType readSchema, Filter filter) {
        this.filter = checkNotNull(filter, "readSupport");
        this.readSupport = checkNotNull(readSupport, "readSchema");
        this.readSchema = checkNotNull(readSchema, "filter");
    }

    public ParquetRecordReader(ReadSupport<T> readSupport, MessageType readSchema) {
        this(readSupport, readSchema, FilterCompat.NOOP);
    }

    public void setSkipCorruptedRecord(boolean skipCorruptedRecord) {
        this.skipCorruptedRecord = skipCorruptedRecord;
    }

    public void initialize(ParquetFileReader reader, Configuration configuration) {
        this.reader = reader;
        FileMetaData parquetFileMetadata = reader.getFooter().getFileMetaData();
        // real schema of parquet file
        this.fileSchema = parquetFileMetadata.getSchema();
        Map<String, String> fileMetadata = parquetFileMetadata.getKeyValueMetaData();
        ReadSupport.ReadContext readContext =
                readSupport.init(
                        new InitContext(configuration, toSetMultiMap(fileMetadata), readSchema));

        this.columnIOFactory = new ColumnIOFactory(parquetFileMetadata.getCreatedBy());
        this.recordMaterializer =
                readSupport.prepareForRead(configuration, fileMetadata, readSchema, readContext);
        this.numTotalRecords = reader.getRecordCount();
    }

    private RecordReader<T> createRecordReader(PageReadStore pages) throws IOException {
        if (pages == null) {
            throw new IOException(
                    "Expecting more rows but reached last block. Read "
                            + numReadRecords
                            + " out of "
                            + numTotalRecords);
        }
        MessageColumnIO columnIO = columnIOFactory.getColumnIO(readSchema, fileSchema, true);
        return columnIO.getRecordReader(pages, recordMaterializer, filter);
    }

    /**
     * Moves the reading position to the given block and seeks to and reads the given record.
     *
     * @param block The block to seek to.
     * @param recordInBlock The number of the record in the block to return next.
     */
    public void seek(long block, long recordInBlock) throws IOException {

        List<BlockMetaData> blockMetaData = reader.getRowGroups();

        if (block == -1L && recordInBlock == -1L) {
            // the split was fully consumed
            currentBlock = blockMetaData.size() - 1;
            numReadRecords = numTotalRecords;
            numRecordsUpToCurrentBlock = numTotalRecords;
            return;
        }

        // init all counters for the start of the first block
        currentBlock = 0;
        numRecordsUpToPreviousBlock = 0;
        numRecordsUpToCurrentBlock = blockMetaData.get(0).getRowCount();
        numReadRecords = 0;

        // seek to the given block
        while (currentBlock < block) {
            currentBlock++;
            reader.skipNextRowGroup();
            numRecordsUpToPreviousBlock = numRecordsUpToCurrentBlock;
            numRecordsUpToCurrentBlock += blockMetaData.get(currentBlock).getRowCount();
            numReadRecords = numRecordsUpToPreviousBlock;
        }

        // seek to and read the given record
        PageReadStore pages = reader.readNextRowGroup();
        recordReader = createRecordReader(pages);
        for (int i = 0; i <= recordInBlock; i++) {
            readNextRecord();
        }
    }

    /**
     * Returns the current read position in the split, i.e., the current block and the number of
     * records that were returned from that block.
     *
     * @return The current read position in the split.
     */
    public Tuple2<Long, Long> getCurrentReadPosition() {

        // compute number of returned records
        long numRecordsReturned = numReadRecords;
        if (!readRecordReturned && numReadRecords > 0) {
            numRecordsReturned -= 1;
        }

        if (numRecordsReturned == numTotalRecords) {
            // all records of split returned.
            return Tuple2.of(-1L, -1L);
        }

        if (numRecordsReturned == numRecordsUpToCurrentBlock) {
            // all records of block returned. Next record is in next block
            return Tuple2.of(currentBlock + 1L, 0L);
        }

        // compute number of returned records of this block
        long numRecordsOfBlockReturned = numRecordsReturned - numRecordsUpToPreviousBlock;
        return Tuple2.of((long) currentBlock, numRecordsOfBlockReturned);
    }

    /**
     * Checks if the record reader returned all records. This method must be called before a record
     * can be returned.
     *
     * @return False if there are more records to be read. True if all records have been returned.
     */
    public boolean reachEnd() throws IOException {
        // check if we have a read row that was not returned yet
        if (readRecord != null && !readRecordReturned) {
            return false;
        }
        // check if there are more rows to be read
        if (numReadRecords >= numTotalRecords) {
            return true;
        }
        // try to read next row
        return !readNextRecord();
    }

    /**
     * Reads the next record.
     *
     * @return True if a record could be read, false otherwise.
     */
    private boolean readNextRecord() throws IOException {
        boolean recordFound = false;
        while (!recordFound) {
            // no more records left
            if (numReadRecords >= numTotalRecords) {
                return false;
            }

            try {
                if (numReadRecords == numRecordsUpToCurrentBlock) {
                    // advance to next block
                    PageReadStore pages = reader.readNextRowGroup();
                    recordReader = createRecordReader(pages);
                    numRecordsUpToPreviousBlock = numRecordsUpToCurrentBlock;
                    numRecordsUpToCurrentBlock += pages.getRowCount();
                    currentBlock++;
                }

                numReadRecords++;
                try {
                    readRecord = recordReader.read();
                    readRecordReturned = false;
                } catch (RecordMaterializationException e) {
                    String errorMessage =
                            String.format(
                                    "skipping a corrupt record in block number [%d] record number [%s] of file %s",
                                    currentBlock,
                                    numReadRecords - numRecordsUpToPreviousBlock,
                                    reader.getFile());

                    if (!skipCorruptedRecord) {
                        LOG.error(errorMessage);
                        throw e;
                    } else {
                        LOG.warn(errorMessage);
                    }
                    continue;
                }

                if (readRecord == null) {
                    readRecordReturned = true;
                    numReadRecords = numRecordsUpToCurrentBlock;
                    LOG.debug("filtered record reader reached end of block");
                    continue;
                }

                recordFound = true;
                LOG.debug("read value: {}", readRecord);
            } catch (RecordMaterializationException e) {
                LOG.error(
                        String.format(
                                "Can not read value at %d in block %d in file %s",
                                numReadRecords - numRecordsUpToPreviousBlock,
                                currentBlock,
                                reader.getFile()),
                        e);
                if (!skipCorruptedRecord) {
                    throw e;
                }
                return false;
            }
        }

        return true;
    }

    /**
     * Returns the next record. Note that the reachedEnd() method must be called before.
     *
     * @return The next record.
     */
    @CheckReturnValue(when = When.NEVER)
    public T nextRecord() {
        readRecordReturned = true;
        return readRecord;
    }

    public void close() throws IOException {
        if (reader != null) {
            reader.close();
        }
    }

    private static <K, V> Map<K, Set<V>> toSetMultiMap(Map<K, V> map) {
        Map<K, Set<V>> setMultiMap = new HashMap<>();
        for (Map.Entry<K, V> entry : map.entrySet()) {
            Set<V> set = Collections.singleton(entry.getValue());
            setMultiMap.put(entry.getKey(), Collections.unmodifiableSet(set));
        }
        return Collections.unmodifiableMap(setMultiMap);
    }
}
