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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.util.IOUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link PartitionedFile} is the persistent file type of sort-merge based blocking shuffle. Each
 * {@link PartitionedFile} contains two physical files: one is the data file and the other is the
 * index file. Both the data file and the index file have multiple regions. Data belonging to the
 * same subpartition are stored together in each data region and the corresponding index region
 * contains index entries of all subpartitions. Each index entry is a (long, integer) value tuple of
 * which the long value represents the file offset of the target subpartition and the integer value
 * is the number of buffers.
 */
public class PartitionedFile {

    public static final String DATA_FILE_SUFFIX = ".shuffle.data";

    public static final String INDEX_FILE_SUFFIX = ".shuffle.index";

    /**
     * Size of each index entry in the index file: 8 bytes for file offset and 4 bytes for number of
     * buffers.
     */
    public static final int INDEX_ENTRY_SIZE = 8 + 4;

    /** Number of data regions in this {@link PartitionedFile}. */
    private final int numRegions;

    /** Number of subpartitions of this {@link PartitionedFile}. */
    private final int numSubpartitions;

    /** Path of the data file which stores all data in this {@link PartitionedFile}. */
    private final Path dataFilePath;

    /** Path of the index file which stores all index entries in this {@link PartitionedFile}. */
    private final Path indexFilePath;

    /** Used to accelerate index data access. */
    @Nullable private final ByteBuffer indexEntryCache;

    public PartitionedFile(
            int numRegions,
            int numSubpartitions,
            Path dataFilePath,
            Path indexFilePath,
            @Nullable ByteBuffer indexEntryCache) {
        checkArgument(numRegions >= 0, "Illegal number of data regions.");
        checkArgument(numSubpartitions > 0, "Illegal number of subpartitions.");

        this.numRegions = numRegions;
        this.numSubpartitions = numSubpartitions;
        this.dataFilePath = checkNotNull(dataFilePath);
        this.indexFilePath = checkNotNull(indexFilePath);
        this.indexEntryCache = indexEntryCache;
    }

    public Path getDataFilePath() {
        return dataFilePath;
    }

    public Path getIndexFilePath() {
        return indexFilePath;
    }

    public int getNumRegions() {
        return numRegions;
    }

    /**
     * Returns the index entry offset of the target region and subpartition in the index file. Both
     * region index and subpartition index start from 0.
     */
    private long getIndexEntryOffset(int region, int subpartition) {
        checkArgument(region >= 0 && region < getNumRegions(), "Illegal target region.");
        checkArgument(
                subpartition >= 0 && subpartition < numSubpartitions,
                "Subpartition index out of bound.");

        return (((long) region) * numSubpartitions + subpartition) * INDEX_ENTRY_SIZE;
    }

    /**
     * Gets the index entry of the target region and subpartition either from the index data cache
     * or the index data file.
     */
    void getIndexEntry(FileChannel indexFile, ByteBuffer target, int region, int subpartition)
            throws IOException {
        checkArgument(target.capacity() == INDEX_ENTRY_SIZE, "Illegal target buffer size.");

        target.clear();
        long indexEntryOffset = getIndexEntryOffset(region, subpartition);
        if (indexEntryCache != null) {
            for (int i = 0; i < INDEX_ENTRY_SIZE; ++i) {
                target.put(indexEntryCache.get((int) indexEntryOffset + i));
            }
        } else {
            indexFile.position(indexEntryOffset);
            BufferReaderWriterUtil.readByteBufferFully(indexFile, target);
        }
        target.flip();
    }

    public void deleteQuietly() {
        IOUtils.deleteFileQuietly(dataFilePath);
        IOUtils.deleteFileQuietly(indexFilePath);
    }

    @Override
    public String toString() {
        return "PartitionedFile{"
                + "numRegions="
                + numRegions
                + ", numSubpartitions="
                + numSubpartitions
                + ", dataFilePath="
                + dataFilePath
                + ", indexFilePath="
                + indexFilePath
                + ", indexDataCache="
                + indexEntryCache
                + '}';
    }
}
