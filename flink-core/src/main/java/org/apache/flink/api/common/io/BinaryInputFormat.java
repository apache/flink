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

package org.apache.flink.api.common.io;

import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.BlockLocation;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FilterInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Base class for all input formats that use blocks of fixed size. The input splits are aligned to
 * these blocks, meaning that each split will consist of one block. Without configuration, these
 * block sizes equal the native block sizes of the HDFS.
 *
 * <p>A block will contain a {@link BlockInfo} at the end of the block. There, the reader can find
 * some statistics about the split currently being read, that will help correctly parse the contents
 * of the block.
 */
@Public
public abstract class BinaryInputFormat<T> extends FileInputFormat<T>
        implements CheckpointableInputFormat<FileInputSplit, Tuple2<Long, Long>> {

    private static final long serialVersionUID = 1L;

    /** The log. */
    private static final Logger LOG = LoggerFactory.getLogger(BinaryInputFormat.class);

    /** The config parameter which defines the fixed length of a record. */
    public static final String BLOCK_SIZE_PARAMETER_KEY = "input.block_size";

    public static final long NATIVE_BLOCK_SIZE = Long.MIN_VALUE;

    /** The block size to use. */
    private long blockSize = NATIVE_BLOCK_SIZE;

    private transient DataInputViewStreamWrapper dataInputStream;

    /** The BlockInfo for the Block corresponding to the split currently being read. */
    private transient BlockInfo blockInfo;

    /** A wrapper around the block currently being read. */
    private transient BlockBasedInput blockBasedInput = null;

    /**
     * The number of records already read from the block. This is used to decide if the end of the
     * block has been reached.
     */
    private long readRecords = 0;

    @Override
    public void configure(Configuration parameters) {
        super.configure(parameters);

        // the if is to prevent the configure() method from
        // overwriting the value set by the setter

        if (this.blockSize == NATIVE_BLOCK_SIZE) {
            long blockSize = parameters.getLong(BLOCK_SIZE_PARAMETER_KEY, NATIVE_BLOCK_SIZE);
            setBlockSize(blockSize);
        }
    }

    public void setBlockSize(long blockSize) {
        if (blockSize < 1 && blockSize != NATIVE_BLOCK_SIZE) {
            throw new IllegalArgumentException(
                    "The block size parameter must be set and larger than 0.");
        }
        if (blockSize > Integer.MAX_VALUE) {
            throw new UnsupportedOperationException(
                    "Currently only block sizes up to Integer.MAX_VALUE are supported");
        }
        this.blockSize = blockSize;
    }

    public long getBlockSize() {
        return this.blockSize;
    }

    @Override
    public FileInputSplit[] createInputSplits(int minNumSplits) throws IOException {
        final List<FileStatus> files = this.getFiles();

        final List<FileInputSplit> inputSplits = new ArrayList<FileInputSplit>(minNumSplits);
        for (FileStatus file : files) {
            final FileSystem fs = file.getPath().getFileSystem();
            final long blockSize =
                    this.blockSize == NATIVE_BLOCK_SIZE ? fs.getDefaultBlockSize() : this.blockSize;

            for (long pos = 0, length = file.getLen(); pos < length; pos += blockSize) {
                long remainingLength = Math.min(pos + blockSize, length) - pos;

                // get the block locations and make sure they are in order with respect to their
                // offset
                final BlockLocation[] blocks = fs.getFileBlockLocations(file, pos, remainingLength);
                Arrays.sort(blocks);

                inputSplits.add(
                        new FileInputSplit(
                                inputSplits.size(),
                                file.getPath(),
                                pos,
                                remainingLength,
                                blocks[0].getHosts()));
            }
        }

        if (inputSplits.size() < minNumSplits) {
            LOG.warn(
                    String.format(
                            "With the given block size %d, the files %s cannot be split into %d blocks. Filling up with empty splits...",
                            blockSize, Arrays.toString(getFilePaths()), minNumSplits));
            FileStatus last = files.get(files.size() - 1);
            final BlockLocation[] blocks =
                    last.getPath().getFileSystem().getFileBlockLocations(last, 0, last.getLen());
            for (int index = files.size(); index < minNumSplits; index++) {
                inputSplits.add(
                        new FileInputSplit(
                                index, last.getPath(), last.getLen(), 0, blocks[0].getHosts()));
            }
        }

        return inputSplits.toArray(new FileInputSplit[inputSplits.size()]);
    }

    protected List<FileStatus> getFiles() throws IOException {
        // get all the files that are involved in the splits
        List<FileStatus> files = new ArrayList<>();

        for (Path filePath : getFilePaths()) {
            final FileSystem fs = filePath.getFileSystem();
            final FileStatus pathFile = fs.getFileStatus(filePath);

            if (pathFile.isDir()) {
                // input is directory. list all contained files
                final FileStatus[] partials = fs.listStatus(filePath);
                for (FileStatus partial : partials) {
                    if (!partial.isDir()) {
                        files.add(partial);
                    }
                }
            } else {
                files.add(pathFile);
            }
        }
        return files;
    }

    @Override
    public SequentialStatistics getStatistics(BaseStatistics cachedStats) {

        final FileBaseStatistics cachedFileStats =
                cachedStats instanceof FileBaseStatistics ? (FileBaseStatistics) cachedStats : null;

        try {
            final ArrayList<FileStatus> allFiles = new ArrayList<FileStatus>(1);
            final FileBaseStatistics stats =
                    getFileStats(cachedFileStats, getFilePaths(), allFiles);
            if (stats == null) {
                return null;
            }
            // check whether the file stats are still sequential stats (in that case they are still
            // valid)
            if (stats instanceof SequentialStatistics) {
                return (SequentialStatistics) stats;
            }
            return createStatistics(allFiles, stats);
        } catch (IOException ioex) {
            if (LOG.isWarnEnabled()) {
                LOG.warn(
                        String.format(
                                "Could not determine complete statistics for files '%s' due to an I/O error",
                                Arrays.toString(getFilePaths())),
                        ioex);
            }
        } catch (Throwable t) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        String.format(
                                "Unexpected problem while getting the file statistics for files '%s'",
                                Arrays.toString(getFilePaths())),
                        t);
            }
        }
        // no stats available
        return null;
    }

    protected FileInputSplit[] getInputSplits() throws IOException {
        return this.createInputSplits(0);
    }

    public BlockInfo createBlockInfo() {
        return new BlockInfo();
    }

    private BlockInfo createAndReadBlockInfo() throws IOException {
        BlockInfo blockInfo = new BlockInfo();
        if (this.splitLength > blockInfo.getInfoSize()) {
            // At first we go and read  the block info containing the recordCount, the
            // accumulatedRecordCount
            // and the firstRecordStart offset in the current block. This is written at the end of
            // the block and
            // is of fixed size, currently 3 * Long.SIZE.

            // TODO: seek not supported by compressed streams. Will throw exception
            this.stream.seek(this.splitStart + this.splitLength - blockInfo.getInfoSize());
            blockInfo.read(new DataInputViewStreamWrapper(this.stream));
        }
        return blockInfo;
    }

    /**
     * Fill in the statistics. The last modification time and the total input size are prefilled.
     *
     * @param files The files that are associated with this block input format.
     * @param stats The pre-filled statistics.
     */
    protected SequentialStatistics createStatistics(
            List<FileStatus> files, FileBaseStatistics stats) throws IOException {
        if (files.isEmpty()) {
            return null;
        }

        BlockInfo blockInfo = new BlockInfo();
        long totalCount = 0;
        for (FileStatus file : files) {
            // invalid file
            if (file.getLen() < blockInfo.getInfoSize()) {
                continue;
            }

            FileSystem fs = file.getPath().getFileSystem();
            try (FSDataInputStream fdis = fs.open(file.getPath(), blockInfo.getInfoSize())) {
                fdis.seek(file.getLen() - blockInfo.getInfoSize());

                blockInfo.read(new DataInputViewStreamWrapper(fdis));
                totalCount += blockInfo.getAccumulatedRecordCount();
            }
        }

        final float avgWidth =
                totalCount == 0 ? 0 : ((float) stats.getTotalInputSize() / totalCount);
        return new SequentialStatistics(
                stats.getLastModificationTime(), stats.getTotalInputSize(), avgWidth, totalCount);
    }

    private static class SequentialStatistics extends FileBaseStatistics {

        private final long numberOfRecords;

        public SequentialStatistics(
                long fileModTime, long fileSize, float avgBytesPerRecord, long numberOfRecords) {
            super(fileModTime, fileSize, avgBytesPerRecord);
            this.numberOfRecords = numberOfRecords;
        }

        @Override
        public long getNumberOfRecords() {
            return this.numberOfRecords;
        }
    }

    @Override
    public void open(FileInputSplit split) throws IOException {
        super.open(split);

        this.blockInfo = this.createAndReadBlockInfo();

        // We set the size of the BlockBasedInput to splitLength as each split contains one block.
        // After reading the block info, we seek in the file to the correct position.

        this.readRecords = 0;
        this.stream.seek(this.splitStart + this.blockInfo.getFirstRecordStart());
        this.blockBasedInput =
                new BlockBasedInput(
                        this.stream, (int) blockInfo.getFirstRecordStart(), this.splitLength);
        this.dataInputStream = new DataInputViewStreamWrapper(blockBasedInput);
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return this.readRecords >= this.blockInfo.getRecordCount();
    }

    @Override
    public T nextRecord(T record) throws IOException {
        if (this.reachedEnd()) {
            return null;
        }
        record = this.deserialize(record, this.dataInputStream);
        this.readRecords++;
        return record;
    }

    protected abstract T deserialize(T reuse, DataInputView dataInput) throws IOException;

    /**
     * Reads the content of a block of data. The block contains its {@link BlockInfo} at the end,
     * and this method takes this into account when reading the data.
     */
    protected class BlockBasedInput extends FilterInputStream {
        private final int maxPayloadSize;

        private int blockPos;

        public BlockBasedInput(FSDataInputStream in, int blockSize) {
            super(in);
            this.blockPos = (int) BinaryInputFormat.this.blockInfo.getFirstRecordStart();
            this.maxPayloadSize = blockSize - BinaryInputFormat.this.blockInfo.getInfoSize();
        }

        public BlockBasedInput(FSDataInputStream in, int startPos, long length) {
            super(in);
            this.blockPos = startPos;
            this.maxPayloadSize = (int) (length - BinaryInputFormat.this.blockInfo.getInfoSize());
        }

        @Override
        public int read() throws IOException {
            if (this.blockPos++ >= this.maxPayloadSize) {
                this.skipHeader();
            }
            return this.in.read();
        }

        private long getCurrBlockPos() {
            return this.blockPos;
        }

        private void skipHeader() throws IOException {
            byte[] dummy = new byte[BinaryInputFormat.this.blockInfo.getInfoSize()];
            this.in.read(dummy, 0, dummy.length);

            // the blockPos is set to 0 for the case of remote reads,
            // these are the cases where the last record of a block spills on the next block
            this.blockPos = 0;
        }

        @Override
        public int read(byte[] b) throws IOException {
            return this.read(b, 0, b.length);
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            int totalRead = 0;
            for (int remainingLength = len, offset = off; remainingLength > 0; ) {
                int blockLen = Math.min(remainingLength, this.maxPayloadSize - this.blockPos);
                int read = this.in.read(b, offset, blockLen);
                if (read < 0) {
                    return read;
                }
                totalRead += read;
                this.blockPos += read;
                offset += read;
                if (this.blockPos >= this.maxPayloadSize) {
                    this.skipHeader();
                }
                remainingLength -= read;
            }
            return totalRead;
        }
    }

    // --------------------------------------------------------------------------------------------
    //  Checkpointing
    // --------------------------------------------------------------------------------------------

    @PublicEvolving
    @Override
    public Tuple2<Long, Long> getCurrentState() throws IOException {
        if (this.blockBasedInput == null) {
            throw new RuntimeException(
                    "You must have forgotten to call open() on your input format.");
        }

        return new Tuple2<>(
                this.blockBasedInput.getCurrBlockPos(), // the last read index in the block
                this.readRecords // the number of records read
                );
    }

    @PublicEvolving
    @Override
    public void reopen(FileInputSplit split, Tuple2<Long, Long> state) throws IOException {
        Preconditions.checkNotNull(split, "reopen() cannot be called on a null split.");
        Preconditions.checkNotNull(state, "reopen() cannot be called with a null initial state.");

        try {
            this.open(split);
        } finally {
            this.blockInfo = this.createAndReadBlockInfo();

            long blockPos = state.f0;
            this.readRecords = state.f1;

            this.stream.seek(this.splitStart + blockPos);
            this.blockBasedInput =
                    new BlockBasedInput(this.stream, (int) blockPos, this.splitLength);
            this.dataInputStream = new DataInputViewStreamWrapper(blockBasedInput);
        }
    }
}
