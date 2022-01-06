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

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.src.util.CheckpointedPosition;
import org.apache.flink.connector.file.src.util.IteratorResultIterator;
import org.apache.flink.connector.file.src.util.Pool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.utils.FSDataInputStreamWrapper;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Iterator;

/** Provides a {@link BulkFormat} for Avro records. */
@Internal
public abstract class AbstractAvroBulkFormat<A, T, SplitT extends FileSourceSplit>
        implements BulkFormat<T, SplitT> {

    private static final long serialVersionUID = 1L;

    protected final Schema readerSchema;

    protected AbstractAvroBulkFormat(Schema readerSchema) {
        this.readerSchema = readerSchema;
    }

    @Override
    public AvroReader createReader(Configuration config, SplitT split) throws IOException {
        open(split);
        return createReader(split);
    }

    @Override
    public AvroReader restoreReader(Configuration config, SplitT split) throws IOException {
        open(split);
        return createReader(split);
    }

    @Override
    public boolean isSplittable() {
        return true;
    }

    private AvroReader createReader(SplitT split) throws IOException {
        long end = split.offset() + split.length();
        if (split.getReaderPosition().isPresent()) {
            CheckpointedPosition position = split.getReaderPosition().get();
            return new AvroReader(
                    split.path(),
                    split.offset(),
                    end,
                    position.getOffset(),
                    position.getRecordsAfterOffset());
        } else {
            return new AvroReader(split.path(), split.offset(), end, -1, 0);
        }
    }

    protected void open(SplitT split) {}

    protected abstract T convert(A record);

    protected abstract A createReusedAvroRecord();

    private class AvroReader implements BulkFormat.Reader<T> {

        private final DataFileReader<A> reader;

        private final long end;
        private final Pool<A> pool;

        private long currentBlockStart;
        private long currentRecordsToSkip;

        private AvroReader(Path path, long offset, long end, long blockStart, long recordsToSkip)
                throws IOException {
            A reuse = createReusedAvroRecord();

            this.reader = createReaderFromPath(path);
            if (blockStart >= 0) {
                reader.seek(blockStart);
            } else {
                reader.sync(offset);
            }
            for (int i = 0; i < recordsToSkip; i++) {
                reader.next(reuse);
            }

            this.end = end;
            this.pool = new Pool<>(1);
            this.pool.add(reuse);

            this.currentBlockStart = reader.previousSync();
            this.currentRecordsToSkip = recordsToSkip;
        }

        private DataFileReader<A> createReaderFromPath(Path path) throws IOException {
            FileSystem fileSystem = path.getFileSystem();
            DatumReader<A> datumReader = new GenericDatumReader<>(null, readerSchema);
            SeekableInput in =
                    new FSDataInputStreamWrapper(
                            fileSystem.open(path), fileSystem.getFileStatus(path).getLen());
            return (DataFileReader<A>) DataFileReader.openReader(in, datumReader);
        }

        @Nullable
        @Override
        public RecordIterator<T> readBatch() throws IOException {
            A reuse;
            try {
                reuse = pool.pollEntry();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(
                        "Interrupted while waiting for the previous batch to be consumed", e);
            }

            if (!readNextBlock()) {
                pool.recycler().recycle(reuse);
                return null;
            }

            currentBlockStart = reader.previousSync();
            Iterator<T> iterator =
                    new AvroBlockIterator(
                            reader.getBlockCount() - currentRecordsToSkip, reader, reuse);
            long recordsToSkip = currentRecordsToSkip;
            currentRecordsToSkip = 0;
            return new IteratorResultIterator<>(
                    iterator,
                    currentBlockStart,
                    recordsToSkip,
                    () -> pool.recycler().recycle(reuse));
        }

        private boolean readNextBlock() throws IOException {
            // read the next block with reader,
            // returns true if a block is read and false if we reach the end of this split
            return reader.hasNext() && !reader.pastSync(end);
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }
    }

    private class AvroBlockIterator implements Iterator<T> {

        private long numRecordsRemaining;
        private final DataFileReader<A> reader;
        private final A reuse;

        private AvroBlockIterator(long numRecordsRemaining, DataFileReader<A> reader, A reuse) {
            this.numRecordsRemaining = numRecordsRemaining;
            this.reader = reader;
            this.reuse = reuse;
        }

        @Override
        public boolean hasNext() {
            return numRecordsRemaining > 0;
        }

        @Override
        public T next() {
            try {
                numRecordsRemaining--;
                // reader.next merely deserialize bytes in memory to java objects
                // and will not read from file
                return convert(reader.next(reuse));
            } catch (IOException e) {
                throw new RuntimeException(
                        "Encountered exception when reading from avro format file", e);
            }
        }
    }
}
