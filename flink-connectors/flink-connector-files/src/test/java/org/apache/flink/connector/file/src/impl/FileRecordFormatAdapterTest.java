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

package org.apache.flink.connector.file.src.impl;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.src.reader.FileRecordFormat;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/** Unit and behavior tests for the {@link FileRecordFormatAdapter}. */
@SuppressWarnings("serial")
public class FileRecordFormatAdapterTest extends AdapterTestBase<FileRecordFormat<Integer>> {

    @Override
    protected FileRecordFormat<Integer> createCheckpointedFormat() {
        return new IntFileRecordFormat(true);
    }

    @Override
    protected FileRecordFormat<Integer> createNonCheckpointedFormat() {
        return new IntFileRecordFormat(false);
    }

    @Override
    protected FileRecordFormat<Integer> createFormatFailingInInstantiation() {
        return new FailingInstantiationFormat();
    }

    @Override
    protected BulkFormat<Integer, FileSourceSplit> wrapWithAdapter(
            FileRecordFormat<Integer> format) {
        return new FileRecordFormatAdapter<>(format);
    }

    // ------------------------------------------------------------------------
    //  not applicable tests
    // ------------------------------------------------------------------------

    @Override
    public void testClosesStreamIfReaderCreationFails() throws Exception {
        // ignore this test
    }

    // ------------------------------------------------------------------------
    //  test mocks
    // ------------------------------------------------------------------------

    private static final class IntFileRecordFormat implements FileRecordFormat<Integer> {

        private final boolean checkpointed;

        IntFileRecordFormat(boolean checkpointed) {
            this.checkpointed = checkpointed;
        }

        @Override
        public Reader<Integer> createReader(
                Configuration config, Path filePath, long splitOffset, long splitLength)
                throws IOException {

            final FileSystem fs = filePath.getFileSystem();
            final FileStatus status = fs.getFileStatus(filePath);
            final FSDataInputStream in = fs.open(filePath);

            final long fileLen = status.getLen();
            final long splitEnd = splitOffset + splitLength;

            assertEquals("invalid file length", 0, fileLen % 4);

            // round all positions to the next integer boundary
            // to simulate common split behavior, we round up to the next int boundary even when we
            // are at a perfect boundary. exceptions are if we are start or end.
            final long start = splitOffset == 0L ? 0L : splitOffset + 4 - splitOffset % 4;
            final long end = splitEnd == fileLen ? fileLen : splitEnd + 4 - splitEnd % 4;
            in.seek(start);

            return new TestIntReader(in, end, checkpointed);
        }

        @Override
        public Reader<Integer> restoreReader(
                Configuration config,
                Path filePath,
                long restoredOffset,
                long splitOffset,
                long splitLength)
                throws IOException {

            final FileSystem fs = filePath.getFileSystem();
            final FileStatus status = fs.getFileStatus(filePath);
            final FSDataInputStream in = fs.open(filePath);

            final long fileLen = status.getLen();
            final long splitEnd = splitOffset + splitLength;

            assertEquals("invalid file length", 0, fileLen % 4);

            // round end position to the next integer boundary
            final long end = splitEnd == fileLen ? fileLen : splitEnd + 4 - splitEnd % 4;
            // no rounding of checkpointed offset
            in.seek(restoredOffset);
            return new TestIntReader(in, end, checkpointed);
        }

        @Override
        public boolean isSplittable() {
            return true;
        }

        @Override
        public TypeInformation<Integer> getProducedType() {
            return Types.INT;
        }
    }

    private static final class FailingInstantiationFormat implements FileRecordFormat<Integer> {

        @Override
        public Reader<Integer> createReader(
                Configuration config, Path filePath, long splitOffset, long splitLength)
                throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public Reader<Integer> restoreReader(
                Configuration config,
                Path filePath,
                long restoredOffset,
                long splitOffset,
                long splitLength)
                throws IOException {

            final FSDataInputStream in = filePath.getFileSystem().open(filePath);
            return new FailingReader(in);
        }

        @Override
        public boolean isSplittable() {
            return false;
        }

        @Override
        public TypeInformation<Integer> getProducedType() {
            return Types.INT;
        }

        private static final class FailingReader implements FileRecordFormat.Reader<Integer> {

            private final FSDataInputStream stream;

            FailingReader(FSDataInputStream stream) {
                this.stream = stream;
            }

            @Nullable
            @Override
            public Integer read() throws IOException {
                throw new IOException("test exception");
            }

            @Override
            public void close() throws IOException {
                stream.close();
            }
        }
    }
}
