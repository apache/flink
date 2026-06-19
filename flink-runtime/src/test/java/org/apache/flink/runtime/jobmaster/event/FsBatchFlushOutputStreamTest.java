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

package org.apache.flink.runtime.jobmaster.event;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.testutils.junit.utils.TempDirUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link FsBatchFlushOutputStream}. */
class FsBatchFlushOutputStreamTest {

    @TempDir private java.nio.file.Path temporaryFolder;
    private static final int BUFFER_SIZE = 10000;

    @Test
    void testBatchFlush() throws IOException {
        final Path workingDir = new Path(TempDirUtils.newFolder(temporaryFolder).getAbsolutePath());
        final Path file = new Path(workingDir, "test-file");

        TestingFsBatchFlushOutputStream outputStream =
                new TestingFsBatchFlushOutputStream(
                        file.getFileSystem(), file, FileSystem.WriteMode.NO_OVERWRITE, BUFFER_SIZE);

        int dataSize = BUFFER_SIZE / 10;
        for (int i = 0; i < 9; ++i) {
            outputStream.write(new byte[dataSize]);
            assertThat(outputStream.flushCount).isZero();
        }
        outputStream.write(new byte[dataSize]);
        assertThat(outputStream.flushCount).isEqualTo(1);

        for (int i = 0; i < 9; ++i) {
            outputStream.write(new byte[dataSize]);
            assertThat(outputStream.flushCount).isEqualTo(1);
        }
        outputStream.write(new byte[dataSize - 1]);
        assertThat(outputStream.flushCount).isEqualTo(1);

        outputStream.write(new byte[1]);
        assertThat(outputStream.flushCount).isEqualTo(2);
    }

    @Test
    void testWriteLargerThanBufferSize() throws IOException {
        final Path workingDir = new Path(TempDirUtils.newFolder(temporaryFolder).getAbsolutePath());
        final Path file = new Path(workingDir, "test-file");

        TestingFsBatchFlushOutputStream outputStream =
                new TestingFsBatchFlushOutputStream(
                        file.getFileSystem(), file, FileSystem.WriteMode.NO_OVERWRITE, BUFFER_SIZE);

        assertThat(outputStream.flushCount).isZero();
        outputStream.write(new byte[BUFFER_SIZE + 1]);
        assertThat(outputStream.flushCount).isEqualTo(1);
    }

    private static class TestingFsBatchFlushOutputStream extends FsBatchFlushOutputStream {

        int flushCount = 0;

        TestingFsBatchFlushOutputStream(
                FileSystem fileSystem,
                Path filePath,
                FileSystem.WriteMode overwriteMode,
                int bufferSize)
                throws IOException {
            super(fileSystem, filePath, overwriteMode, bufferSize);
        }

        @Override
        public void flush() throws IOException {
            super.flush();
            flushCount++;
        }
    }
}
