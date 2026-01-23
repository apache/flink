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

package org.apache.flink.runtime.state.filesystem;

import org.apache.flink.core.fs.BlockLocation;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.fs.hdfs.HadoopDataOutputStream;
import org.apache.flink.runtime.state.CheckpointedStateScope;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Verifies the checkpoint workflow calls sync(), and that sync maps to hflush (not hsync)
 * when using HadoopDataOutputStream.
 */
class CheckpointSyncToHadoopFlushWorkflowTest {

    @TempDir private java.nio.file.Path tmpDir;

    @Test
    void testCheckpointFinalizeTriggersHflushViaHadoopDataOutputStream() throws Exception {
        // Mock the underlying Hadoop stream so we can verify hflush vs hsync
        org.apache.hadoop.fs.FSDataOutputStream hadoopOut =
                mock(org.apache.hadoop.fs.FSDataOutputStream.class);

        AtomicInteger flinkSyncCalls = new AtomicInteger();

        FileSystem testFs = new HadoopStreamBackedFileSystem(flinkSyncCalls, hadoopOut);

        Path exclusive = new Path(new File(tmpDir.toFile(), "exclusive").toURI());
        Path shared = new Path(new File(tmpDir.toFile(), "shared").toURI());

        testFs.mkdirs(exclusive);
        testFs.mkdirs(shared);

        FsCheckpointStreamFactory factory =
                new FsCheckpointStreamFactory(
                        testFs, exclusive, shared, /*fileSizeThreshold*/ 0, /*bufferSize*/ 4096);

        FsCheckpointStreamFactory.FsCheckpointStateOutputStream out =
                factory.createCheckpointStateOutputStream(CheckpointedStateScope.EXCLUSIVE);

        out.write(new byte[1234]);
        out.closeAndGetHandle();

        // 1) Workflow-level: checkpoint finalization called sync()
        assertThat(flinkSyncCalls.get())
                .as("Checkpoint finalize should call FSDataOutputStream.sync() at least once")
                .isGreaterThan(0);

        // 2) Mapping-level: HadoopDataOutputStream.sync() invoked hflush() and NOT hsync()
        verify(hadoopOut, times(1)).hflush();
        verify(hadoopOut, never()).hsync();
    }

    /**
     * Test-only FileSystem: its create() returns a HadoopDataOutputStream wrapping a mock
     * org.apache.hadoop.fs.FSDataOutputStream, so we can assert hflush/hsync calls.
     */
    private static final class HadoopStreamBackedFileSystem extends FileSystem {
        private final AtomicInteger syncCalls;
        private final org.apache.hadoop.fs.FSDataOutputStream hadoopOut;

        HadoopStreamBackedFileSystem(
                AtomicInteger syncCalls, org.apache.hadoop.fs.FSDataOutputStream hadoopOut) {
            this.syncCalls = syncCalls;
            this.hadoopOut = hadoopOut;
        }

        @Override
        public URI getUri() {
            return URI.create("test://hadoop-stream-backed-fs/");
        }

        @Override
        public FSDataOutputStream create(Path filePath, WriteMode overwriteMode) {
            // Wrap the mock Hadoop output stream with Flink's HadoopDataOutputStream.
            // We additionally count how many times Flink-level sync() was invoked.
            return new CountingSyncFSDataOutputStream(new HadoopDataOutputStream(hadoopOut), syncCalls);
        }

        // ---- Minimal stubs required by FileSystem; not used by this test ----

        @Override
        public boolean delete(Path f, boolean recursive) {
            return true;
        }

        @Override
        public boolean mkdirs(Path f) {
            return true;
        }

        @Override
        public FileStatus[] listStatus(Path f) {
            return new FileStatus[0];
        }

        @Override
        public boolean rename(Path src, Path dst) {
            return true;
        }

        @Override
        public boolean exists(Path f) {
            return true;
        }

        @Override
        public FileStatus getFileStatus(Path f) {
            throw new UnsupportedOperationException();
        }

        @Override
        public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len) {
            return new BlockLocation[0];
        }

        @Override
        public FSDataInputStream open(Path f, int bufferSize) {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Decorator around a Flink FSDataOutputStream that counts sync() calls but delegates behavior.
     */
    private static final class CountingSyncFSDataOutputStream extends FSDataOutputStream {
        private final FSDataOutputStream delegate;
        private final AtomicInteger syncCalls;

        // Buffer just to satisfy write() even though FsCheckpointStreamFactory may buffer internally too.
        private final ByteArrayOutputStream buf = new ByteArrayOutputStream();

        CountingSyncFSDataOutputStream(FSDataOutputStream delegate, AtomicInteger syncCalls) {
            this.delegate = delegate;
            this.syncCalls = syncCalls;
        }

        @Override
        public long getPos() throws IOException {
            // delegate.getPos() may call into mocked Hadoop methods; keep local pos deterministic.
            return buf.size();
        }

        @Override
        public void write(int b) throws IOException {
            buf.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            buf.write(b, off, len);
        }

        @Override
        public void flush() throws IOException {
            delegate.flush();
        }

        @Override
        public void sync() throws IOException {
            syncCalls.incrementAndGet();
            delegate.sync();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }
}
