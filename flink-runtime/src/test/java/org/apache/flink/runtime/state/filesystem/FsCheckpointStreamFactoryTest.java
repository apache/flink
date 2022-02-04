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

import org.apache.flink.core.fs.EntropyInjectingFileSystem;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.local.LocalFileSystem;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.StreamStateHandle;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Unit tests for the {@link FsCheckpointStreamFactory}. */
public class FsCheckpointStreamFactoryTest {

    @TempDir private Path exclusiveStateDir;
    @TempDir private Path sharedStateDir;

    // ------------------------------------------------------------------------
    //  tests
    // ------------------------------------------------------------------------

    @Test
    @SuppressWarnings("ConstantConditions")
    public void testWriteFlushesIfAboveThreshold() throws IOException {
        int fileSizeThreshold = 100;
        final FsCheckpointStreamFactory factory =
                createFactory(
                        FileSystem.getLocalFileSystem(), fileSizeThreshold, fileSizeThreshold);
        final FsCheckpointStreamFactory.FsCheckpointStateOutputStream stream =
                factory.createCheckpointStateOutputStream(CheckpointedStateScope.EXCLUSIVE);
        stream.write(new byte[fileSizeThreshold]);
        File[] files = new File(exclusiveStateDir.toUri()).listFiles();
        assertEquals(1, files.length);
        File file = files[0];
        assertEquals(fileSizeThreshold, file.length());
        stream.write(new byte[fileSizeThreshold - 1]); // should buffer without flushing
        stream.write(127); // should buffer without flushing
        assertEquals(fileSizeThreshold, file.length());
    }

    @Test
    public void testExclusiveStateHasRelativePathHandles() throws IOException {
        final FsCheckpointStreamFactory factory = createFactory(FileSystem.getLocalFileSystem(), 0);

        final FsCheckpointStreamFactory.FsCheckpointStateOutputStream stream =
                factory.createCheckpointStateOutputStream(CheckpointedStateScope.EXCLUSIVE);
        stream.write(1657);
        final StreamStateHandle handle = stream.closeAndGetHandle();

        assertThat(handle, instanceOf(RelativeFileStateHandle.class));
        assertPathsEqual(
                exclusiveStateDir, ((RelativeFileStateHandle) handle).getFilePath().getParent());
    }

    @Test
    public void testSharedStateHasAbsolutePathHandles() throws IOException {
        final FsCheckpointStreamFactory factory = createFactory(FileSystem.getLocalFileSystem(), 0);

        final FsCheckpointStreamFactory.FsCheckpointStateOutputStream stream =
                factory.createCheckpointStateOutputStream(CheckpointedStateScope.SHARED);
        stream.write(0);
        final StreamStateHandle handle = stream.closeAndGetHandle();

        assertThat(handle, instanceOf(FileStateHandle.class));
        assertThat(handle, not(instanceOf(RelativeFileStateHandle.class)));
        assertPathsEqual(sharedStateDir, ((FileStateHandle) handle).getFilePath().getParent());
    }

    @Test
    public void testEntropyMakesExclusiveStateAbsolutePaths() throws IOException {
        final FsCheckpointStreamFactory factory =
                createFactory(new FsStateBackendEntropyTest.TestEntropyAwareFs(), 0);

        final FsCheckpointStreamFactory.FsCheckpointStateOutputStream stream =
                factory.createCheckpointStateOutputStream(CheckpointedStateScope.EXCLUSIVE);
        stream.write(0);
        final StreamStateHandle handle = stream.closeAndGetHandle();

        assertThat(handle, instanceOf(FileStateHandle.class));
        assertThat(handle, not(instanceOf(RelativeFileStateHandle.class)));
        assertPathsEqual(exclusiveStateDir, ((FileStateHandle) handle).getFilePath().getParent());
    }

    @Test
    public void testFSWithDisabledEntropyHasRelativePaths() throws IOException {
        final FsCheckpointStreamFactory factory = createFactory(new DisabledEntropyFS(), 0);

        final FsCheckpointStreamFactory.FsCheckpointStateOutputStream stream =
                factory.createCheckpointStateOutputStream(CheckpointedStateScope.EXCLUSIVE);
        stream.write(0);
        final StreamStateHandle handle = stream.closeAndGetHandle();

        assertThat(handle, instanceOf(RelativeFileStateHandle.class));
        assertPathsEqual(
                exclusiveStateDir, ((RelativeFileStateHandle) handle).getFilePath().getParent());
    }

    @Test
    public void testFlushUnderThreshold() throws IOException {
        flushAndVerify(10, 10, true);
    }

    @Test
    public void testFlushAboveThreshold() throws IOException {
        flushAndVerify(10, 11, false);
    }

    private void flushAndVerify(int minFileSize, int bytesToFlush, boolean expectEmpty)
            throws IOException {
        FsCheckpointStreamFactory.FsCheckpointStateOutputStream stream =
                createFactory(new FsStateBackendEntropyTest.TestEntropyAwareFs(), minFileSize)
                        .createCheckpointStateOutputStream(CheckpointedStateScope.EXCLUSIVE);

        stream.write(new byte[bytesToFlush], 0, bytesToFlush);
        stream.flush();
        assertEquals(expectEmpty ? 0 : 1, new File(exclusiveStateDir.toUri()).listFiles().length);
    }

    // ------------------------------------------------------------------------
    //  test utils
    // ------------------------------------------------------------------------

    private static void assertPathsEqual(Path expected, org.apache.flink.core.fs.Path actual) {
        final org.apache.flink.core.fs.Path reNormalizedExpected =
                new org.apache.flink.core.fs.Path(
                        new org.apache.flink.core.fs.Path(expected.toUri()).toString());
        assertEquals(reNormalizedExpected, actual);
    }

    private FsCheckpointStreamFactory createFactory(FileSystem fs, int fileSizeThreshold) {
        return createFactory(fs, fileSizeThreshold, 4096);
    }

    private FsCheckpointStreamFactory createFactory(
            FileSystem fs, int fileSizeThreshold, int bufferSize) {
        return new FsCheckpointStreamFactory(
                fs,
                new org.apache.flink.core.fs.Path(exclusiveStateDir.toUri()),
                new org.apache.flink.core.fs.Path(sharedStateDir.toUri()),
                fileSizeThreshold,
                bufferSize);
    }

    private static final class DisabledEntropyFS extends LocalFileSystem
            implements EntropyInjectingFileSystem {
        @Override
        public String getEntropyInjectionKey() {
            return null;
        }

        @Override
        public String generateEntropy() {
            return null;
        }
    }
}
