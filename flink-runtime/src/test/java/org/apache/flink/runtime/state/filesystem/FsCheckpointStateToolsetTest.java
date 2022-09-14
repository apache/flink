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

import org.apache.flink.core.fs.DuplicatingFileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FsCheckpointStateToolset}. */
class FsCheckpointStateToolsetTest {
    @Test
    void testCanDuplicateNonFileStreamHandle() throws IOException {
        final FsCheckpointStateToolset stateToolset =
                new FsCheckpointStateToolset(
                        new Path("test-path"), new TestDuplicatingFileSystem());

        final boolean canFastDuplicate =
                stateToolset.canFastDuplicate(new ByteStreamStateHandle("test", new byte[] {}));
        assertThat(canFastDuplicate).isFalse();
    }

    @Test
    void testCanDuplicate() throws IOException {
        final FsCheckpointStateToolset stateToolset =
                new FsCheckpointStateToolset(
                        new Path("test-path"), new TestDuplicatingFileSystem());

        final boolean canFastDuplicate =
                stateToolset.canFastDuplicate(
                        new FileStateHandle(new Path("old-test-path", "test-file"), 0));
        assertThat(canFastDuplicate).isTrue();
    }

    @Test
    void testCannotDuplicate() throws IOException {
        final FsCheckpointStateToolset stateToolset =
                new FsCheckpointStateToolset(
                        new Path("test-path"), new TestDuplicatingFileSystem());

        final boolean canFastDuplicate =
                stateToolset.canFastDuplicate(
                        new FileStateHandle(new Path("test-path", "test-file"), 0));
        assertThat(canFastDuplicate).isFalse();
    }

    @Test
    void testDuplicating() throws IOException {
        final TestDuplicatingFileSystem fs = new TestDuplicatingFileSystem();
        final FsCheckpointStateToolset stateToolset =
                new FsCheckpointStateToolset(new Path("test-path"), fs);

        final List<StreamStateHandle> duplicated =
                stateToolset.duplicate(
                        Arrays.asList(
                                new FileStateHandle(new Path("old-test-path", "test-file1"), 0),
                                new FileStateHandle(new Path("old-test-path", "test-file2"), 0),
                                new RelativeFileStateHandle(
                                        new Path("old-test-path", "test-file3"), "test-file3", 0)));

        assertThat(duplicated)
                .containsExactly(
                        new FileStateHandle(new Path("test-path", "test-file1"), 0),
                        new FileStateHandle(new Path("test-path", "test-file2"), 0),
                        new RelativeFileStateHandle(
                                new Path("test-path", "test-file3"), "test-file3", 0));
    }

    private static final class TestDuplicatingFileSystem implements DuplicatingFileSystem {

        @Override
        public boolean canFastDuplicate(Path source, Path destination) throws IOException {
            return !source.equals(destination);
        }

        @Override
        public void duplicate(List<CopyRequest> requests) throws IOException {}
    }
}
