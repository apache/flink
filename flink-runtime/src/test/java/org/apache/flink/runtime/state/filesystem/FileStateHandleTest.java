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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemFactory;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.local.LocalFileSystem;
import org.apache.flink.core.plugin.TestingPluginManager;
import org.apache.flink.util.TestLoggerExtension;
import org.apache.flink.util.function.BiFunctionWithException;
import org.apache.flink.util.function.FunctionWithException;
import org.apache.flink.util.function.RunnableWithException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

/** Tests for the {@link FileStateHandle}. */
@ExtendWith(TestLoggerExtension.class)
public class FileStateHandleTest {

    private static final String TEST_SCHEME = "test";

    private static Path resolve(String... segments) {
        return new Path(TEST_SCHEME + "://" + String.join("/", segments));
    }

    /**
     * Tests that the file state handle does not attempt to check and clean up the parent directory.
     * Doing directory contents checks and cleaning up the parent directory in the state handle
     * disposal has previously led to excessive file system metadata requests, which especially on
     * systems like Amazon S3 is prohibitively expensive.
     */
    @Test
    public void testDisposeDoesNotDeleteParentDirectory() throws Exception {
        final Path p = resolve("path", "with", "parent");
        final List<Path> pathsToDelete = new ArrayList<>();

        initializeFileSystem(
                MockedLocalFileSystem.newBuilder()
                        .setDeleteFunction(
                                (path, ignoredRecursionMarker) -> {
                                    pathsToDelete.add(path);
                                    return true;
                                })
                        .build());

        final FileStateHandle handle = new FileStateHandle(p, 42);
        handle.discardState();
        assertThat(pathsToDelete)
                .as(
                        "Only one delete call should have happened on the actual path but not the parent.")
                .singleElement()
                .isEqualTo(p);
    }

    @Test
    public void testDiscardStateForNonExistingFileWithoutAnErrorBeingExposedByTheFileSystem()
            throws Exception {
        testDiscardStateForNonExistingFile(
                MockedLocalFileSystem.newBuilder()
                        // deletion call was successful
                        .setDeleteFunction((ignoredPath, ignoredRecursionMarker) -> true)
                        .setExistsFunction(
                                path ->
                                        fail(
                                                "The exists call should not have been triggered. This call "
                                                        + "should be avoided because it might be quite "
                                                        + "expensive in object stores."))
                        .build());
    }

    @Test
    public void testDiscardStateForNonExistingFileWithDeleteCallReturningFalse() throws Exception {
        testDiscardStateForNonExistingFile(
                MockedLocalFileSystem.newBuilder()
                        .setDeleteFunction((ignoredPath, ignoredRecursionMarker) -> false)
                        .setExistsFunction(path -> false)
                        .build());
    }

    @Test
    public void testDiscardStateForNonExistingFileWithEDeleteCallFailing() throws Exception {
        testDiscardStateForNonExistingFile(
                MockedLocalFileSystem.newBuilder()
                        .setDeleteFunction(
                                (ignoredPath, ignoredRecursionMarker) -> {
                                    throw new IOException(
                                            "Expected IOException caused by FileSystem.delete.");
                                })
                        .setExistsFunction(path -> false)
                        .build());
    }

    private void testDiscardStateForNonExistingFile(FileSystem fileSystem) throws Exception {
        runInFileSystemContext(
                fileSystem,
                () -> {
                    final FileStateHandle handle =
                            new FileStateHandle(resolve("path", "to", "state"), 0);
                    // should not fail
                    handle.discardState();
                });
    }

    @Test
    public void testDiscardStateWithDeletionFailureThroughException() throws Exception {
        testDiscardStateFailed(
                MockedLocalFileSystem.newBuilder()
                        .setDeleteFunction(
                                (ignoredPath, ignoredRecursionMarker) -> {
                                    throw new IOException(
                                            "Expected IOException to simulate IO error.");
                                })
                        .setExistsFunction(path -> true)
                        .build());
    }

    @Test
    public void testDiscardStateWithDeletionFailureThroughReturnValue() throws Exception {
        testDiscardStateFailed(
                MockedLocalFileSystem.newBuilder()
                        .setDeleteFunction((ignoredPath, ignoredRecursionMarker) -> false)
                        .setExistsFunction(path -> true)
                        .build());
    }

    private static void testDiscardStateFailed(FileSystem fileSystem) throws Exception {
        runInFileSystemContext(
                fileSystem,
                () -> {
                    final FileStateHandle handle =
                            new FileStateHandle(resolve("path", "to", "state"), 0);
                    assertThrows(IOException.class, handle::discardState);
                });
    }

    private static void runInFileSystemContext(
            FileSystem fileSystem, RunnableWithException testCallback) throws Exception {
        initializeFileSystem(fileSystem);

        try {
            testCallback.run();
        } finally {
            FileSystem.initialize(new Configuration(), null);
        }
    }

    private static void initializeFileSystem(FileSystem fileSystem) {
        final Map<Class<?>, Iterator<?>> fileSystemPlugins = new HashMap<>();
        fileSystemPlugins.put(
                FileSystemFactory.class,
                Collections.singletonList(new TestingFileSystemFactory(TEST_SCHEME, fileSystem))
                        .iterator());

        FileSystem.initialize(new Configuration(), new TestingPluginManager(fileSystemPlugins));
    }

    private static class MockedLocalFileSystem extends LocalFileSystem {

        private final BiFunctionWithException<Path, Boolean, Boolean, IOException> deleteFunction;
        private final FunctionWithException<Path, Boolean, IOException> existsFunction;

        public MockedLocalFileSystem(
                BiFunctionWithException<Path, Boolean, Boolean, IOException> deleteFunction,
                FunctionWithException<Path, Boolean, IOException> existsFunction) {
            this.deleteFunction = deleteFunction;
            this.existsFunction = existsFunction;
        }

        @Override
        public boolean delete(Path f, boolean recursive) throws IOException {
            return deleteFunction.apply(f, recursive);
        }

        @Override
        public boolean exists(Path f) throws IOException {
            return existsFunction.apply(f);
        }

        public static Builder newBuilder() {
            return new Builder();
        }

        private static class Builder {

            private BiFunctionWithException<Path, Boolean, Boolean, IOException> deleteFunction =
                    (ignoredPath, ignoredRecursionMarker) -> true;
            private FunctionWithException<Path, Boolean, IOException> existsFunction =
                    ignoredPath -> true;

            public Builder setDeleteFunction(
                    BiFunctionWithException<Path, Boolean, Boolean, IOException> deleteFunction) {
                this.deleteFunction = deleteFunction;
                return this;
            }

            public Builder setExistsFunction(
                    FunctionWithException<Path, Boolean, IOException> existsFunction) {
                this.existsFunction = existsFunction;
                return this;
            }

            public MockedLocalFileSystem build() {
                return new MockedLocalFileSystem(deleteFunction, existsFunction);
            }
        }
    }

    private static class TestingFileSystemFactory implements FileSystemFactory {

        private final String scheme;
        private final FileSystem fileSystem;

        public TestingFileSystemFactory(String scheme, FileSystem fileSystem) {
            this.scheme = scheme;
            this.fileSystem = fileSystem;
        }

        @Override
        public String getScheme() {
            return scheme;
        }

        @Override
        public FileSystem create(URI fsUri) throws IOException {
            return fileSystem;
        }
    }
}
