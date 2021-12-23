/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.blob;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.local.LocalDataOutputStream;
import org.apache.flink.runtime.state.filesystem.TestFs;
import org.apache.flink.util.TestLoggerExtension;
import org.apache.flink.util.function.FunctionWithException;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link FileSystemBlobStore}. */
@ExtendWith(TestLoggerExtension.class)
class FileSystemBlobStoreTest {

    @Test
    public void fileSystemBlobStoreCallsSyncOnPut(@TempDir Path storageDirectory)
            throws IOException {
        final Path blobStoreDirectory = storageDirectory.resolve("blobStore");

        final AtomicReference<TestingLocalDataOutputStream> createdOutputStream =
                new AtomicReference<>();
        final FunctionWithException<org.apache.flink.core.fs.Path, FSDataOutputStream, IOException>
                outputStreamFactory =
                        value -> {
                            final File file = new File(value.toString());
                            FileUtils.createParentDirectories(file);
                            final TestingLocalDataOutputStream outputStream =
                                    new TestingLocalDataOutputStream(file);
                            createdOutputStream.compareAndSet(null, outputStream);
                            return outputStream;
                        };
        try (FileSystemBlobStore fileSystemBlobStore =
                new FileSystemBlobStore(
                        new TestFs(outputStreamFactory), blobStoreDirectory.toString())) {
            final BlobKey blobKey = BlobKey.createKey(BlobKey.BlobType.PERMANENT_BLOB);
            final File localFile = storageDirectory.resolve("localFile").toFile();
            FileUtils.createParentDirectories(localFile);
            FileUtils.writeStringToFile(localFile, "foobar", StandardCharsets.UTF_8);

            fileSystemBlobStore.put(localFile, new JobID(), blobKey);

            assertThat(createdOutputStream.get().hasSyncBeenCalled()).isTrue();
        }
    }

    private static class TestingLocalDataOutputStream extends LocalDataOutputStream {

        private boolean hasSyncBeenCalled = false;

        private TestingLocalDataOutputStream(File file) throws IOException {
            super(file);
        }

        @Override
        public void sync() throws IOException {
            hasSyncBeenCalled = true;
            super.sync();
        }

        public boolean hasSyncBeenCalled() {
            return hasSyncBeenCalled;
        }
    }
}
