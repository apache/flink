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

package org.apache.flink.changelog.fs;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemFactory;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.local.LocalFileSystem;
import org.apache.flink.core.plugin.TestingPluginManager;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.apache.flink.util.IOUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.UUID;

import static java.util.Collections.singletonMap;
import static org.apache.flink.changelog.fs.FsStateChangelogOptions.CACHE_IDLE_TIMEOUT;

/** {@link ChangelogStreamHandleReaderWithCache} test. */
class ChangelogStreamHandleReaderWithCacheTest {

    @TempDir java.nio.file.Path tempFolder;

    @Test
    void testCloseStreamTwice() throws Exception {
        String tempFolderPath = tempFolder.toUri().getPath();

        registerFileSystem(
                new LocalFileSystem() {
                    @Override
                    public boolean isDistributedFS() {
                        return true;
                    }
                },
                tempFolder.toUri().getScheme());

        byte[] data = {0x00}; // not compressed, empty data
        Path handlePath = new Path(tempFolderPath, UUID.randomUUID().toString());
        FileStateHandle stateHandle = prepareFileStateHandle(handlePath, data);

        Configuration configuration = new Configuration();
        configuration.set(CACHE_IDLE_TIMEOUT, Duration.ofDays(365)); // cache forever
        configuration.set(CoreOptions.TMP_DIRS, tempFolderPath);

        try (ChangelogStreamHandleReaderWithCache reader =
                new ChangelogStreamHandleReaderWithCache(configuration)) {

            DataInputStream inputStream = reader.openAndSeek(stateHandle, 0L);

            inputStream.close();
            inputStream.close(); // close twice

            reader.openAndSeek(stateHandle, 0L); // should not throw FileNotFoundException
        }
    }

    private FileStateHandle prepareFileStateHandle(Path path, byte[] data) throws IOException {
        try (InputStream inputStream = new ByteArrayInputStream(data);
                OutputStream outputStream =
                        path.getFileSystem().create(path, FileSystem.WriteMode.OVERWRITE)) {

            IOUtils.copyBytes(inputStream, outputStream);
        }
        return new FileStateHandle(path, data.length);
    }

    private static void registerFileSystem(FileSystem fs, String scheme) {
        FileSystem.initialize(
                new Configuration(),
                new TestingPluginManager(
                        singletonMap(
                                FileSystemFactory.class,
                                Collections.singleton(
                                                new FileSystemFactory() {
                                                    @Override
                                                    public FileSystem create(URI fsUri) {
                                                        return fs;
                                                    }

                                                    @Override
                                                    public String getScheme() {
                                                        return scheme;
                                                    }
                                                })
                                        .iterator())));
    }
}
