/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.forst.fs;

import org.apache.flink.core.fs.local.LocalFileSystem;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ForStFileSystemTrackingCreatedDirDecorator}. */
public class ForStFileSystemTrackingCreatedDirDecoratorTest {
    @TempDir static Path tempDir;

    private static class MockLocalFileSystem extends LocalFileSystem {
        private final boolean dummyMkdirEnabled;

        public MockLocalFileSystem(boolean dummyMkdirEnabled) {
            super();
            this.dummyMkdirEnabled = dummyMkdirEnabled;
        }

        @Override
        public synchronized boolean mkdirs(org.apache.flink.core.fs.Path path) throws IOException {
            if (dummyMkdirEnabled) {
                return true;
            } else {
                return super.mkdirs(path);
            }
        }
    }

    @Test
    public void testMkdirAndCheck() throws IOException {
        mkdirAndCheck(false);
    }

    @Test
    public void testDummyMkdirAndCheck() throws IOException {
        mkdirAndCheck(true);
    }

    void mkdirAndCheck(boolean enableDummyMkdir) throws IOException {
        org.apache.flink.core.fs.Path remotePath =
                new org.apache.flink.core.fs.Path(tempDir.toString() + "/remote");
        org.apache.flink.core.fs.Path localPath =
                new org.apache.flink.core.fs.Path(tempDir.toString() + "/local");

        MockLocalFileSystem mockLocalFileSystem = new MockLocalFileSystem(enableDummyMkdir);
        ForStFlinkFileSystem fileSystem =
                ForStFileSystemUtils.tryDecorate(
                        new ForStFlinkFileSystem(
                                mockLocalFileSystem,
                                remotePath.toString(),
                                localPath.toString(),
                                null));
        if (enableDummyMkdir) {
            assertThat(fileSystem).isInstanceOf(ForStFileSystemTrackingCreatedDirDecorator.class);
        }

        // create a directory
        String dirPathStr = genRandomFilePathStr();
        org.apache.flink.core.fs.Path testMkdirPath = new org.apache.flink.core.fs.Path(dirPathStr);
        fileSystem.mkdirs(testMkdirPath);
        assertThat(mockLocalFileSystem.exists(testMkdirPath)).isEqualTo(!enableDummyMkdir);
        assertThat(fileSystem.exists(testMkdirPath)).isTrue();

        // create sub directories
        for (int i = 0; i < 10; i++) {
            String subDirName = UUID.randomUUID().toString();
            org.apache.flink.core.fs.Path testSubMkdirPath =
                    new org.apache.flink.core.fs.Path(dirPathStr, subDirName);
            fileSystem.mkdirs(testSubMkdirPath);
            assertThat(mockLocalFileSystem.exists(testSubMkdirPath)).isEqualTo(!enableDummyMkdir);
            assertThat(fileSystem.exists(testSubMkdirPath)).isTrue();
        }
    }

    private String genRandomFilePathStr() {
        return tempDir.toString() + "/" + UUID.randomUUID();
    }
}
