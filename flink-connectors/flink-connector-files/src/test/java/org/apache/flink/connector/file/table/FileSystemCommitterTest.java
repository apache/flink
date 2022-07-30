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

package org.apache.flink.connector.file.table;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link FileSystemCommitter}. */
class FileSystemCommitterTest {

    private FileSystemFactory fileSystemFactory = FileSystem::get;

    private TableMetaStoreFactory metaStoreFactory;
    @TempDir private java.nio.file.Path outputPath;
    @TempDir private java.nio.file.Path path;

    @BeforeEach
    public void before() throws IOException {
        metaStoreFactory = new TestMetaStoreFactory(new Path(outputPath.toString()));
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private void createFile(java.nio.file.Path parent, String path, String... files)
            throws IOException {
        java.nio.file.Path dir = Files.createDirectories(Paths.get(parent.toString(), path));
        for (String file : files) {
            Files.createFile(dir.resolve(file));
        }
    }

    @Test
    void testPartition() throws Exception {
        FileSystemCommitter committer =
                new FileSystemCommitter(
                        fileSystemFactory,
                        metaStoreFactory,
                        true,
                        new Path(path.toString()),
                        2,
                        false,
                        new LinkedHashMap<String, String>());

        createFile(path, "task-1/p1=0/p2=0/", "f1", "f2");
        createFile(path, "task-2/p1=0/p2=0/", "f3");
        createFile(path, "task-2/p1=0/p2=1/", "f4");
        committer.commitPartitions();
        assertThat(new File(outputPath.toFile(), "p1=0/p2=0/f1")).exists();
        assertThat(new File(outputPath.toFile(), "p1=0/p2=0/f2")).exists();
        assertThat(new File(outputPath.toFile(), "p1=0/p2=0/f3")).exists();
        assertThat(new File(outputPath.toFile(), "p1=0/p2=1/f4")).exists();

        createFile(path, "task-2/p1=0/p2=1/", "f5");
        committer.commitPartitions();
        assertThat(new File(outputPath.toFile(), "p1=0/p2=0/f1")).exists();
        assertThat(new File(outputPath.toFile(), "p1=0/p2=0/f2")).exists();
        assertThat(new File(outputPath.toFile(), "p1=0/p2=0/f3")).exists();
        assertThat(new File(outputPath.toFile(), "p1=0/p2=1/f5")).exists();

        committer =
                new FileSystemCommitter(
                        fileSystemFactory,
                        metaStoreFactory,
                        false,
                        new Path(path.toString()),
                        2,
                        false,
                        new LinkedHashMap<String, String>());
        createFile(path, "task-2/p1=0/p2=1/", "f6");
        committer.commitPartitions();
        assertThat(new File(outputPath.toFile(), "p1=0/p2=1/f5")).exists();
        assertThat(new File(outputPath.toFile(), "p1=0/p2=1/f6")).exists();
    }

    @Test
    void testNotPartition() throws Exception {
        FileSystemCommitter committer =
                new FileSystemCommitter(
                        fileSystemFactory,
                        metaStoreFactory,
                        true,
                        new Path(path.toString()),
                        0,
                        false,
                        new LinkedHashMap<String, String>());

        createFile(path, "task-1/", "f1", "f2");
        createFile(path, "task-2/", "f3");
        committer.commitPartitions();
        assertThat(new File(outputPath.toFile(), "f1")).exists();
        assertThat(new File(outputPath.toFile(), "f2")).exists();
        assertThat(new File(outputPath.toFile(), "f3")).exists();

        createFile(path, "task-2/", "f4");
        committer.commitPartitions();
        assertThat(new File(outputPath.toFile(), "f4")).exists();

        committer =
                new FileSystemCommitter(
                        fileSystemFactory,
                        metaStoreFactory,
                        false,
                        new Path(path.toString()),
                        0,
                        false,
                        new LinkedHashMap<String, String>());
        createFile(path, "task-2/", "f5");
        committer.commitPartitions();
        assertThat(new File(outputPath.toFile(), "f4")).exists();
        assertThat(new File(outputPath.toFile(), "f5")).exists();
    }

    static class TestMetaStoreFactory implements TableMetaStoreFactory {

        private final Path outputPath;

        TestMetaStoreFactory(Path outputPath) {
            this.outputPath = outputPath;
        }

        @Override
        public TableMetaStore createTableMetaStore() {
            return new TableMetaStore() {

                @Override
                public Path getLocationPath() {
                    return outputPath;
                }

                @Override
                public Optional<Path> getPartition(LinkedHashMap<String, String> partSpec) {
                    return Optional.empty();
                }

                @Override
                public void createOrAlterPartition(
                        LinkedHashMap<String, String> partitionSpec, Path partitionPath)
                        throws Exception {}

                @Override
                public void close() {}
            };
        }
    }
}
