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
import org.apache.flink.core.fs.local.LocalFileSystem;
import org.apache.flink.table.catalog.ObjectIdentifier;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link FileSystemCommitter}. */
public class FileSystemCommitterTest {

    private static final String SUCCESS_FILE_NAME = "_SUCCESS";

    private final FileSystemFactory fileSystemFactory = FileSystem::get;

    private TableMetaStoreFactory metaStoreFactory;
    private List<PartitionCommitPolicy> policies;
    private ObjectIdentifier identifier;
    @TempDir private java.nio.file.Path outputPath;
    @TempDir private java.nio.file.Path path;

    @BeforeEach
    public void before() throws IOException {
        metaStoreFactory = new TestMetaStoreFactory(new Path(outputPath.toString()));
        policies =
                new PartitionCommitPolicyFactory(
                                "metastore,success-file", null, SUCCESS_FILE_NAME, null)
                        .createPolicyChain(
                                Thread.currentThread().getContextClassLoader(),
                                LocalFileSystem::getSharedInstance);
        identifier = ObjectIdentifier.of("hiveCatalog", "default", "test");
    }

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
                        identifier,
                        new LinkedHashMap<>(),
                        policies);

        createFile(path, "task-1-attempt-0/p1=0/p2=0/", "f1", "f2");
        createFile(path, "task-2-attempt-0/p1=0/p2=0/", "f3");
        createFile(path, "task-2-attempt-0/p1=0/p2=1/", "f4");
        committer.commitPartitions();
        assertThat(new File(outputPath.toFile(), "p1=0/p2=0/f1")).exists();
        assertThat(new File(outputPath.toFile(), "p1=0/p2=0/f2")).exists();
        assertThat(new File(outputPath.toFile(), "p1=0/p2=0/f3")).exists();
        assertThat(new File(outputPath.toFile(), "p1=0/p2=0/" + SUCCESS_FILE_NAME)).exists();
        assertThat(new File(outputPath.toFile(), "p1=0/p2=1/f4")).exists();
        assertThat(new File(outputPath.toFile(), "p1=0/p2=1/" + SUCCESS_FILE_NAME)).exists();

        createFile(path, "task-2-attempt-0/p1=0/p2=1/", "f5");
        committer.commitPartitions();
        assertThat(new File(outputPath.toFile(), "p1=0/p2=0/f1")).exists();
        assertThat(new File(outputPath.toFile(), "p1=0/p2=0/f2")).exists();
        assertThat(new File(outputPath.toFile(), "p1=0/p2=0/f3")).exists();
        assertThat(new File(outputPath.toFile(), "p1=0/p2=0/" + SUCCESS_FILE_NAME)).exists();
        assertThat(new File(outputPath.toFile(), "p1=0/p2=1/f5")).exists();
        assertThat(new File(outputPath.toFile(), "p1=0/p2=1/" + SUCCESS_FILE_NAME)).exists();

        committer =
                new FileSystemCommitter(
                        fileSystemFactory,
                        metaStoreFactory,
                        false,
                        new Path(path.toString()),
                        2,
                        false,
                        identifier,
                        new LinkedHashMap<>(),
                        policies);
        createFile(path, "task-2-attempt-0/p1=0/p2=1/", "f6");
        committer.commitPartitions();
        assertThat(new File(outputPath.toFile(), "p1=0/p2=1/f5")).exists();
        assertThat(new File(outputPath.toFile(), "p1=0/p2=1/f6")).exists();
        assertThat(new File(outputPath.toFile(), "p1=0/p2=1/" + SUCCESS_FILE_NAME)).exists();
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
                        identifier,
                        new LinkedHashMap<String, String>(),
                        policies);

        createFile(path, "task-1-attempt-0/", "f1", "f2");
        createFile(path, "task-2-attempt-0/", "f3");
        committer.commitPartitions();
        assertThat(new File(outputPath.toFile(), "f1")).exists();
        assertThat(new File(outputPath.toFile(), "f2")).exists();
        assertThat(new File(outputPath.toFile(), "f3")).exists();
        assertThat(new File(outputPath.toFile(), SUCCESS_FILE_NAME)).exists();

        createFile(path, "task-2-attempt-0/", "f4");
        committer.commitPartitions();
        assertThat(new File(outputPath.toFile(), "f4")).exists();
        assertThat(new File(outputPath.toFile(), SUCCESS_FILE_NAME)).exists();

        committer =
                new FileSystemCommitter(
                        fileSystemFactory,
                        metaStoreFactory,
                        false,
                        new Path(path.toString()),
                        0,
                        false,
                        identifier,
                        new LinkedHashMap<String, String>(),
                        policies);
        createFile(path, "task-2-attempt-0/", "f5");
        committer.commitPartitions();
        assertThat(new File(outputPath.toFile(), "f4")).exists();
        assertThat(new File(outputPath.toFile(), "f5")).exists();
        assertThat(new File(outputPath.toFile(), SUCCESS_FILE_NAME)).exists();
    }

    @Test
    void testEmptyPartition() throws Exception {
        LinkedHashMap<String, String> staticPartitions = new LinkedHashMap<>();
        // add new empty partition
        staticPartitions.put("dt", "2022-08-02");
        FileSystemCommitter committer =
                new FileSystemCommitter(
                        fileSystemFactory,
                        metaStoreFactory,
                        true,
                        new Path(path.toString()),
                        1,
                        false,
                        identifier,
                        staticPartitions,
                        policies);

        createFile(path, "task-1-attempt-0/dt=2022-08-02/");
        createFile(path, "task-2-attempt-0/dt=2022-08-02/");

        committer.commitPartitions();

        File emptyPartitionFile = new File(outputPath.toFile(), "dt=2022-08-02");

        // assert partition dir is empty with only success file
        assertThat(emptyPartitionFile).exists();
        assertThat(emptyPartitionFile).isDirectory();
        assertThat(emptyPartitionFile).isNotEmptyDirectory();
        assertThat(emptyPartitionFile)
                .isDirectoryNotContaining(file -> !file.getName().equals(SUCCESS_FILE_NAME));

        // Add new empty partition to overwrite the old one with data
        createFile(outputPath, "dt=2022-08-02/f1");
        assertThat(new File(emptyPartitionFile, "f1")).exists();

        createFile(path, "task-1-attempt-0/dt=2022-08-02/");
        createFile(path, "task-2-attempt-0/dt=2022-08-02/");
        committer.commitPartitions();

        // assert partition dir is still empty because the partition dir is overwritten
        assertThat(emptyPartitionFile).exists();
        assertThat(emptyPartitionFile).isDirectory();
        assertThat(emptyPartitionFile).isNotEmptyDirectory();
        assertThat(emptyPartitionFile)
                .isDirectoryNotContaining(file -> !file.getName().equals(SUCCESS_FILE_NAME));

        // Add empty partition to the old one with data
        createFile(outputPath, "dt=2022-08-02/f1");
        assertThat(new File(emptyPartitionFile, "f1")).exists();

        createFile(path, "task-1-attempt-0/dt=2022-08-02/");
        createFile(path, "task-2-attempt-0/dt=2022-08-02/");
        committer =
                new FileSystemCommitter(
                        fileSystemFactory,
                        metaStoreFactory,
                        false,
                        new Path(path.toString()),
                        1,
                        false,
                        identifier,
                        staticPartitions,
                        policies);
        committer.commitPartitions();

        // assert the partition dir contains remaining 'f1' because overwrite is disabled
        assertThat(emptyPartitionFile).exists();
        assertThat(emptyPartitionFile).isDirectory();
        assertThat(emptyPartitionFile).isNotEmptyDirectory();
        assertThat(new File(emptyPartitionFile, "f1")).exists();
        assertThat(new File(emptyPartitionFile, SUCCESS_FILE_NAME)).exists();
    }

    @Test
    void testPartitionPathNotExist() throws Exception {
        Files.delete(path);
        LinkedHashMap<String, String> staticPartitions = new LinkedHashMap<String, String>();
        FileSystemCommitter committer =
                new FileSystemCommitter(
                        fileSystemFactory,
                        metaStoreFactory,
                        true,
                        new Path(path.toString()),
                        1,
                        false,
                        identifier,
                        staticPartitions,
                        policies);
        committer.commitPartitions();
        assertThat(outputPath.toFile().list()).isEqualTo(new String[0]);
    }

    /** A {@link TableMetaStoreFactory} for test purpose. */
    public static class TestMetaStoreFactory implements TableMetaStoreFactory {
        private static final long serialVersionUID = 1L;

        private final Path outputPath;

        public TestMetaStoreFactory(Path outputPath) {
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
