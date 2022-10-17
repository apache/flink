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

package org.apache.flink.connector.file.table.batch;

import org.apache.flink.connector.file.table.FileSystemCommitterTest;
import org.apache.flink.connector.file.table.FileSystemFactory;
import org.apache.flink.connector.file.table.PartitionCommitPolicyFactory;
import org.apache.flink.connector.file.table.TableMetaStoreFactory;
import org.apache.flink.connector.file.table.batch.compact.BatchPartitionCommitter;
import org.apache.flink.connector.file.table.stream.compact.CompactMessages.CompactOutput;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.catalog.ObjectIdentifier;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for batch partition committer. */
public class BatchPartitionCommitterTest {

    private final FileSystemFactory fileSystemFactory = FileSystem::get;

    private TableMetaStoreFactory metaStoreFactory;
    private ObjectIdentifier identifier;
    @TempDir private java.nio.file.Path path;
    @TempDir private java.nio.file.Path outputPath;

    @BeforeEach
    public void before() {
        metaStoreFactory =
                new FileSystemCommitterTest.TestMetaStoreFactory(new Path(outputPath.toString()));
        identifier = ObjectIdentifier.of("hiveCatalog", "default", "test");
    }

    @Test
    public void testPartitionCommit() throws Exception {
        BatchPartitionCommitter partitionCommitter =
                new BatchPartitionCommitter(
                        fileSystemFactory,
                        metaStoreFactory,
                        false,
                        false,
                        new Path(path.toString()),
                        new String[] {"p1", "p2"},
                        new LinkedHashMap<>(),
                        identifier,
                        new PartitionCommitPolicyFactory(null, null, null));
        try (OneInputStreamOperatorTestHarness<CompactOutput, Void> testHarness =
                new OneInputStreamOperatorTestHarness<>(partitionCommitter)) {
            testHarness.setup();
            testHarness.open();

            List<Path> pathList1 = createFiles(path, "task-1/p1=0/p2=0/", "f1", "f2");
            List<Path> pathList2 = createFiles(path, "task-2/p1=0/p2=0/", "f3");
            List<Path> pathList3 = createFiles(path, "task-2/p1=0/p2=1/", "f4");

            Map<String, List<Path>> compactedFiles = new HashMap<>();
            pathList1.addAll(pathList2);
            compactedFiles.put("p1=0/p2=0/", pathList1);
            compactedFiles.put("p1=0/p2=1/", pathList3);

            testHarness.processElement(new StreamRecord<>(new CompactOutput(compactedFiles)));
        }
        assertThat(new File(outputPath.toFile(), "p1=0/p2=0/f1")).exists();
        assertThat(new File(outputPath.toFile(), "p1=0/p2=0/f2")).exists();
        assertThat(new File(outputPath.toFile(), "p1=0/p2=0/f3")).exists();
        assertThat(new File(outputPath.toFile(), "p1=0/p2=1/f4")).exists();
    }

    private List<Path> createFiles(java.nio.file.Path parent, String path, String... files)
            throws IOException {
        java.nio.file.Path dir = Files.createDirectories(Paths.get(parent.toString(), path));
        List<Path> paths = new ArrayList<>();
        for (String file : files) {
            paths.add(new Path(Files.createFile(dir.resolve(file)).toFile().getPath()));
        }
        return paths;
    }
}
