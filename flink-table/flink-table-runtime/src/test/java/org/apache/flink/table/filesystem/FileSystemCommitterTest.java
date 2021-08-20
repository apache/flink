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

package org.apache.flink.table.filesystem;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Optional;

/** Test for {@link FileSystemCommitter}. */
public class FileSystemCommitterTest {

    @ClassRule public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

    private File tmpFile;
    private File outputFile;

    private Path tmpPath;

    private FileSystemFactory fileSystemFactory = FileSystem::get;

    private TableMetaStoreFactory metaStoreFactory;

    @Before
    public void before() throws IOException {
        tmpFile = TEMP_FOLDER.newFolder();
        outputFile = TEMP_FOLDER.newFolder();

        tmpPath = new Path(tmpFile.getPath());
        Path outputPath = new Path(outputFile.getPath());
        metaStoreFactory = new TestMetaStoreFactory(outputPath);
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private void createFile(String path, String... files) throws IOException {
        File p1 = new File(tmpFile, path);
        p1.mkdirs();
        for (String file : files) {
            new File(p1, file).createNewFile();
        }
    }

    @Test
    public void testPartition() throws Exception {
        FileSystemCommitter committer =
                new FileSystemCommitter(fileSystemFactory, metaStoreFactory, true, tmpPath, 2);

        createFile("task-1/p1=0/p2=0/", "f1", "f2");
        createFile("task-2/p1=0/p2=0/", "f3");
        createFile("task-2/p1=0/p2=1/", "f4");
        committer.commitPartitions();
        Assert.assertTrue(new File(outputFile, "p1=0/p2=0/f1").exists());
        Assert.assertTrue(new File(outputFile, "p1=0/p2=0/f2").exists());
        Assert.assertTrue(new File(outputFile, "p1=0/p2=0/f3").exists());
        Assert.assertTrue(new File(outputFile, "p1=0/p2=1/f4").exists());

        createFile("task-2/p1=0/p2=1/", "f5");
        committer.commitPartitions();
        Assert.assertTrue(new File(outputFile, "p1=0/p2=0/f1").exists());
        Assert.assertTrue(new File(outputFile, "p1=0/p2=0/f2").exists());
        Assert.assertTrue(new File(outputFile, "p1=0/p2=0/f3").exists());
        Assert.assertTrue(new File(outputFile, "p1=0/p2=1/f5").exists());

        committer = new FileSystemCommitter(fileSystemFactory, metaStoreFactory, false, tmpPath, 2);
        createFile("task-2/p1=0/p2=1/", "f6");
        committer.commitPartitions();
        Assert.assertTrue(new File(outputFile, "p1=0/p2=1/f5").exists());
        Assert.assertTrue(new File(outputFile, "p1=0/p2=1/f6").exists());
    }

    @Test
    public void testNotPartition() throws Exception {
        FileSystemCommitter committer =
                new FileSystemCommitter(fileSystemFactory, metaStoreFactory, true, tmpPath, 0);

        createFile("task-1/", "f1", "f2");
        createFile("task-2/", "f3");
        committer.commitPartitions();
        Assert.assertTrue(new File(outputFile, "f1").exists());
        Assert.assertTrue(new File(outputFile, "f2").exists());
        Assert.assertTrue(new File(outputFile, "f3").exists());

        createFile("task-2/", "f4");
        committer.commitPartitions();
        Assert.assertTrue(new File(outputFile, "f4").exists());

        committer = new FileSystemCommitter(fileSystemFactory, metaStoreFactory, false, tmpPath, 0);
        createFile("task-2/", "f5");
        committer.commitPartitions();
        Assert.assertTrue(new File(outputFile, "f4").exists());
        Assert.assertTrue(new File(outputFile, "f5").exists());
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
