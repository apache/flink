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

package org.apache.flink.connector.file.src.enumerate;

import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.testutils.TestingFileSystem;
import org.apache.flink.core.fs.Path;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.apache.flink.connector.file.src.enumerate.NonSplittingRecursiveEnumeratorTest.assertSplitsEqual;
import static org.apache.flink.connector.file.src.enumerate.NonSplittingRecursiveEnumeratorTest.toPaths;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for the {@link NonSplittingRecursiveAllDirEnumerator}. */
public class NonSplittingRecursiveAllDirEnumeratorTest {
    /**
     * Testing file system reference, to be cleaned up in an @After method. That way it also gets
     * cleaned up on a test failure, without needing finally clauses in every test.
     */
    protected TestingFileSystem testFs;

    @AfterEach
    void unregisterTestFs() throws Exception {
        if (testFs != null) {
            testFs.unregister();
        }
    }

    // ------------------------------------------------------------------------

    @Test
    void testIncludeSingleFile() throws Exception {
        final Path[] testPaths =
                new Path[] {
                    new Path("testfs:///dir/file1"),
                    new Path("testfs:///dir/nested/file.out"),
                    new Path("testfs:///dir/nested/anotherfile.txt")
                };
        testFs = TestingFileSystem.createWithFiles("testfs", testPaths);
        testFs.register();

        Path baseDir = new Path("testfs:///dir");
        final FileEnumerator enumerator = createEnumerator(baseDir.getPath() + "/nested/file.out");
        final Collection<FileSourceSplit> splits =
                enumerator.enumerateSplits(new Path[] {baseDir}, 1);

        assertThat(toPaths(splits)).containsExactlyInAnyOrder(testPaths[1]);
    }

    @Test
    void testIncludeFilesFromRegexDirectory() throws Exception {
        final Path[] testPaths =
                new Path[] {
                    new Path("testfs:///dir/file1"),
                    new Path("testfs:///dir/nested/file.out"),
                    new Path("testfs:///dir/nested/anotherFile.txt"),
                    new Path("testfs:///dir/nested/nested/doubleNestedFile.txt")
                };
        testFs = TestingFileSystem.createWithFiles("testfs", testPaths);
        testFs.register();

        Path baseDir = new Path("testfs:///dir");
        final FileEnumerator enumerator = createEnumerator(baseDir.getPath() + "/nest.[a-z]/.*");
        final Collection<FileSourceSplit> splits =
                enumerator.enumerateSplits(new Path[] {baseDir}, 1);

        assertThat(toPaths(splits))
                .containsExactlyInAnyOrder(Arrays.copyOfRange(testPaths, 1, testPaths.length));
    }

    @Test
    void testIncludeSingleFileFromMultiDirectory() throws Exception {
        final Path[] testPaths =
                new Path[] {
                    new Path("testfs:///dir/file1"),
                    new Path("testfs:///dir/nested/file.out"),
                    new Path("testfs:///dir/nested/anotherFile.txt"),
                    new Path("testfs:///dir/nested/nested/doubleNestedFile.txt"),
                    new Path("testfs:///dir/anotherNested/file.out"),
                    new Path("testfs:///dir/anotherNested/nested/file.out"),
                };
        testFs = TestingFileSystem.createWithFiles("testfs", testPaths);
        testFs.register();

        Path baseDir = new Path("testfs:///dir");
        final FileEnumerator enumerator = createEnumerator(baseDir.getPath() + "/.*/file.out");
        final Collection<FileSourceSplit> splits =
                enumerator.enumerateSplits(new Path[] {baseDir}, 1);

        assertThat(toPaths(splits))
                .containsExactlyInAnyOrder(
                        Arrays.stream(testPaths)
                                .filter(p -> p.getPath().endsWith("file.out"))
                                .toArray(Path[]::new));
    }

    @Test
    void testDefaultHiddenFilesFilter() throws Exception {
        final Path[] testPaths =
                new Path[] {
                    new Path("testfs:///visiblefile"),
                    new Path("testfs:///.hiddenfile1"),
                    new Path("testfs:///_hiddenfile2")
                };
        testFs = TestingFileSystem.createWithFiles("testfs", testPaths);
        testFs.register();

        Path baseDir = new Path("testfs:///");
        final FileEnumerator enumerator = createEnumerator("/.*");
        final Collection<FileSourceSplit> splits =
                enumerator.enumerateSplits(new Path[] {baseDir}, 1);

        assertThat(toPaths(splits)).isEqualTo(Collections.singletonList(testPaths[0]));
    }

    @Test
    void testHiddenDirectories() throws Exception {
        final Path[] testPaths =
                new Path[] {
                    new Path("testfs:///dir/visiblefile"),
                    new Path("testfs:///dir/.hiddendir/file"),
                    new Path("testfs:///_notvisible/afile")
                };
        testFs = TestingFileSystem.createWithFiles("testfs", testPaths);
        testFs.register();

        Path baseDir = new Path("testfs:///");
        final FileEnumerator enumerator = createEnumerator("/.*");
        final Collection<FileSourceSplit> splits =
                enumerator.enumerateSplits(new Path[] {baseDir}, 1);

        assertThat(toPaths(splits)).isEqualTo(Collections.singletonList(testPaths[0]));
    }

    @Test
    void testFilesWithNoBlockInfo() throws Exception {
        final Path testPath = new Path("testfs:///dir/file1");
        testFs =
                TestingFileSystem.createForFileStatus(
                        "testfs",
                        TestingFileSystem.TestFileStatus.forFileWithBlocks(testPath, 12345L));
        testFs.register();

        final FileEnumerator enumerator = createEnumerator("/.*/file.");
        final Collection<FileSourceSplit> splits =
                enumerator.enumerateSplits(new Path[] {new Path("testfs:///dir")}, 0);

        assertThat(splits).hasSize(1);
        assertSplitsEqual(
                new FileSourceSplit("ignoredId", testPath, 0L, 12345L, 0, 12345L),
                splits.iterator().next());
    }

    @Test
    void testFileWithIncorrectBlocks() throws Exception {
        final Path testPath = new Path("testfs:///testdir/testfile");

        testFs =
                TestingFileSystem.createForFileStatus(
                        "testfs",
                        TestingFileSystem.TestFileStatus.forFileWithBlocks(
                                testPath,
                                10000L,
                                new TestingFileSystem.TestBlockLocation(0L, 1000L),
                                new TestingFileSystem.TestBlockLocation(2000L, 1000L)));
        testFs.register();

        final FileEnumerator enumerator = createEnumerator("/.*");
        final Collection<FileSourceSplit> splits =
                enumerator.enumerateSplits(new Path[] {new Path("testfs:///testdir")}, 0);

        assertThat(splits).hasSize(1);
        assertSplitsEqual(
                new FileSourceSplit("ignoredId", testPath, 0L, 10000L, 0, 12345L),
                splits.iterator().next());
    }

    @Test
    void testFileWithMultipleBlocks() throws Exception {
        final Path testPath = new Path("testfs:///dir/file");
        testFs =
                TestingFileSystem.createForFileStatus(
                        "testfs",
                        TestingFileSystem.TestFileStatus.forFileWithBlocks(
                                testPath,
                                1000L,
                                new TestingFileSystem.TestBlockLocation(0L, 100L, "host1", "host2"),
                                new TestingFileSystem.TestBlockLocation(
                                        100L, 520L, "host2", "host3"),
                                new TestingFileSystem.TestBlockLocation(
                                        620L, 380L, "host3", "host4")));
        testFs.register();

        final FileEnumerator enumerator = createEnumerator(testPath.getPath());
        final Collection<FileSourceSplit> splits =
                enumerator.enumerateSplits(new Path[] {new Path("testfs:///dir")}, 0);

        assertSplitsEqual(
                new FileSourceSplit(
                        "ignoredId",
                        testPath,
                        0L,
                        1000L,
                        0,
                        1000L,
                        "host1",
                        "host2",
                        "host3",
                        "host4"),
                splits.iterator().next());
    }

    // ------------------------------------------------------------------------

    /**
     * The instantiation of the enumerator is overridable so that we can reuse these tests for
     * sub-classes.
     */
    protected FileEnumerator createEnumerator(String pattern) {
        return new NonSplittingRecursiveAllDirEnumerator(pattern);
    }
}
