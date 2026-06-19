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

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collection;

import static org.apache.flink.connector.file.src.enumerate.NonSplittingRecursiveEnumeratorTest.assertSplitsEqual;

/** Unit tests for the {@link BlockSplittingRecursiveAllDirEnumerator}. */
public class BlockSplittingRecursiveAllDirEnumeratorTest
        extends NonSplittingRecursiveAllDirEnumeratorTest {
    @Test
    @Override
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

        final Collection<FileSourceSplit> expected =
                Arrays.asList(
                        new FileSourceSplit(
                                "ignoredId", testPath, 0L, 100L, 0, 1000L, "host1", "host2"),
                        new FileSourceSplit(
                                "ignoredId", testPath, 100L, 520L, 0, 1000L, "host1", "host2"),
                        new FileSourceSplit(
                                "ignoredId", testPath, 620L, 380L, 0, 1000L, "host1", "host2"));

        assertSplitsEqual(expected, splits);
    }

    protected FileEnumerator createEnumerator(String pattern) {
        return new BlockSplittingRecursiveAllDirEnumerator(pattern);
    }
}
