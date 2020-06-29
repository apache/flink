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

import org.junit.After;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * Unit tests for the {@link NonSplittingRecursiveEnumerator}.
 */
public class NonSplittingRecursiveEnumeratorTest {

	/** Testing file system reference, to be cleaned up in an @After method. That way it also gets
	 * cleaned up on a test failure, without needing finally clauses in every test. */
	protected TestingFileSystem testFs;

	@After
	public void unregisterTestFs() throws Exception {
		if (testFs != null) {
			testFs.unregister();
		}
	}

	// ------------------------------------------------------------------------

	@Test
	public void testIncludeFilesFromNestedDirectories() throws Exception {
		final Path[] testPaths = new Path[] {
				new Path("testfs:///dir/file1"),
				new Path("testfs:///dir/nested/file.out"),
				new Path("testfs:///dir/nested/anotherfile.txt")};
		testFs = TestingFileSystem.createWithFiles("testfs", testPaths);
		testFs.register();

		final NonSplittingRecursiveEnumerator enumerator = createEnumerator();
		final Collection<FileSourceSplit> splits = enumerator.enumerateSplits(
				new Path[] { new Path("testfs:///dir")}, 1);

		assertThat(toPaths(splits), containsInAnyOrder(testPaths));
	}

	@Test
	public void testDefaultHiddenFilesFilter() throws Exception {
		final Path[] testPaths = new Path[] {
				new Path("testfs:///visiblefile"),
				new Path("testfs:///.hiddenfile1"),
				new Path("testfs:///_hiddenfile2")};
		testFs = TestingFileSystem.createWithFiles("testfs", testPaths);
		testFs.register();

		final NonSplittingRecursiveEnumerator enumerator = createEnumerator();
		final Collection<FileSourceSplit> splits = enumerator.enumerateSplits(
				new Path[] { new Path("testfs:///")}, 1);

		assertEquals(Collections.singletonList(new Path("testfs:///visiblefile")), toPaths(splits));
	}

	@Test
	public void testHiddenDirectories() throws Exception {
		final Path[] testPaths = new Path[] {
				new Path("testfs:///dir/visiblefile"),
				new Path("testfs:///dir/.hiddendir/file"),
				new Path("testfs:///_notvisible/afile")};
		testFs = TestingFileSystem.createWithFiles("testfs", testPaths);
		testFs.register();

		final NonSplittingRecursiveEnumerator enumerator = createEnumerator();
		final Collection<FileSourceSplit> splits = enumerator.enumerateSplits(
				new Path[] { new Path("testfs:///")}, 1);

		assertEquals(Collections.singletonList(new Path("testfs:///dir/visiblefile")), toPaths(splits));
	}

	@Test
	public void testFilesWithNoBlockInfo() throws Exception {
		final Path testPath = new Path("testfs:///dir/file1");
		testFs = TestingFileSystem.createForFileStatus(
				"testfs",
				TestingFileSystem.TestFileStatus.forFileWithBlocks(testPath, 12345L));
		testFs.register();

		final NonSplittingRecursiveEnumerator enumerator = createEnumerator();
		final Collection<FileSourceSplit> splits = enumerator.enumerateSplits(
				new Path[] { new Path("testfs:///dir")}, 0);

		assertEquals(1, splits.size());
		assertSplitsEqual(
				new FileSourceSplit("ignoredId", testPath, 0L, 12345L),
				splits.iterator().next());
	}

	@Test
	public void testFileWithIncorrectBlocks() throws Exception {
		final Path testPath = new Path("testfs:///testdir/testfile");

		testFs = TestingFileSystem.createForFileStatus(
				"testfs",
				TestingFileSystem.TestFileStatus.forFileWithBlocks(testPath, 10000L,
						new TestingFileSystem.TestBlockLocation(0L, 1000L),
						new TestingFileSystem.TestBlockLocation(2000L, 1000L)));
		testFs.register();

		final NonSplittingRecursiveEnumerator enumerator = createEnumerator();
		final Collection<FileSourceSplit> splits = enumerator.enumerateSplits(
				new Path[] { new Path("testfs:///testdir")}, 0);

		assertEquals(1, splits.size());
		assertSplitsEqual(
				new FileSourceSplit("ignoredId", testPath, 0L, 10000L),
				splits.iterator().next());
	}

	@Test
	public void testFileWithMultipleBlocks() throws Exception {
		final Path testPath = new Path("testfs:///dir/file");
		testFs = TestingFileSystem.createForFileStatus(
				"testfs",
				TestingFileSystem.TestFileStatus.forFileWithBlocks(testPath, 1000L,
						new TestingFileSystem.TestBlockLocation(0L, 100L, "host1", "host2"),
						new TestingFileSystem.TestBlockLocation(100L, 520L, "host2", "host3"),
						new TestingFileSystem.TestBlockLocation(620L, 380L, "host3", "host4")));
		testFs.register();

		final NonSplittingRecursiveEnumerator enumerator = createEnumerator();
		final Collection<FileSourceSplit> splits = enumerator.enumerateSplits(
				new Path[] { new Path("testfs:///dir")}, 0);

		assertSplitsEqual(
				new FileSourceSplit("ignoredId", testPath, 0L, 1000L, "host1", "host2", "host3", "host4"),
				splits.iterator().next());
	}

	// ------------------------------------------------------------------------

	/**
	 * The instantiation of the enumerator is overridable so that we can reuse these tests for sub-classes.
	 */
	protected NonSplittingRecursiveEnumerator createEnumerator() {
		return new NonSplittingRecursiveEnumerator();
	}

	// ------------------------------------------------------------------------

	protected static void assertSplitsEqual(final FileSourceSplit expected, final FileSourceSplit actual) {
		assertEquals(expected.path(), actual.path());
		assertEquals(expected.offset(), actual.offset());
		assertEquals(expected.length(), actual.length());
		assertArrayEquals(expected.hostnames(), actual.hostnames());
	}

	protected static void assertSplitsEqual(
			final Collection<FileSourceSplit> expected,
			final Collection<FileSourceSplit> actual) {

		assertEquals(expected.size(), actual.size());

		final ArrayList<FileSourceSplit> expectedCopy = new ArrayList<>(expected);
		final ArrayList<FileSourceSplit> actualCopy = new ArrayList<>(expected);
		expectedCopy.sort(NonSplittingRecursiveEnumeratorTest::compareFileSourceSplit);
		actualCopy.sort(NonSplittingRecursiveEnumeratorTest::compareFileSourceSplit);

		for (int i = 0; i < expectedCopy.size(); i++) {
			assertSplitsEqual(expectedCopy.get(i), actualCopy.get(i));
		}
	}

	protected static Collection<Path> toPaths(Collection<FileSourceSplit> splits) {
		return splits.stream().map(FileSourceSplit::path).collect(Collectors.toList());
	}

	private static int compareFileSourceSplit(FileSourceSplit a, FileSourceSplit b) {
		return Long.compare(a.offset(), b.offset());
	}
}
