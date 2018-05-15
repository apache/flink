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

package org.apache.flink.core.fs;

import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.util.StringUtils;

import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Common tests for the behavior of {@link FileSystem} methods.
 */
public abstract class FileSystemBehaviorTestSuite {

	private static final Random RND = new Random();

	/** The cached file system instance. */
	private FileSystem fs;

	/** The cached base path. */
	private Path basePath;

	// ------------------------------------------------------------------------
	//  FileSystem-specific methods
	// ------------------------------------------------------------------------

	/**
	 * Gets an instance of the {@code FileSystem} to be tested.
	 */
	public abstract FileSystem getFileSystem() throws Exception;

	/**
	 * Gets the base path in the file system under which tests will place their temporary files.
	 */
	public abstract Path getBasePath() throws Exception;

	/**
	 * Gets the kind of the file system (file system, object store, ...).
	 */
	public abstract FileSystemKind getFileSystemKind();

	// ------------------------------------------------------------------------
	//  Init / Cleanup
	// ------------------------------------------------------------------------

	@Before
	public void prepare() throws Exception {
		fs = getFileSystem();
		basePath = new Path(getBasePath(), randomName());
		fs.mkdirs(basePath);
	}

	@After
	public void cleanup() throws Exception {
		fs.delete(basePath, true);
	}

	// ------------------------------------------------------------------------
	//  Suite of Tests
	// ------------------------------------------------------------------------

	// --- file system kind

	@Test
	public void testFileSystemKind() {
		assertEquals(getFileSystemKind(), fs.getKind());
	}

	// --- access and scheme

	@Test
	public void testPathAndScheme() throws Exception {
		assertEquals(fs.getUri(), getBasePath().getFileSystem().getUri());
		assertEquals(fs.getUri().getScheme(), getBasePath().toUri().getScheme());
	}

	@Test
	public void testHomeAndWorkDir() {
		assertEquals(fs.getUri().getScheme(), fs.getWorkingDirectory().toUri().getScheme());
		assertEquals(fs.getUri().getScheme(), fs.getHomeDirectory().toUri().getScheme());
	}

	// --- mkdirs

	@Test
	public void testMkdirsReturnsTrueWhenCreatingDirectory() throws Exception {
		// this test applies to object stores as well, as rely on the fact that they
		// return true when things are not bad

		final Path directory = new Path(basePath, randomName());
		assertTrue(fs.mkdirs(directory));

		if (getFileSystemKind() != FileSystemKind.OBJECT_STORE) {
			assertTrue(fs.exists(directory));
		}
	}

	@Test
	public void testMkdirsCreatesParentDirectories() throws Exception {
		// this test applies to object stores as well, as rely on the fact that they
		// return true when things are not bad

		final Path directory = new Path(new Path(new Path(basePath, randomName()), randomName()), randomName());
		assertTrue(fs.mkdirs(directory));

		if (getFileSystemKind() != FileSystemKind.OBJECT_STORE) {
			assertTrue(fs.exists(directory));
		}
	}

	@Test
	public void testMkdirsReturnsTrueForExistingDirectory() throws Exception {
		// this test applies to object stores as well, as rely on the fact that they
		// return true when things are not bad

		final Path directory = new Path(basePath, randomName());

		// make sure the directory exists
		createRandomFileInDirectory(directory);

		assertTrue(fs.mkdirs(directory));
	}

	@Test
	public void testMkdirsFailsForExistingFile() throws Exception {
		// test is not defined for object stores, they have no proper notion
		// of directories
		assumeNotObjectStore();

		final Path file = new Path(getBasePath(), randomName());
		createFile(file);

		try {
			fs.mkdirs(file);
			fail("should fail with an IOException");
		}
		catch (IOException e) {
			// good!
		}
	}

	@Test
	public void testMkdirsFailsWithExistingParentFile() throws Exception {
		// test is not defined for object stores, they have no proper notion
		// of directories
		assumeNotObjectStore();

		final Path file = new Path(getBasePath(), randomName());
		createFile(file);

		final Path dirUnderFile = new Path(file, randomName());
		try {
			fs.mkdirs(dirUnderFile);
			fail("should fail with an IOException");
		}
		catch (IOException e) {
			// good!
		}
	}

	@Test
	public void testCreateAtomically() throws Exception {
		// test is not defined for object stores, they have no proper notion
		// of directories
		assumeNotObjectStore();

		byte[] resultData = new byte[102400];
		byte[] expectedData = new byte[102400];
		new Random().nextBytes(expectedData);

		final Path file = new Path(getBasePath(), randomName());

		// test the FileSystem#createAtomically(...) work properly for writing data normally.
		{
			try {
				createFileAtomically(file, WriteMode.NO_OVERWRITE, expectedData, false);
			} catch (UnsupportedOperationException ex) {
				// if this FileSystem doesn't support createAtomically(), we quit this test.
				return;
			}

			readFromFile(file, resultData);
			assertArrayEquals(expectedData, resultData);
		}

		// test the FileSystem#createAtomically(...) work properly with WriteMode.NO_OVERWRITE.
		{
			try {
				createFileAtomically(file, WriteMode.NO_OVERWRITE, expectedData, false);
				fail("should fail because the file is already exists.");
			} catch (Exception ex) {
				// we expect this exception.
			}
		}

		// test the atomic feature of the FileSystem#createAtomically(...) with mode WriteMode.NO_OVERWRITE
		{
			final Path file2 = new Path(getBasePath(), randomName());
			try {
				createFileAtomically(file2, WriteMode.NO_OVERWRITE, expectedData, true);
				fail("should throw an exception on purpose.");
			} catch (Exception ex) {
				assertFalse(fs.exists(file2));
			}
		}
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	private static String randomName() {
		return StringUtils.getRandomString(RND, 16, 16, 'a', 'z');
	}

	private void createFile(Path file) throws IOException {
		try (FSDataOutputStream out = fs.create(file, WriteMode.NO_OVERWRITE)) {
			out.write(new byte[] {1, 2, 3, 4, 5, 6, 7, 8});
		}
	}

	private void createRandomFileInDirectory(Path directory) throws IOException {
		fs.mkdirs(directory);
		createFile(new Path(directory, randomName()));
	}

	private void assumeNotObjectStore() {
		Assume.assumeTrue("Test does not apply to object stores",
				getFileSystemKind() != FileSystemKind.OBJECT_STORE);
	}

	private void createFileAtomically(Path file, WriteMode writeMode, byte[] data, boolean throwException) throws Exception {
		try (FSDataOutputStream outputStream = fs.createAtomically(file, writeMode)) {
			outputStream.write(data);
			if (throwException) {
				throw new RuntimeException("throws exception on purpose.");
			}
			((TwoPhaseFSDataOutputStream) outputStream).closeAndPublish();
		}
	}

	private void readFromFile(Path file, byte[] data) throws Exception {
		try (FSDataInputStream inputStream = fs.open(file)) {
			int totalSize = data.length;
			int len;
			int pos = 0;
			while (pos < totalSize && (len = inputStream.read(data, pos, totalSize - pos)) > 0) {
				pos += len;
			}
		}
	}
}
