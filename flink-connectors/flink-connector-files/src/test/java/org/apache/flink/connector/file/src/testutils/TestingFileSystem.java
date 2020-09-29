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

package org.apache.flink.connector.file.src.testutils;

import org.apache.flink.core.fs.BlockLocation;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemKind;
import org.apache.flink.core.fs.Path;

import javax.annotation.Nullable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link FileSystem} for tests containing a pre-defined set of files and directories.
 *
 * <p>The file system can be registered under its schema at the file system registry, so that
 * the {@link Path#getFileSystem()} method finds this test file system. Use the
 * {@link TestingFileSystem#register()} method to do that, and don't forget to
 * {@link TestingFileSystem#unregister()} when the test is done.
 */
public class TestingFileSystem extends FileSystem {

	private final String scheme;

	private final Map<Path, Collection<FileStatus>> directories;

	private final Map<Path, TestFileStatus> files;

	private TestingFileSystem(
			final String scheme,
			final Map<Path, Collection<FileStatus>> directories,
			final Map<Path, TestFileStatus> files) {
		this.scheme = scheme;
		this.directories = directories;
		this.files = files;
	}

	// ------------------------------------------------------------------------
	//  Factories
	// ------------------------------------------------------------------------

	public static TestingFileSystem createWithFiles(final String scheme, final Path... files) {
		return createWithFiles(scheme, Arrays.asList(files));
	}

	public static TestingFileSystem createWithFiles(final String scheme, final Collection<Path> files) {
		checkNotNull(scheme, "scheme");
		checkNotNull(files, "files");

		final Collection<TestFileStatus> status = files.stream()
				.map((path) -> TestFileStatus.forFileWithDefaultBlock(path, 10L << 20))
				.collect(Collectors.toList());

		return createForFileStatus(scheme, status);
	}

	public static TestingFileSystem createForFileStatus(final String scheme, final TestFileStatus... files) {
		return createForFileStatus(scheme, Arrays.asList(files));
	}

	public static TestingFileSystem createForFileStatus(final String scheme, final Collection<TestFileStatus> files) {
		checkNotNull(scheme, "scheme");
		checkNotNull(files, "files");

		final HashMap<Path, TestFileStatus> fileMap = new HashMap<>(files.size());
		final HashMap<Path, Collection<FileStatus>> directories = new HashMap<>();

		for (TestFileStatus file : files) {
			if (fileMap.putIfAbsent(file.getPath(), file) != null) {
				throw new IllegalStateException("Already have a status for path " + file.getPath());
			}
			addParentDirectories(file, fileMap, directories);
		}

		return new TestingFileSystem(scheme, directories, fileMap);
	}

	private static void addParentDirectories(
			final TestFileStatus file,
			final Map<Path, TestFileStatus> files,
			final Map<Path, Collection<FileStatus>> directories) {

		final Path parentPath = file.getPath().getParent();
		if (parentPath == null) {
			return;
		}

		final TestFileStatus parentStatus = TestFileStatus.forDirectory(parentPath);
		directories.computeIfAbsent(parentPath, (key) -> new ArrayList<>()).add(file);

		final TestFileStatus existingParent = files.putIfAbsent(parentPath, parentStatus);
		if (existingParent == null) {
			addParentDirectories(parentStatus, files, directories);
		} else {
			checkArgument(existingParent.isDir(), "have a file already for a directory path");
		}
	}


	// ------------------------------------------------------------------------
	//  Test Utility
	// ------------------------------------------------------------------------

	public void register() throws Exception {
		final Object key = createFsKey(scheme);
		final Map<Object, Object> fsMap = getFsRegistry();
		fsMap.put(key, this);
	}

	public void unregister() throws Exception {
		final Object key = createFsKey(scheme);
		final Map<Object, Object> fsMap = getFsRegistry();
		fsMap.remove(key);
	}

	private static Object createFsKey(String scheme) throws Exception {
		final Class<?> fsKeyClass = Class.forName("org.apache.flink.core.fs.FileSystem$FSKey");
		final Constructor<?> ctor = fsKeyClass.getConstructor(String.class, String.class);
		ctor.setAccessible(true);
		return ctor.newInstance(scheme, null);
	}

	@SuppressWarnings("unchecked")
	private static Map<Object, Object> getFsRegistry() throws Exception {
		final Field cacheField = FileSystem.class.getDeclaredField("CACHE");
		cacheField.setAccessible(true);
		return (Map<Object, Object>) cacheField.get(null);
	}

	// ------------------------------------------------------------------------
	//  File System Methods
	// ------------------------------------------------------------------------

	@Override
	public Path getWorkingDirectory() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Path getHomeDirectory() {
		throw new UnsupportedOperationException();
	}

	@Override
	public URI getUri() {
		return URI.create(scheme + "://");
	}

	@Override
	public FileStatus getFileStatus(Path f) throws IOException {
		final FileStatus status = files.get(f);
		if (status != null) {
			return status;
		} else {
			throw new FileNotFoundException("File not found: " + f);
		}
	}

	@Override
	public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len) throws IOException {
		final TestFileStatus status = file instanceof TestFileStatus
				? (TestFileStatus) file
				: files.get(file.getPath());

		if (status == null) {
			throw new FileNotFoundException(file.getPath().toString());
		}

		return status.getBlocks();
	}

	@Override
	public FSDataInputStream open(Path f, int bufferSize) throws IOException {
		return open(f);
	}

	@Override
	public FSDataInputStream open(Path f) throws IOException {
		final TestFileStatus status = (TestFileStatus) getFileStatus(f);
		if (status.stream != null) {
			return status.stream;
		} else {
			throw new UnsupportedOperationException("No stream registered for this file");
		}
	}

	@Override
	public FileStatus[] listStatus(Path f) throws IOException {
		final Collection<FileStatus> dirContents = directories.get(f);
		if (dirContents != null) {
			return dirContents.toArray(new FileStatus[dirContents.size()]);
		} else {
			throw new FileNotFoundException("Directory not found: " + f);
		}
	}

	@Override
	public boolean delete(Path f, boolean recursive) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean mkdirs(Path f) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public FSDataOutputStream create(Path f, WriteMode overwriteMode) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean rename(Path src, Path dst) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isDistributedFS() {
		return false;
	}

	@Override
	public FileSystemKind getKind() {
		return FileSystemKind.FILE_SYSTEM;
	}

	// ------------------------------------------------------------------------

	/**
	 * Test implementation of a {@link FileStatus}.
	 */
	public static final class TestFileStatus implements FileStatus {

		private static final long TIME = System.currentTimeMillis();

		public static TestFileStatus forFileWithDefaultBlock(final Path path, final long len) {
			return forFileWithBlocks(path, len, new TestBlockLocation(0L, len));
		}

		public static TestFileStatus forFileWithBlocks(final Path path, final long len, final BlockLocation... blocks) {
			checkNotNull(blocks);
			return new TestFileStatus(path, len, false, blocks, null);
		}

		public static TestFileStatus forFileWithStream(final Path path, final long len, final FSDataInputStream stream) {
			return new TestFileStatus(path, len, false, new BlockLocation[] {new TestBlockLocation(0L, len)}, stream);
		}

		public static TestFileStatus forDirectory(final Path path) {
			return new TestFileStatus(path, 4096L, true, null, null);
		}

		// ------------------------------------------------

		private final Path path;
		private final long len;
		private final boolean isDir;
		private @Nullable final BlockLocation[] blocks;
		@Nullable final FSDataInputStream stream;

		private TestFileStatus(
				final Path path,
				final long len,
				final boolean isDir,
				@Nullable final BlockLocation[] blocks,
				@Nullable final FSDataInputStream stream) {
			this.path = path;
			this.len = len;
			this.isDir = isDir;
			this.blocks = blocks;
			this.stream = stream;
		}

		@Override
		public long getLen() {
			return len;
		}

		@Override
		public long getBlockSize() {
			return 64 << 20;
		}

		@Override
		public short getReplication() {
			return 1;
		}

		@Override
		public long getModificationTime() {
			return TIME;
		}

		@Override
		public long getAccessTime() {
			return TIME;
		}

		@Override
		public boolean isDir() {
			return isDir;
		}

		@Override
		public Path getPath() {
			return path;
		}

		public BlockLocation[] getBlocks() {
			return blocks;
		}

		@Override
		public String toString() {
			return path.toString() + (isDir ? " [DIR]" : " [FILE]");
		}
	}

	// ------------------------------------------------------------------------

	/**
	 * Test implementation of a {@link BlockLocation}.
	 */
	public static final class TestBlockLocation implements BlockLocation {

		private final String[] hosts;
		private final long offset;
		private final long length;

		public TestBlockLocation(long offset, long length, String... hosts) {
			checkArgument(offset >= 0);
			checkArgument(length >= 0);
			this.offset = offset;
			this.length = length;
			this.hosts = checkNotNull(hosts);
		}

		@Override
		public String[] getHosts() throws IOException {
			return hosts;
		}

		@Override
		public long getOffset() {
			return offset;
		}

		@Override
		public long getLength() {
			return length;
		}

		@Override
		public int compareTo(BlockLocation o) {
			return 0;
		}
	}
}
