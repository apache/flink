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

package org.apache.flink.fs.dummy;

import org.apache.flink.core.fs.BlockLocation;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemKind;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.local.LocalBlockLocation;

import javax.annotation.Nullable;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

/**
 * A FileSystem implementation for integration testing purposes. Supports and serves read-only content from static
 * key value map.
 */
class DummyFSFileSystem extends FileSystem {

	static final URI FS_URI = URI.create("dummy:///");

	private final URI workingDir;

	private final URI homeDir;

	private final Map<String, byte[]> contents;

	DummyFSFileSystem(Map<String, String> contents) {
		this.workingDir = new File(System.getProperty("user.dir")).toURI();
		this.homeDir = new File(System.getProperty("user.home")).toURI();
		this.contents = convertToByteArrayMap(contents);
	}

	// ------------------------------------------------------------------------

	@Override
	public URI getUri() {
		return FS_URI;
	}

	@Override
	public Path getWorkingDirectory() {
		return new Path(workingDir);
	}

	@Override
	public Path getHomeDirectory() {
		return new Path(homeDir);
	}

	@Override
	public boolean exists(Path f) throws IOException {
		return getDataByPath(f) != null;
	}

	@Override
	public FileStatus[] listStatus(final Path f) throws IOException {
		byte[] data = getDataByPath(f);
		if (data == null) {
			return null;
		}
		return new FileStatus[] { new DummyFSFileStatus(f, data.length) };
	}

	@Override
	public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len) throws IOException {
		return new BlockLocation[] {
			new LocalBlockLocation(file.getLen())
		};
	}

	@Override
	public FileStatus getFileStatus(Path f) throws IOException {
		byte[] data = getDataByPath(f);
		if (data == null) {
			throw new FileNotFoundException("File " + f + " does not exist.");
		}
		return new DummyFSFileStatus(f, data.length);
	}

	@Override
	public FSDataInputStream open(final Path f, final int bufferSize) throws IOException {
		return open(f);
	}

	@Override
	public FSDataInputStream open(final Path f) throws IOException {
		return DummyFSInputStream.create(getDataByPath(f));
	}

	@Override
	public boolean delete(final Path path, final boolean recursive) throws IOException {
		throw new UnsupportedOperationException("Dummy FS doesn't support delete operation");
	}

	@Override
	public boolean mkdirs(final Path path) throws IOException {
		throw new UnsupportedOperationException("Dummy FS doesn't support mkdirs operation");
	}

	@Override
	public FSDataOutputStream create(final Path path, final WriteMode overwrite) throws IOException {
		throw new UnsupportedOperationException("Dummy FS doesn't support create operation");
	}

	@Override
	public boolean rename(final Path src, final Path dst) throws IOException {
		throw new UnsupportedOperationException("Dummy FS doesn't support rename operation");
	}

	@Override
	public boolean isDistributedFS() {
		return true;
	}

	@Override
	public FileSystemKind getKind() {
		return FileSystemKind.OBJECT_STORE;
	}

	@Nullable
	private byte[] getDataByPath(Path path) {
		return contents.get(path.toUri().getPath());
	}

	private static Map<String, byte[]> convertToByteArrayMap(Map<String, String> content) {
		Map<String, byte[]> data = new HashMap<>();
		Charset utf8 = Charset.forName("UTF-8");
		content.entrySet().forEach(
			entry -> data.put(entry.getKey(), entry.getValue().getBytes(utf8))
		);
		return data;
	}
}
