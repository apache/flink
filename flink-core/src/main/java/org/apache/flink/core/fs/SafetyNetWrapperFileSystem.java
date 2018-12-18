/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.core.fs;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.WrappingProxy;

import java.io.IOException;
import java.net.URI;

/**
 * This is a {@link WrappingProxy} around {@link FileSystem} which (i) wraps all opened streams as
 * {@link ClosingFSDataInputStream} or {@link ClosingFSDataOutputStream} and (ii) registers them to
 * a {@link SafetyNetCloseableRegistry}.
 *
 * <p>Streams obtained by this are therefore managed by the {@link SafetyNetCloseableRegistry} to
 * prevent resource leaks from unclosed streams.
 */
@Internal
public class SafetyNetWrapperFileSystem extends FileSystem implements WrappingProxy<FileSystem> {

	private final SafetyNetCloseableRegistry registry;
	private final FileSystem unsafeFileSystem;

	public SafetyNetWrapperFileSystem(FileSystem unsafeFileSystem, SafetyNetCloseableRegistry registry) {
		this.registry = Preconditions.checkNotNull(registry);
		this.unsafeFileSystem = Preconditions.checkNotNull(unsafeFileSystem);
	}

	@Override
	public Path getWorkingDirectory() {
		return unsafeFileSystem.getWorkingDirectory();
	}

	@Override
	public Path getHomeDirectory() {
		return unsafeFileSystem.getHomeDirectory();
	}

	@Override
	public URI getUri() {
		return unsafeFileSystem.getUri();
	}

	@Override
	public FileStatus getFileStatus(Path f) throws IOException {
		return unsafeFileSystem.getFileStatus(f);
	}

	@Override
	public RecoverableWriter createRecoverableWriter() throws IOException {
		return unsafeFileSystem.createRecoverableWriter();
	}

	@Override
	public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len) throws IOException {
		return unsafeFileSystem.getFileBlockLocations(file, start, len);
	}

	@Override
	public FSDataInputStream open(Path f, int bufferSize) throws IOException {
		FSDataInputStream innerStream = unsafeFileSystem.open(f, bufferSize);
		return ClosingFSDataInputStream.wrapSafe(innerStream, registry, String.valueOf(f));
	}

	@Override
	public FSDataInputStream open(Path f) throws IOException {
		FSDataInputStream innerStream = unsafeFileSystem.open(f);
		return ClosingFSDataInputStream.wrapSafe(innerStream, registry, String.valueOf(f));
	}

	@Override
	@SuppressWarnings("deprecation")
	public long getDefaultBlockSize() {
		return unsafeFileSystem.getDefaultBlockSize();
	}

	@Override
	public FileStatus[] listStatus(Path f) throws IOException {
		return unsafeFileSystem.listStatus(f);
	}

	@Override
	public boolean exists(Path f) throws IOException {
		return unsafeFileSystem.exists(f);
	}

	@Override
	public boolean delete(Path f, boolean recursive) throws IOException {
		return unsafeFileSystem.delete(f, recursive);
	}

	@Override
	public boolean mkdirs(Path f) throws IOException {
		return unsafeFileSystem.mkdirs(f);
	}

	@Override
	@SuppressWarnings("deprecation")
	public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize, short replication, long blockSize)
			throws IOException {

		FSDataOutputStream innerStream = unsafeFileSystem.create(f, overwrite, bufferSize, replication, blockSize);
		return ClosingFSDataOutputStream.wrapSafe(innerStream, registry, String.valueOf(f));
	}

	@Override
	public FSDataOutputStream create(Path f, WriteMode overwrite) throws IOException {
		FSDataOutputStream innerStream = unsafeFileSystem.create(f, overwrite);
		return ClosingFSDataOutputStream.wrapSafe(innerStream, registry, String.valueOf(f));
	}

	@Override
	public boolean rename(Path src, Path dst) throws IOException {
		return unsafeFileSystem.rename(src, dst);
	}

	@Override
	public boolean initOutPathLocalFS(Path outPath, WriteMode writeMode, boolean createDirectory) throws IOException {
		return unsafeFileSystem.initOutPathLocalFS(outPath, writeMode, createDirectory);
	}

	@Override
	public boolean initOutPathDistFS(Path outPath, WriteMode writeMode, boolean createDirectory) throws IOException {
		return unsafeFileSystem.initOutPathDistFS(outPath, writeMode, createDirectory);
	}

	@Override
	public boolean isDistributedFS() {
		return unsafeFileSystem.isDistributedFS();
	}

	@Override
	public FileSystemKind getKind() {
		return unsafeFileSystem.getKind();
	}

	@Override
	public FileSystem getWrappedDelegate() {
		return unsafeFileSystem;
	}
}
