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

package org.apache.flink.core.fs.local;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import java.io.File;

/**
 * The class <code>LocalFileStatus</code> provides an implementation of the {@link FileStatus} interface
 * for the local file system.
 */
@Internal
public class LocalFileStatus implements FileStatus {

	/**
	 * The file this file status belongs to.
	 */
	private final File file;

	/**
	 * The path of this file this file status belongs to.
	 */
	private final Path path;

	/**
	 * Creates a <code>LocalFileStatus</code> object from a given {@link File} object.
	 *
	 * @param f
	 *        the {@link File} object this <code>LocalFileStatus</code> refers to
	 * @param fs
	 *        the file system the corresponding file has been read from
	 */
	public LocalFileStatus(final File f, final FileSystem fs) {
		this.file = f;
		this.path = new Path(fs.getUri().getScheme() + ":" + f.toURI().getPath());
	}

	@Override
	public long getAccessTime() {
		return 0; // We don't have access files for local files
	}

	@Override
	public long getBlockSize() {
		return this.file.length();
	}

	@Override
	public long getLen() {
		return this.file.length();
	}

	@Override
	public long getModificationTime() {
		return this.file.lastModified();
	}

	@Override
	public short getReplication() {
		return 1; // For local files replication is always 1
	}

	@Override
	public boolean isDir() {
		return this.file.isDirectory();
	}

	@Override
	public Path getPath() {
		return this.path;
	}

	public File getFile() {
		return this.file;
	}

	@Override
	public String toString() {
		return "LocalFileStatus{" +
			"file=" + file +
			", path=" + path +
			'}';
	}
}
