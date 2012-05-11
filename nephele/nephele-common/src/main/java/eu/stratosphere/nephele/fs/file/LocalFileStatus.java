/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.fs.file;

import java.io.File;

import eu.stratosphere.nephele.fs.FileStatus;
import eu.stratosphere.nephele.fs.FileSystem;
import eu.stratosphere.nephele.fs.Path;

/**
 * The class <code>LocalFileStatus</code> provides an implementation of the {@link FileStatus} interface
 * for the local file system.
 * 
 * @author warneke
 */
public class LocalFileStatus implements FileStatus {

	/**
	 * The file this file status belongs to.
	 */
	private File file = null;

	/**
	 * The path of this file this file status belongs to.
	 */
	private Path path = null;

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
		this.path = new Path(fs.getUri().getScheme() + ":" + f.getAbsolutePath());
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public long getAccessTime() {

		return 0; // We don't have access files for local files
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public long getBlockSize() {

		return this.file.length();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public long getLen() {

		return this.file.length();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public long getModificationTime() {

		return this.file.lastModified();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public short getReplication() {

		return 1; // For local files replication is always 1
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isDir() {

		return this.file.isDirectory();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Path getPath() {

		return this.path;
	}
}
