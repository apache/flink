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

package org.apache.flink.runtime.fs.hdfs;

import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.core.fs.BlockLocation;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.util.HadoopUtils;

import java.io.IOException;
import java.net.URI;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link FileSystem} that wraps an {@link org.apache.hadoop.fs.FileSystem Hadoop File System}.
 */
public final class HadoopFileSystem extends FileSystem {

	/** The wrapped Hadoop File System. */
	private final org.apache.hadoop.fs.FileSystem fs;

	/**
	 * Wraps the given Hadoop File System object as a Flink File System object.
	 * The given Hadoop file system object is expected to be initialized already.
	 *
	 * @param hadoopFileSystem The Hadoop FileSystem that will be used under the hood.
	 */
	public HadoopFileSystem(org.apache.hadoop.fs.FileSystem hadoopFileSystem) {
		this.fs = checkNotNull(hadoopFileSystem, "hadoopFileSystem");
	}

	/**
	 * Gets the underlying Hadoop FileSystem.
	 * @return The underlying Hadoop FileSystem.
	 */
	public org.apache.hadoop.fs.FileSystem getHadoopFileSystem() {
		return this.fs;
	}

	// ------------------------------------------------------------------------
	//  file system methods
	// ------------------------------------------------------------------------

	@Override
	public Path getWorkingDirectory() {
		return new Path(this.fs.getWorkingDirectory().toUri());
	}

	public Path getHomeDirectory() {
		return new Path(this.fs.getHomeDirectory().toUri());
	}

	@Override
	public URI getUri() {
		return fs.getUri();
	}

	@Override
	public FileStatus getFileStatus(final Path f) throws IOException {
		org.apache.hadoop.fs.FileStatus status = this.fs.getFileStatus(new org.apache.hadoop.fs.Path(f.toString()));
		return new HadoopFileStatus(status);
	}

	@Override
	public BlockLocation[] getFileBlockLocations(final FileStatus file, final long start, final long len)
			throws IOException {
		if (!(file instanceof HadoopFileStatus)) {
			throw new IOException("file is not an instance of DistributedFileStatus");
		}

		final HadoopFileStatus f = (HadoopFileStatus) file;

		final org.apache.hadoop.fs.BlockLocation[] blkLocations = fs.getFileBlockLocations(f.getInternalFileStatus(),
			start, len);

		// Wrap up HDFS specific block location objects
		final HadoopBlockLocation[] distBlkLocations = new HadoopBlockLocation[blkLocations.length];
		for (int i = 0; i < distBlkLocations.length; i++) {
			distBlkLocations[i] = new HadoopBlockLocation(blkLocations[i]);
		}

		return distBlkLocations;
	}

	@Override
	public HadoopDataInputStream open(final Path f, final int bufferSize) throws IOException {
		final org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(f.toString());
		final org.apache.hadoop.fs.FSDataInputStream fdis = this.fs.open(path, bufferSize);
		return new HadoopDataInputStream(fdis);
	}

	@Override
	public HadoopDataInputStream open(final Path f) throws IOException {
		final org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(f.toString());
		final org.apache.hadoop.fs.FSDataInputStream fdis = fs.open(path);
		return new HadoopDataInputStream(fdis);
	}

	@Override
	public HadoopDataOutputStream create(final Path f, final boolean overwrite, final int bufferSize,
			final short replication, final long blockSize) throws IOException {
		final org.apache.hadoop.fs.FSDataOutputStream fdos = this.fs.create(
			new org.apache.hadoop.fs.Path(f.toString()), overwrite, bufferSize, replication, blockSize);
		return new HadoopDataOutputStream(fdos);
	}

	@Override
	public HadoopDataOutputStream create(final Path f, final WriteMode overwrite) throws IOException {
		final org.apache.hadoop.fs.FSDataOutputStream fsDataOutputStream = this.fs
			.create(new org.apache.hadoop.fs.Path(f.toString()), overwrite == WriteMode.OVERWRITE);
		return new HadoopDataOutputStream(fsDataOutputStream);
	}

	@Override
	public boolean delete(final Path f, final boolean recursive) throws IOException {
		return this.fs.delete(new org.apache.hadoop.fs.Path(f.toString()), recursive);
	}

	@Override
	public FileStatus[] listStatus(final Path f) throws IOException {
		final org.apache.hadoop.fs.FileStatus[] hadoopFiles = this.fs.listStatus(new org.apache.hadoop.fs.Path(f.toString()));
		final FileStatus[] files = new FileStatus[hadoopFiles.length];

		// Convert types
		for (int i = 0; i < files.length; i++) {
			files[i] = new HadoopFileStatus(hadoopFiles[i]);
		}

		return files;
	}

	@Override
	public boolean mkdirs(final Path f) throws IOException {
		return this.fs.mkdirs(new org.apache.hadoop.fs.Path(f.toString()));
	}

	@Override
	public boolean rename(final Path src, final Path dst) throws IOException {
		return this.fs.rename(new org.apache.hadoop.fs.Path(src.toString()),
			new org.apache.hadoop.fs.Path(dst.toString()));
	}

	@SuppressWarnings("deprecation")
	@Override
	public long getDefaultBlockSize() {
		return this.fs.getDefaultBlockSize();
	}

	@Override
	public boolean isDistributedFS() {
		return true;
	}

	// ------------------------------------------------------------------------
	//  Miscellaneous Utilities
	// ------------------------------------------------------------------------

	/**
	 * Returns a new Hadoop Configuration object using the path to the hadoop conf configured
	 * in the main configuration (flink-conf.yaml).
	 * This method is public because its being used in the HadoopDataSource.
	 *
	 * @deprecated This method should not be used, because it dynamically (and possibly incorrectly)
	 *             re-loads the Flink configuration.
	 *             Use {@link HadoopUtils#getHadoopConfiguration(org.apache.flink.configuration.Configuration)}
	 *             instead.
	 */
	@Deprecated
	public static org.apache.hadoop.conf.Configuration getHadoopConfiguration() {

		org.apache.flink.configuration.Configuration flinkConfiguration =
				GlobalConfiguration.loadConfiguration();

		return HadoopUtils.getHadoopConfiguration(flinkConfiguration);
	}
}
