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

package eu.stratosphere.nephele.fs.hdfs;

import java.io.IOException;
import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.fs.BlockLocation;
import eu.stratosphere.nephele.fs.FSDataInputStream;
import eu.stratosphere.nephele.fs.FSDataOutputStream;
import eu.stratosphere.nephele.fs.FileStatus;
import eu.stratosphere.nephele.fs.FileSystem;
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.nephele.util.StringUtils;

/**
 * Concrete implementation of the {@Link FileSystem} base class for the Hadoop Distribution File System. The
 * class is essentially a wrapper class which encapsulated the original Hadoop HDFS API.
 * 
 * @author warneke
 */
public final class DistributedFileSystem extends FileSystem {

	private final Configuration conf;

	private final org.apache.hadoop.fs.FileSystem fs;

	private static final String HDFS_IMPLEMENTATION_KEY = "fs.hdfs.impl";

	private static final Log LOG = LogFactory.getLog(DistributedFileSystem.class);

	/**
	 * Creates a new DistributedFileSystem object to access HDFS
	 * 
	 * @throws IOException
	 *         throw if the required HDFS classes cannot be instantiated
	 */
	public DistributedFileSystem() throws IOException {

		// Create new Hadoop configuration object
		this.conf = new Configuration();

		// Try to load HDFS configuration from Hadoop's own configuration files
		final String hdfsDefaultPath = GlobalConfiguration.getString("fs.hdfs.hdfsdefault", null);
		if (hdfsDefaultPath != null) {
			this.conf.addResource(new org.apache.hadoop.fs.Path(hdfsDefaultPath));
		} else {
			LOG.debug("Cannot find hdfs-default configuration file");
		}

		final String hdfsSitePath = GlobalConfiguration.getString("fs.hdfs.hdfssite", null);
		if (hdfsSitePath != null) {
			conf.addResource(new org.apache.hadoop.fs.Path(hdfsSitePath));
		} else {
			LOG.debug("Cannot find hdfs-site configuration file");
		}

		final Class<?> clazz = conf.getClass(HDFS_IMPLEMENTATION_KEY, null);
		if (clazz == null) {
			throw new IOException("No FileSystem found for " + HDFS_IMPLEMENTATION_KEY);
		}

		try {
			this.fs = (org.apache.hadoop.fs.FileSystem) clazz.newInstance();
		} catch (InstantiationException e) {
			throw new IOException("InstantiationException occured: " + StringUtils.stringifyException(e));
		} catch (IllegalAccessException e) {
			throw new IOException("IllegalAccessException occured: " + StringUtils.stringifyException(e));
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Path getWorkingDirectory() {

		return new Path(this.fs.getWorkingDirectory().toUri());
	}

	@Override
	public URI getUri() {

		return fs.getUri();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void initialize(URI name) throws IOException {

		// For HDFS we have to have an authority
		if (name.getAuthority() == null) {
			name = URI.create(this.conf.get("fs.default.name"));
		}

		// Initialize HDFS
		this.fs.initialize(name, this.conf);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public FileStatus getFileStatus(final Path f) throws IOException {

		org.apache.hadoop.fs.FileStatus status = this.fs.getFileStatus(new org.apache.hadoop.fs.Path(f.toString()));

		return new DistributedFileStatus(status);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public BlockLocation[] getFileBlockLocations(final FileStatus file, final long start, final long len)
			throws IOException {

		if (!(file instanceof DistributedFileStatus)) {
			throw new IOException("file is not an instance of DistributedFileStatus");
		}

		final DistributedFileStatus f = (DistributedFileStatus) file;

		final org.apache.hadoop.fs.BlockLocation[] blkLocations = fs.getFileBlockLocations(f.getInternalFileStatus(),
			start, len);

		// Wrap up HDFS specific block location objects
		final DistributedBlockLocation[] distBlkLocations = new DistributedBlockLocation[blkLocations.length];
		for (int i = 0; i < distBlkLocations.length; i++) {
			distBlkLocations[i] = new DistributedBlockLocation(blkLocations[i]);
		}

		return distBlkLocations;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public FSDataInputStream open(final Path f, final int bufferSize) throws IOException {

		final org.apache.hadoop.fs.FSDataInputStream fdis = this.fs.open(new org.apache.hadoop.fs.Path(f.toString()),
			bufferSize);

		return new DistributedDataInputStream(fdis);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public FSDataInputStream open(final Path f) throws IOException {

		final org.apache.hadoop.fs.FSDataInputStream fdis = fs.open(new org.apache.hadoop.fs.Path(f.toString()));

		return new DistributedDataInputStream(fdis);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public FSDataOutputStream create(final Path f, final boolean overwrite, final int bufferSize,
			final short replication, final long blockSize)
			throws IOException {

		final org.apache.hadoop.fs.FSDataOutputStream fdos = this.fs.create(
			new org.apache.hadoop.fs.Path(f.toString()),
			overwrite, bufferSize, replication, blockSize);

		return new DistributedDataOutputStream(fdos);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public FSDataOutputStream create(final Path f, final boolean overwrite) throws IOException {

		final org.apache.hadoop.fs.FSDataOutputStream fdos = this.fs
			.create(new org.apache.hadoop.fs.Path(f.toString()), overwrite);

		return new DistributedDataOutputStream(fdos);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean delete(final Path f, final boolean recursive) throws IOException {

		return this.fs.delete(new org.apache.hadoop.fs.Path(f.toString()), recursive);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public FileStatus[] listStatus(final Path f) throws IOException {

		final org.apache.hadoop.fs.FileStatus[] hadoopFiles = this.fs.listStatus(new org.apache.hadoop.fs.Path(f
			.toString()));
		final FileStatus[] files = new FileStatus[hadoopFiles.length];

		// Convert types
		for (int i = 0; i < files.length; i++) {
			files[i] = new DistributedFileStatus(hadoopFiles[i]);
		}

		return files;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean mkdirs(final Path f) throws IOException {

		return this.fs.mkdirs(new org.apache.hadoop.fs.Path(f.toString()));
	}

}
