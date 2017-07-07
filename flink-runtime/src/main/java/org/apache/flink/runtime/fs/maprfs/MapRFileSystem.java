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

package org.apache.flink.runtime.fs.maprfs;

import org.apache.flink.core.fs.BlockLocation;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.fs.hdfs.HadoopBlockLocation;
import org.apache.flink.runtime.fs.hdfs.HadoopDataInputStream;
import org.apache.flink.runtime.fs.hdfs.HadoopDataOutputStream;
import org.apache.flink.runtime.fs.hdfs.HadoopFileStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * Concrete implementation of the {@link FileSystem} base class for the MapR
 * file system. The class contains MapR specific code to initialize the
 * connection to the file system. Apart from that, we code mainly reuses the
 * existing HDFS wrapper code.
 */
public final class MapRFileSystem extends FileSystem {

	/**
	 * The log object used for debugging.
	 */
	private static final Logger LOG = LoggerFactory.getLogger(MapRFileSystem.class);

	/**
	 * The name of MapR's class containing the implementation of the Hadoop HDFS
	 * interface.
	 */
	private static final String MAPR_FS_IMPL_CLASS = "com.mapr.fs.MapRFileSystem";

	/**
	 * Name of the environment variable to determine the location of the MapR
	 * installation.
	 */
	private static final String MAPR_HOME_ENV = "MAPR_HOME";

	/**
	 * The default location of the MapR installation.
	 */
	private static final String DEFAULT_MAPR_HOME = "/opt/mapr/";

	/**
	 * The path relative to the MAPR_HOME where MapR stores how to access the
	 * configured clusters.
	 */
	private static final String MAPR_CLUSTER_CONF_FILE = "/conf/mapr-clusters.conf";

	/**
	 * A Hadoop configuration object used during the file system initialization.
	 */
	private final org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();

	/**
	 * The MapR class containing the implementation of the Hadoop HDFS
	 * interface.
	 */
	private final Class<? extends org.apache.hadoop.fs.FileSystem> fsClass;

	/**
	 * The MapR implementation of the Hadoop HDFS interface.
	 */
	private org.apache.hadoop.fs.FileSystem fs;

	/**
	 * Creates a new MapRFileSystem object to access the MapR file system.
	 *
	 * @throws IOException
	 *             throw if the required MapR classes cannot be found
	 */
	@SuppressWarnings("unchecked")
	public MapRFileSystem() throws IOException {

		if (LOG.isDebugEnabled()) {
			LOG.debug(String.format(
					"Trying to load class %s to access the MapR file system",
					MAPR_FS_IMPL_CLASS));
		}

		try {
			this.fsClass = (Class<? extends org.apache.hadoop.fs.FileSystem>) Class
					.forName(MAPR_FS_IMPL_CLASS);
		} catch (Exception e) {
			throw new IOException(
					String.format(
							"Cannot find class %s, probably the runtime was not compiled against the MapR Hadoop libraries",
							MAPR_FS_IMPL_CLASS), e);
		}
	}

	@Override
	public Path getWorkingDirectory() {

		return new Path(this.fs.getWorkingDirectory().toUri());
	}

	public Path getHomeDirectory() {
		return new Path(this.fs.getHomeDirectory().toUri());
	}

	@Override
	public URI getUri() {

		return this.fs.getUri();
	}

	@Override
	public void initialize(final URI path) throws IOException {

		if (LOG.isInfoEnabled()) {
			LOG.info(String.format("Initializing MapR file system for path %s",
					path.toString()));
		}

		final String authority = path.getAuthority();
		if (authority == null || authority.isEmpty()) {

			// Use the default constructor to instantiate MapR file system
			// object

			try {
				this.fs = this.fsClass.newInstance();
			} catch (Exception e) {
				throw new IOException(e);
			}
		} else {

			// We have an authority, check the MapR cluster configuration to
			// find the CLDB locations.
			final String[] cldbLocations = getCLDBLocations(authority);

			// Find the appropriate constructor
			final Constructor<? extends org.apache.hadoop.fs.FileSystem> constructor;
			try {
				constructor = this.fsClass.getConstructor(String.class,
						String[].class);
			} catch (NoSuchMethodException e) {
				throw new IOException(e);
			}

			// Instantiate the file system object
			try {
				this.fs = constructor.newInstance(authority, cldbLocations);
			} catch (Exception e) {
				throw new IOException(e);
			}
		}

		this.fs.initialize(path, this.conf);
	}

	/**
	 * Retrieves the CLDB locations for the given MapR cluster name.
	 *
	 * @param authority
	 *            the name of the MapR cluster
	 * @return a list of CLDB locations
	 * @throws IOException
	 *             thrown if the CLDB locations for the given MapR cluster name
	 *             cannot be determined
	 */
	private static String[] getCLDBLocations(final String authority)
			throws IOException {

		// Determine the MapR home
		String maprHome = System.getenv(MAPR_HOME_ENV);
		if (maprHome == null) {
			maprHome = DEFAULT_MAPR_HOME;
		}

		final File maprClusterConf = new File(maprHome, MAPR_CLUSTER_CONF_FILE);

		if (LOG.isDebugEnabled()) {
			LOG.debug(String.format(
					"Trying to retrieve MapR cluster configuration from %s",
					maprClusterConf));
		}

		// Read the cluster configuration file, format is specified at
		// http://doc.mapr.com/display/MapR/mapr-clusters.conf
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(maprClusterConf));

			String line;
			while ((line = br.readLine()) != null) {

				// Normalize the string
				line = line.trim();
				line = line.replace('\t', ' ');

				final String[] fields = line.split(" ");
				if (fields == null) {
					continue;
				}

				if (fields.length < 1) {
					continue;
				}

				final String clusterName = fields[0];

				if (!clusterName.equals(authority)) {
					continue;
				}

				final List<String> cldbLocations = new ArrayList<String>();

				for (int i = 1; i < fields.length; ++i) {

					// Make sure this is not a key-value pair MapR recently
					// introduced in the file format along with their security
					// features.
					if (!fields[i].isEmpty() && !fields[i].contains("=")) {
						cldbLocations.add(fields[i]);
					}
				}

				if (cldbLocations.isEmpty()) {
					throw new IOException(
							String.format(
									"%s contains entry for cluster %s but no CLDB locations.",
									maprClusterConf, authority));
				}

				return cldbLocations.toArray(new String[0]);
			}

		} finally {
			if (br != null) {
				br.close();
			}
		}

		throw new IOException(String.format(
				"Unable to find CLDB locations for cluster %s", authority));
	}

	@Override
	public FileStatus getFileStatus(final Path f) throws IOException {

		final org.apache.hadoop.fs.FileStatus status = this.fs
				.getFileStatus(new org.apache.hadoop.fs.Path(f.toString()));

		return new HadoopFileStatus(status);
	}

	@Override
	public BlockLocation[] getFileBlockLocations(final FileStatus file,
			final long start, final long len) throws IOException {

		if (!(file instanceof HadoopFileStatus)) {
			throw new IOException(
					"file is not an instance of DistributedFileStatus");
		}

		final HadoopFileStatus f = (HadoopFileStatus) file;

		final org.apache.hadoop.fs.BlockLocation[] blkLocations = fs
				.getFileBlockLocations(f.getInternalFileStatus(), start, len);

		// Wrap up HDFS specific block location objects
		final HadoopBlockLocation[] distBlkLocations = new HadoopBlockLocation[blkLocations.length];
		for (int i = 0; i < distBlkLocations.length; i++) {
			distBlkLocations[i] = new HadoopBlockLocation(blkLocations[i]);
		}

		return distBlkLocations;
	}

	@Override
	public FSDataInputStream open(final Path f, final int bufferSize)
			throws IOException {
		final org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(f.toString());
		final org.apache.hadoop.fs.FSDataInputStream fdis = this.fs.open(path, bufferSize);
		return new HadoopDataInputStream(fdis);
	}

	@Override
	public FSDataInputStream open(final Path f) throws IOException {
		final org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(f.toString());
		final org.apache.hadoop.fs.FSDataInputStream fdis = this.fs.open(path);

		return new HadoopDataInputStream(fdis);
	}

	@Override
	public FSDataOutputStream create(final Path f, final boolean overwrite,
			final int bufferSize, final short replication, final long blockSize)
			throws IOException {

		final org.apache.hadoop.fs.FSDataOutputStream fdos = this.fs.create(
				new org.apache.hadoop.fs.Path(f.toString()), overwrite,
				bufferSize, replication, blockSize);

		return new HadoopDataOutputStream(fdos);
	}

	@Override
	public FSDataOutputStream create(final Path f, final WriteMode overwrite)
			throws IOException {

		final org.apache.hadoop.fs.FSDataOutputStream fdos = this.fs.create(
				new org.apache.hadoop.fs.Path(f.toString()), overwrite == WriteMode.OVERWRITE);

		return new HadoopDataOutputStream(fdos);
	}

	@Override
	public boolean delete(final Path f, final boolean recursive)
			throws IOException {

		return this.fs.delete(new org.apache.hadoop.fs.Path(f.toString()),
				recursive);
	}

	@Override
	public FileStatus[] listStatus(final Path f) throws IOException {

		final org.apache.hadoop.fs.FileStatus[] hadoopFiles = this.fs
				.listStatus(new org.apache.hadoop.fs.Path(f.toString()));
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
}
