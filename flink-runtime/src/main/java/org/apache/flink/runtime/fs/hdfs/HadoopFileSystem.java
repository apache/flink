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

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.UnknownHostException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.flink.core.fs.HadoopFileSystemWrapper;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.core.fs.BlockLocation;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.InstantiationUtil;

import org.apache.hadoop.conf.Configuration;

/**
 * Concrete implementation of the {@link FileSystem} base class for the Hadoop File System. The
 * class is a wrapper class which encapsulated the original Hadoop HDFS API.
 *
 * If no file system class is specified, the wrapper will automatically load the Hadoop
 * distributed file system (HDFS).
 *
 */
public final class HadoopFileSystem extends FileSystem implements HadoopFileSystemWrapper {
	
	private static final Logger LOG = LoggerFactory.getLogger(HadoopFileSystem.class);
	
	private static final String DEFAULT_HDFS_CLASS = "org.apache.hadoop.hdfs.DistributedFileSystem";
	
	/**
	 * Configuration value name for the DFS implementation name. Usually not specified in hadoop configurations.
	 */
	private static final String HDFS_IMPLEMENTATION_KEY = "fs.hdfs.impl";

	private final org.apache.hadoop.conf.Configuration conf;

	private final org.apache.hadoop.fs.FileSystem fs;


	/**
	 * Creates a new DistributedFileSystem object to access HDFS
	 * 
	 * @throws IOException
	 *         throw if the required HDFS classes cannot be instantiated
	 */
	public HadoopFileSystem(Class<? extends org.apache.hadoop.fs.FileSystem> fsClass) throws IOException {
		// Create new Hadoop configuration object
		this.conf = getHadoopConfiguration();

		if(fsClass == null) {
			fsClass = getDefaultHDFSClass();
		}

		this.fs = instantiateFileSystem(fsClass);
	}

	private Class<? extends org.apache.hadoop.fs.FileSystem> getDefaultHDFSClass() throws IOException {
		Class<? extends org.apache.hadoop.fs.FileSystem> fsClass = null;

		// try to get the FileSystem implementation class Hadoop 2.0.0 style
		{
			LOG.debug("Trying to load HDFS class Hadoop 2.x style.");

			Object fsHandle = null;
			try {
				Method newApi = org.apache.hadoop.fs.FileSystem.class.getMethod("getFileSystemClass", String.class, org.apache.hadoop.conf.Configuration.class);
				fsHandle = newApi.invoke(null, "hdfs", conf);
			} catch (Exception e) {
				// if we can't find the FileSystem class using the new API,
				// clazz will still be null, we assume we're running on an older Hadoop version
			}

			if (fsHandle != null) {
				if (fsHandle instanceof Class && org.apache.hadoop.fs.FileSystem.class.isAssignableFrom((Class<?>) fsHandle)) {
					fsClass = ((Class<?>) fsHandle).asSubclass(org.apache.hadoop.fs.FileSystem.class);

					if (LOG.isDebugEnabled()) {
						LOG.debug("Loaded '" + fsClass.getName() + "' as HDFS class.");
					}
				}
				else {
					LOG.debug("Unexpected return type from 'org.apache.hadoop.fs.FileSystem.getFileSystemClass(String, Configuration)'.");
					throw new RuntimeException("The value returned from org.apache.hadoop.fs.FileSystem.getFileSystemClass(String, Configuration) is not a valid subclass of org.apache.hadoop.fs.FileSystem.");
				}
			}
		}

		// fall back to an older Hadoop version
		if (fsClass == null)
		{
			// first of all, check for a user-defined hdfs class
			if (LOG.isDebugEnabled()) {
				LOG.debug("Falling back to loading HDFS class old Hadoop style. Looking for HDFS class configuration entry '"
						+ HDFS_IMPLEMENTATION_KEY + "'.");
			}

			Class<?> classFromConfig = conf.getClass(HDFS_IMPLEMENTATION_KEY, null);

			if (classFromConfig != null)
			{
				if (org.apache.hadoop.fs.FileSystem.class.isAssignableFrom(classFromConfig)) {
					fsClass = classFromConfig.asSubclass(org.apache.hadoop.fs.FileSystem.class);

					if (LOG.isDebugEnabled()) {
						LOG.debug("Loaded HDFS class '" + fsClass.getName() + "' as specified in configuration.");
					}
				}
				else {
					if (LOG.isDebugEnabled()) {
						LOG.debug("HDFS class specified by " + HDFS_IMPLEMENTATION_KEY + " is of wrong type.");
					}

					throw new IOException("HDFS class specified by " + HDFS_IMPLEMENTATION_KEY +
							" cannot be cast to a FileSystem type.");
				}
			}
			else {
				// load the default HDFS class
				if (LOG.isDebugEnabled()) {
					LOG.debug("Trying to load default HDFS implementation " + DEFAULT_HDFS_CLASS);
				}

				try {
					Class <?> reflectedClass = Class.forName(DEFAULT_HDFS_CLASS);
					if (org.apache.hadoop.fs.FileSystem.class.isAssignableFrom(reflectedClass)) {
						fsClass = reflectedClass.asSubclass(org.apache.hadoop.fs.FileSystem.class);
					} else {
						if (LOG.isDebugEnabled()) {
							LOG.debug("Default HDFS class is of wrong type.");
						}

						throw new IOException("The default HDFS class '" + DEFAULT_HDFS_CLASS +
								"' cannot be cast to a FileSystem type.");
					}
				}
				catch (ClassNotFoundException e) {
					if (LOG.isDebugEnabled()) {
						LOG.debug("Default HDFS class cannot be loaded.");
					}

					throw new IOException("No HDFS class has been configured and the default class '" +
							DEFAULT_HDFS_CLASS + "' cannot be loaded.");
				}
			}
		}
		return fsClass;
	}

	/**
	 * Returns a new Hadoop Configuration object using the path to the hadoop conf configured 
	 * in the main configuration (flink-conf.yaml).
	 * This method is public because its being used in the HadoopDataSource.
	 */
	public static org.apache.hadoop.conf.Configuration getHadoopConfiguration() {
		Configuration retConf = new org.apache.hadoop.conf.Configuration();

		// We need to load both core-site.xml and hdfs-site.xml to determine the default fs path and
		// the hdfs configuration
		// Try to load HDFS configuration from Hadoop's own configuration files
		// 1. approach: Flink configuration
		final String hdfsDefaultPath = GlobalConfiguration.getString(ConfigConstants.HDFS_DEFAULT_CONFIG, null);
		if (hdfsDefaultPath != null) {
			retConf.addResource(new org.apache.hadoop.fs.Path(hdfsDefaultPath));
		} else {
			LOG.debug("Cannot find hdfs-default configuration file");
		}

		final String hdfsSitePath = GlobalConfiguration.getString(ConfigConstants.HDFS_SITE_CONFIG, null);
		if (hdfsSitePath != null) {
			retConf.addResource(new org.apache.hadoop.fs.Path(hdfsSitePath));
		} else {
			LOG.debug("Cannot find hdfs-site configuration file");
		}
		
		// 2. Approach environment variables
		String[] possibleHadoopConfPaths = new String[4]; 
		possibleHadoopConfPaths[0] = GlobalConfiguration.getString(ConfigConstants.PATH_HADOOP_CONFIG, null);
		possibleHadoopConfPaths[1] = System.getenv("HADOOP_CONF_DIR");
		
		if (System.getenv("HADOOP_HOME") != null) {
			possibleHadoopConfPaths[2] = System.getenv("HADOOP_HOME")+"/conf";
			possibleHadoopConfPaths[3] = System.getenv("HADOOP_HOME")+"/etc/hadoop"; // hadoop 2.2
		}

		for (String possibleHadoopConfPath : possibleHadoopConfPaths) {
			if (possibleHadoopConfPath != null) {
				if (new File(possibleHadoopConfPath).exists()) {
					if (new File(possibleHadoopConfPath + "/core-site.xml").exists()) {
						retConf.addResource(new org.apache.hadoop.fs.Path(possibleHadoopConfPath + "/core-site.xml"));

						if (LOG.isDebugEnabled()) {
							LOG.debug("Adding " + possibleHadoopConfPath + "/core-site.xml to hadoop configuration");
						}
					}
					if (new File(possibleHadoopConfPath + "/hdfs-site.xml").exists()) {
						retConf.addResource(new org.apache.hadoop.fs.Path(possibleHadoopConfPath + "/hdfs-site.xml"));

						if (LOG.isDebugEnabled()) {
							LOG.debug("Adding " + possibleHadoopConfPath + "/hdfs-site.xml to hadoop configuration");
						}
					}
				}
			}
		}
		return retConf;
	}
	
	private org.apache.hadoop.fs.FileSystem instantiateFileSystem(Class<? extends org.apache.hadoop.fs.FileSystem> fsClass)
		throws IOException
	{
		try {
			return fsClass.newInstance();
		}
		catch (ExceptionInInitializerError e) {
			throw new IOException("The filesystem class '" + fsClass.getName() + "' throw an exception upon initialization.", e.getException());
		}
		catch (Throwable t) {
			String errorMessage = InstantiationUtil.checkForInstantiationError(fsClass);
			if (errorMessage != null) {
				throw new IOException("The filesystem class '" + fsClass.getName() + "' cannot be instantiated: " + errorMessage);
			} else {
				throw new IOException("An error occurred while instantiating the filesystem class '" +
						fsClass.getName() + "'.", t);
			}
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
		return fs.getUri();
	}

	/**
	 * Gets the underlying Hadoop FileSystem.
	 * @return The underlying Hadoop FileSystem.
	 */
	public org.apache.hadoop.fs.FileSystem getHadoopFileSystem() {
		return this.fs;
	}
	
	@Override
	public void initialize(URI path) throws IOException {
		
		// If the authority is not part of the path, we initialize with the fs.defaultFS entry.
		if (path.getAuthority() == null) {
			
			String configEntry = this.conf.get("fs.defaultFS", null);
			if (configEntry == null) {
				// fs.default.name deprecated as of hadoop 2.2.0 http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/DeprecatedProperties.html
				configEntry = this.conf.get("fs.default.name", null);
			}
			
			if (LOG.isDebugEnabled()) {
				LOG.debug("fs.defaultFS is set to " + configEntry);
			}
			
			if (configEntry == null) {
				throw new IOException(getMissingAuthorityErrorPrefix(path) + "Either no default file system (hdfs) configuration was registered, " +
						"or that configuration did not contain an entry for the default file system (usually 'fs.defaultFS').");
			} else {
				try {
					URI initURI = URI.create(configEntry);
					
					if (initURI.getAuthority() == null) {
						throw new IOException(getMissingAuthorityErrorPrefix(path) + "Either no default file system was registered, " +
								"or the provided configuration contains no valid authority component (fs.default.name or fs.defaultFS) " +
								"describing the (hdfs namenode) host and port.");
					} else {
						try {
							this.fs.initialize(initURI, this.conf);
						}
						catch (IOException e) {
							throw new IOException(getMissingAuthorityErrorPrefix(path) +
									"Could not initialize the file system connection with the given default file system address: " + e.getMessage(), e);
						}
					}
				}
				catch (IllegalArgumentException e) {
					throw new IOException(getMissingAuthorityErrorPrefix(path) +
							"The configuration contains an invalid file system default name (fs.default.name or fs.defaultFS): " + configEntry);
				}
			} 
		}
		else {
			// Initialize file system
			try {
				this.fs.initialize(path, this.conf);
			}
			catch (UnknownHostException e) {
				String message = "The (HDFS NameNode) host at '" + path.getAuthority()
						+ "', specified by file path '" + path.toString() + "', cannot be resolved"
						+ (e.getMessage() != null ? ": " + e.getMessage() : ".");
				
				if (path.getPort() == -1) {
					message += " Hint: Have you forgotten a slash? (correct URI would be 'hdfs:///" + path.getAuthority() + path.getPath() + "' ?)";
				}
				
				throw new IOException(message, e);
			}
			catch (Exception e) {
				throw new IOException("The given file URI (" + path.toString() + ") points to the HDFS NameNode at "
						+ path.getAuthority() + ", but the File System could not be initialized with that address"
					+ (e.getMessage() != null ? ": " + e.getMessage() : "."), e);
			}
		}
	}
	
	private static String getMissingAuthorityErrorPrefix(URI path) {
		return "The given HDFS file URI (" + path.toString() + ") did not describe the HDFS NameNode." +
				" The attempt to use a default HDFS configuration, as specified in the '" + ConfigConstants.HDFS_DEFAULT_CONFIG + "' or '" + 
				ConfigConstants.HDFS_SITE_CONFIG + "' config parameter failed due to the following problem: ";
	}


	@Override
	public FileStatus getFileStatus(final Path f) throws IOException {
		org.apache.hadoop.fs.FileStatus status = this.fs.getFileStatus(new org.apache.hadoop.fs.Path(f.toString()));
		return new HadoopFileStatus(status);
	}

	@Override
	public BlockLocation[] getFileBlockLocations(final FileStatus file, final long start, final long len)
	throws IOException
	{
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
			final short replication, final long blockSize)
	throws IOException
	{
		final org.apache.hadoop.fs.FSDataOutputStream fdos = this.fs.create(
			new org.apache.hadoop.fs.Path(f.toString()), overwrite, bufferSize, replication, blockSize);
		return new HadoopDataOutputStream(fdos);
	}


	@Override
	public HadoopDataOutputStream create(final Path f, final boolean overwrite) throws IOException {
		final org.apache.hadoop.fs.FSDataOutputStream fsDataOutputStream = this.fs
			.create(new org.apache.hadoop.fs.Path(f.toString()), overwrite);
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

	@Override
	public Class<?> getHadoopWrapperClassNameForFileSystem(String scheme) {
		Configuration hadoopConf = getHadoopConfiguration();
		Class<? extends org.apache.hadoop.fs.FileSystem> clazz;
		// We can activate this block once we drop Hadoop1 support (only hd2 has the getFileSystemClass-method)
//		try {
//			clazz = org.apache.hadoop.fs.FileSystem.getFileSystemClass(scheme, hadoopConf);
//		} catch (IOException e) {
//			LOG.info("Flink could not load the Hadoop File system implementation for scheme "+scheme);
//			return null;
//		}
		clazz = hadoopConf.getClass("fs." + scheme + ".impl", null, org.apache.hadoop.fs.FileSystem.class);

		if(clazz != null && LOG.isDebugEnabled()) {
			LOG.debug("Flink supports "+scheme+" with the Hadoop file system wrapper, impl "+clazz);
		}
		return clazz;
	}
}
