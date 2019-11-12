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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemFactory;
import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A factory for the MapR file system.
 *
 * <p>This factory tries to reflectively instantiate the MapR file system. It can only be
 * used when the MapR FS libraries are in the classpath.
 */
public class MapRFsFactory implements FileSystemFactory {

	private static final Logger LOG = LoggerFactory.getLogger(MapRFsFactory.class);

	/** Name of the environment variable to determine the location of the MapR
	 * installation. */
	private static final String MAPR_HOME_ENV = "MAPR_HOME";

	/** The default location of the MapR installation. */
	private static final String DEFAULT_MAPR_HOME = "/opt/mapr/";

	/** The path relative to the MAPR_HOME where MapR stores how to access the
	 * configured clusters. */
	private static final String MAPR_CLUSTER_CONF_FILE = "/conf/mapr-clusters.conf";

	/** Name of the class implementing the MapRFileSystem. */
	private static final String MAPR_FS_CLASS_NAME = "com.mapr.fs.MapRFileSystem";

	// ------------------------------------------------------------------------

	@Override
	public String getScheme() {
		return "maprfs";
	}

	@Override
	public FileSystem create(URI fsUri) throws IOException {
		checkNotNull(fsUri, "fsUri");

		checkMaprFsClassInClassPath();

		try {
			LOG.info("Trying to load and instantiate MapR File System");

			final org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
			final org.apache.hadoop.fs.FileSystem fs;

			final String authority = fsUri.getAuthority();
			if (authority == null || authority.isEmpty()) {

				// Use the default constructor to instantiate MapR file system object
				fs = instantiateMapRFsClass();
			}
			else {
				// We have an authority, check the MapR cluster configuration to
				// find the CLDB locations.
				final String[] cldbLocations = getCLDBLocations(authority);
				fs = instantiateMapRFsClass(authority, cldbLocations);
			}

			// now initialize the Hadoop File System object
			fs.initialize(fsUri, conf);

			return new HadoopFileSystem(fs);
		}
		catch (LinkageError e) {
			throw new IOException("Could not load MapR file system. "  +
					"Please make sure the Flink runtime classes are part of the classpath or dependencies.", e);
		}
		catch (IOException e) {
			throw e;
		}
		catch (Throwable t) {
			throw new IOException("Could not instantiate MapR file system.", t);
		}
	}

	// ------------------------------------------------------------------------
	//  MapR Config Loading
	// ------------------------------------------------------------------------

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
	private static String[] getCLDBLocations(String authority) throws IOException {

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

		if (!maprClusterConf.exists()) {
			throw new IOException("Could not find CLDB configuration '" + maprClusterConf.getAbsolutePath() +
				"', assuming MapR home is '" + maprHome + "'.");
		}

		// Read the cluster configuration file, format is specified at
		// http://doc.mapr.com/display/MapR/mapr-clusters.conf

		try (BufferedReader br = new BufferedReader(new FileReader(maprClusterConf))) {

			String line;
			while ((line = br.readLine()) != null) {

				// Normalize the string
				line = line.trim();
				line = line.replace('\t', ' ');

				final String[] fields = line.split(" ");
				if (fields.length < 1) {
					continue;
				}

				final String clusterName = fields[0];

				if (!clusterName.equals(authority)) {
					continue;
				}

				final List<String> cldbLocations = new ArrayList<>();

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

				return cldbLocations.toArray(new String[cldbLocations.size()]);
			}

		}

		throw new IOException(String.format(
			"Unable to find CLDB locations for cluster %s", authority));
	}

	// ------------------------------------------------------------------------
	//  Reflective FS Instantiation
	// ------------------------------------------------------------------------

	private static void checkMaprFsClassInClassPath() throws IOException {
		try {
			Class.forName(MAPR_FS_CLASS_NAME, false, MapRFsFactory.class.getClassLoader());
		}
		catch (ClassNotFoundException e) {
			throw new IOException("Cannot find MapR FS in classpath: " + MAPR_FS_CLASS_NAME, e);
		}
	}

	@VisibleForTesting
	static org.apache.hadoop.fs.FileSystem instantiateMapRFsClass(Object... args) throws IOException {
		final Class<? extends org.apache.hadoop.fs.FileSystem> fsClazz;

		try {
			fsClazz = Class
				.forName(MAPR_FS_CLASS_NAME)
				.asSubclass(org.apache.hadoop.fs.FileSystem.class);
		} catch (ClassNotFoundException e) {
			throw new IOException("Cannot load MapR FS. Class missing in classpath", e);
		} catch (ClassCastException e) {
			throw new IOException("Class '" + MAPR_FS_CLASS_NAME + "' is not a subclass of org.apache.hadoop.fs.FileSystem");
		}

		final Class<?>[] constructorArgs = Arrays.stream(args).map(Object::getClass).toArray(Class[]::new);
		try {
			final Constructor<? extends org.apache.hadoop.fs.FileSystem> ctor =
				fsClazz.getConstructor(constructorArgs);

			return ctor.newInstance(args);
		} catch (Exception e) {
			throw new IOException("Cannot instantiate MapR FS class", e);
		}
	}
}
