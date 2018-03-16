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

import org.apache.flink.core.fs.FileSystemKind;
import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A MapR file system client for Flink.
 *
 * <p>Internally, this class wraps the {@link org.apache.hadoop.fs.FileSystem} implementation
 * of the MapR file system client.
 */
public class MapRFileSystem extends HadoopFileSystem {

	private static final Logger LOG = LoggerFactory.getLogger(MapRFileSystem.class);

	/** Name of the environment variable to determine the location of the MapR
	 * installation. */
	private static final String MAPR_HOME_ENV = "MAPR_HOME";

	/** The default location of the MapR installation. */
	private static final String DEFAULT_MAPR_HOME = "/opt/mapr/";

	/** The path relative to the MAPR_HOME where MapR stores how to access the
	 * configured clusters. */
	private static final String MAPR_CLUSTER_CONF_FILE = "/conf/mapr-clusters.conf";

	// ------------------------------------------------------------------------

	/**
	 * Creates a MapRFileSystem for the given URI.
	 *
	 * @param fsUri The URI describing the file system
	 * @throws IOException Thrown if the file system could not be initialized.
	 */
	public MapRFileSystem(URI fsUri) throws IOException {
		super(instantiateMapRFileSystem(fsUri));
	}

	private static org.apache.hadoop.fs.FileSystem instantiateMapRFileSystem(URI fsUri) throws IOException {
		checkNotNull(fsUri, "fsUri");

		final org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
		final com.mapr.fs.MapRFileSystem fs;

		final String authority = fsUri.getAuthority();
		if (authority == null || authority.isEmpty()) {

			// Use the default constructor to instantiate MapR file system object
			fs = new com.mapr.fs.MapRFileSystem();
		}
		else {
			// We have an authority, check the MapR cluster configuration to
			// find the CLDB locations.
			final String[] cldbLocations = getCLDBLocations(authority);
			fs = new com.mapr.fs.MapRFileSystem(authority, cldbLocations);
		}

		// now initialize the Hadoop File System object
		fs.initialize(fsUri, conf);

		return fs;
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

	@Override
	public FileSystemKind getKind() {
		return FileSystemKind.FILE_SYSTEM;
	}
}
