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

package org.apache.flink.fs.s3hadoop;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemFactory;
import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem;
import org.apache.flink.runtime.util.HadoopUtils;

import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

/**
 * Simple factory for the S3 file system.
 */
public class S3FileSystemFactory implements FileSystemFactory {

	private static final Logger LOG = LoggerFactory.getLogger(S3FileSystemFactory.class);

	/** The prefixes that Flink adds to the Hadoop config under 'fs.s3a.'. */
	private static final String[] CONFIG_PREFIXES = { "s3.", "s3a.", "fs.s3a." };

	/** Keys that are replaced (after prefix replacement, to give a more uniform experience
	 * across different file system implementations. */
	private static final String[][] MIRRORED_CONFIG_KEYS = {
			{ "fs.s3a.access-key", "fs.s3a.access.key" },
			{ "fs.s3a.secret-key", "fs.s3a.secret.key" }
	};

	/** Flink's configuration object. */
	private Configuration flinkConfig;

	/** Hadoop's configuration for the file systems, lazily initialized. */
	private org.apache.hadoop.conf.Configuration hadoopConfig;

	@Override
	public String getScheme() {
		return "s3";
	}

	@Override
	public void configure(Configuration config) {
		flinkConfig = config;
		hadoopConfig = null;
	}

	@Override
	public FileSystem create(URI fsUri) throws IOException {
		LOG.debug("Creating S3 file system (backed by a Hadoop s3a file system)");

		try {
			// -- (1) get the loaded Hadoop config (or fall back to one loaded from the classpath)

			org.apache.hadoop.conf.Configuration hadoopConfig = this.hadoopConfig;
			if (hadoopConfig == null) {
				if (flinkConfig != null) {
					LOG.debug("Loading Hadoop configuration for s3a file system");
					hadoopConfig = HadoopUtils.getHadoopConfiguration(flinkConfig);

					// add additional config entries from the Flink config to the Presto Hadoop config
					for (String key : flinkConfig.keySet()) {
						for (String prefix : CONFIG_PREFIXES) {
							if (key.startsWith(prefix)) {
								String value = flinkConfig.getString(key, null);
								String newKey = "fs.s3a." + key.substring(prefix.length());
								hadoopConfig.set(newKey, flinkConfig.getString(key, null));

								LOG.debug("Adding Flink config entry for {} as {}={} to Hadoop config for " +
										"S3A File System", key, newKey, value);
							}
						}
					}

					// mirror certain keys to make use more uniform across s3 implementations
					// with different keys
					for (String[] mirrored : MIRRORED_CONFIG_KEYS) {
						String value = hadoopConfig.get(mirrored[0], null);
						if (value != null) {
							hadoopConfig.set(mirrored[1], value);
						}
					}

					this.hadoopConfig = hadoopConfig;
				}
				else {
					LOG.warn("The factory has not been configured prior to loading the S3 file system."
							+ " Using Hadoop configuration from the classpath.");

					hadoopConfig = new org.apache.hadoop.conf.Configuration();
					this.hadoopConfig = hadoopConfig;
				}
			}

			// -- (2) Instantiate the Hadoop file system class for that scheme

			final String scheme = fsUri.getScheme();
			final String authority = fsUri.getAuthority();

			if (scheme == null && authority == null) {
				fsUri = org.apache.hadoop.fs.FileSystem.getDefaultUri(hadoopConfig);
			}
			else if (scheme != null && authority == null) {
				URI defaultUri = org.apache.hadoop.fs.FileSystem.getDefaultUri(hadoopConfig);
				if (scheme.equals(defaultUri.getScheme()) && defaultUri.getAuthority() != null) {
					fsUri = defaultUri;
				}
			}

			LOG.debug("Using scheme {} for s3a file system backing the S3 File System", fsUri);

			final S3AFileSystem fs = new S3AFileSystem();
			fs.initialize(fsUri, hadoopConfig);

			return new HadoopFileSystem(fs);
		}
		catch (IOException e) {
			throw e;
		}
		catch (Exception e) {
			throw new IOException(e.getMessage(), e);
		}
	}
}
