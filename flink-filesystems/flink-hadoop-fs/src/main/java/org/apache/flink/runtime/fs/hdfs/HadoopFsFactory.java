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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemFactory;
import org.apache.flink.core.fs.LimitedConnectionsFileSystem;
import org.apache.flink.core.fs.LimitedConnectionsFileSystem.ConnectionLimitingSettings;
import org.apache.flink.core.fs.UnsupportedFileSystemSchemeException;
import org.apache.flink.runtime.util.HadoopUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A file system factory for Hadoop-based file systems.
 *
 * <p>This factory calls Hadoop's mechanism to find a file system implementation for a given file
 * system scheme (a {@link org.apache.hadoop.fs.FileSystem}) and wraps it as a Flink file system
 * (a {@link org.apache.flink.core.fs.FileSystem}).
 */
public class HadoopFsFactory implements FileSystemFactory {

	private static final Logger LOG = LoggerFactory.getLogger(HadoopFsFactory.class);

	/** Flink's configuration object. */
	private Configuration flinkConfig;

	/** Hadoop's configuration for the file systems. */
	private org.apache.hadoop.conf.Configuration hadoopConfig;

	@Override
	public String getScheme() {
		// the hadoop factory creates various schemes
		return "*";
	}

	@Override
	public void configure(Configuration config) {
		flinkConfig = config;
		hadoopConfig = null; // reset the Hadoop Config
	}

	@Override
	public FileSystem create(URI fsUri) throws IOException {
		checkNotNull(fsUri, "fsUri");

		final String scheme = fsUri.getScheme();
		checkArgument(scheme != null, "file system has null scheme");

		// from here on, we need to handle errors due to missing optional
		// dependency classes
		try {
			// -- (1) get the loaded Hadoop config (or fall back to one loaded from the classpath)

			final org.apache.hadoop.conf.Configuration hadoopConfig;
			if (this.hadoopConfig != null) {
				hadoopConfig = this.hadoopConfig;
			}
			else if (flinkConfig != null) {
				hadoopConfig = HadoopUtils.getHadoopConfiguration(flinkConfig);
				this.hadoopConfig = hadoopConfig;
			}
			else {
				LOG.warn("Hadoop configuration has not been explicitly initialized prior to loading a Hadoop file system."
						+ " Using configuration from the classpath.");

				hadoopConfig = new org.apache.hadoop.conf.Configuration();
			}

			// -- (2) get the Hadoop file system class for that scheme
			final org.apache.hadoop.fs.FileSystem hadoopFs = org.apache.hadoop.fs.FileSystem.get(fsUri, hadoopConfig);

			HadoopFileSystem fs = new HadoopFileSystem(hadoopFs);

			// create the Flink file system, optionally limiting the open connections
			if (flinkConfig != null) {
				return limitIfConfigured(fs, scheme, flinkConfig);
			}
			else {
				return fs;
			}
		}
		catch (LinkageError e) {
			throw new UnsupportedFileSystemSchemeException("Cannot support file system for '" + fsUri.getScheme() +
					"' via Hadoop, because Hadoop is not in the classpath, or some classes " +
					"are missing from the classpath.", e);
		}
		catch (IOException e) {
			throw e;
		}
		catch (Exception e) {
			throw new IOException("Cannot instantiate file system for URI: " + fsUri, e);
		}
	}

	private static FileSystem limitIfConfigured(HadoopFileSystem fs, String scheme, Configuration config) {
		final ConnectionLimitingSettings limitSettings = ConnectionLimitingSettings.fromConfig(config, scheme);

		// decorate only if any limit is configured
		if (limitSettings == null) {
			// no limit configured
			return fs;
		}
		else {
			return new LimitedConnectionsFileSystem(
					fs,
					limitSettings.limitTotal,
					limitSettings.limitOutput,
					limitSettings.limitInput,
					limitSettings.streamOpenTimeout,
					limitSettings.streamInactivityTimeout);
		}
	}
}
