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

package org.apache.flink.fs.s3.common;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

/**
 * Base class for file system factories that create S3 file systems.
 */
public abstract class AbstractS3FileSystemFactory implements FileSystemFactory {

	/**
	 * The substring to be replaced by random entropy in checkpoint paths.
	 */
	public static final ConfigOption<String> ENTROPY_INJECT_KEY_OPTION = ConfigOptions
			.key("s3.entropy.key")
			.noDefaultValue()
			.withDescription(
					"This option can be used to improve performance due to sharding issues on Amazon S3. " +
					"For file creations with entropy injection, this key will be replaced by random " +
					"alphanumeric characters. For other file creations, the key will be filtered out.");

	/**
	 * The number of entropy characters, in case entropy injection is configured.
	 */
	public static final ConfigOption<Integer> ENTROPY_INJECT_LENGTH_OPTION = ConfigOptions
			.key("s3.entropy.length")
			.defaultValue(4)
			.withDescription(
					"When '" + ENTROPY_INJECT_KEY_OPTION.key() + "' is set, this option defines the number of " +
					"random characters to replace the entropy key with.");

	// ------------------------------------------------------------------------

	private static final String INVALID_ENTROPY_KEY_CHARS = "^.*[~#@*+%{}<>\\[\\]|\"\\\\].*$";

	private static final Logger LOG = LoggerFactory.getLogger(AbstractS3FileSystemFactory.class);

	/** Name of this factory for logging. */
	private final String name;

	private final HadoopConfigLoader hadoopConfigLoader;

	private Configuration flinkConfig;

	protected AbstractS3FileSystemFactory(String name, HadoopConfigLoader hadoopConfigLoader) {
		this.name = name;
		this.hadoopConfigLoader = hadoopConfigLoader;
	}

	// ------------------------------------------------------------------------

	@Override
	public void configure(Configuration config) {
		flinkConfig = config;
		hadoopConfigLoader.setFlinkConfig(config);
	}

	@Override
	public FileSystem create(URI fsUri) throws IOException {
		Configuration flinkConfig = this.flinkConfig;

		if (flinkConfig == null) {
			LOG.warn("Creating S3 FileSystem without configuring the factory. All behavior will be default.");
			flinkConfig = new Configuration();
		}

		LOG.debug("Creating S3 file system backed by {}", name);
		LOG.debug("Loading Hadoop configuration for {}", name);

		try {
			// create the Hadoop FileSystem
			org.apache.hadoop.conf.Configuration hadoopConfig = hadoopConfigLoader.getOrLoadHadoopConfig();
			org.apache.hadoop.fs.FileSystem fs = createHadoopFileSystem();
			fs.initialize(getInitURI(fsUri, hadoopConfig), hadoopConfig);

			// load the entropy injection settings
			String entropyInjectionKey = flinkConfig.getString(ENTROPY_INJECT_KEY_OPTION);
			int numEntropyChars = -1;
			if (entropyInjectionKey != null) {
				if (entropyInjectionKey.matches(INVALID_ENTROPY_KEY_CHARS)) {
					throw new IllegalConfigurationException("Invalid character in value for " +
							ENTROPY_INJECT_KEY_OPTION.key() + " : " + entropyInjectionKey);
				}
				numEntropyChars = flinkConfig.getInteger(ENTROPY_INJECT_LENGTH_OPTION);
				if (numEntropyChars <= 0) {
					throw new IllegalConfigurationException(
							ENTROPY_INJECT_LENGTH_OPTION.key() + " must configure a value > 0");
				}
			}

			return new FlinkS3FileSystem(fs, entropyInjectionKey, numEntropyChars);
		}
		catch (IOException e) {
			throw e;
		}
		catch (Exception e) {
			throw new IOException(e.getMessage(), e);
		}
	}

	protected abstract org.apache.hadoop.fs.FileSystem createHadoopFileSystem();

	protected abstract URI getInitURI(
		URI fsUri, org.apache.hadoop.conf.Configuration hadoopConfig);
}

