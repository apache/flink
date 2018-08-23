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

package org.apache.flink.fs.s3presto;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemFactory;
import org.apache.flink.runtime.fs.hdfs.HadoopConfigLoader;
import org.apache.flink.util.FlinkRuntimeException;

import com.facebook.presto.hive.PrestoS3FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Simple factory for the S3 file system.
 */
public class S3FileSystemFactory implements FileSystemFactory {

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

	private static final Logger LOG = LoggerFactory.getLogger(S3FileSystemFactory.class);

	private static final Set<String> PACKAGE_PREFIXES_TO_SHADE =
		new HashSet<>(Collections.singletonList("com.amazonaws."));

	private static final Set<String> CONFIG_KEYS_TO_SHADE =
		Collections.unmodifiableSet(new HashSet<>(Collections.singleton("presto.s3.credentials-provider")));

	private static final String FLINK_SHADING_PREFIX = "org.apache.flink.fs.s3presto.shaded.";

	private static final String[] FLINK_CONFIG_PREFIXES = { "s3.", "presto.s3." };

	private static final String[][] MIRRORED_CONFIG_KEYS = {
			{ "presto.s3.access.key", "presto.s3.access-key" },
			{ "presto.s3.secret.key", "presto.s3.secret-key" }
	};

	private static final String INVALID_ENTROPY_KEY_CHARS = "^.*[~#@*+%{}<>\\[\\]|\"\\\\].*$";

	private final HadoopConfigLoader hadoopConfigLoader = createHadoopConfigLoader();

	private Configuration flinkConfig;

	@Override
	public void configure(Configuration config) {
		flinkConfig = config;
		hadoopConfigLoader.setFlinkConfig(config);
	}

	@Override
	public FileSystem create(URI fsUri) throws IOException {
		LOG.debug("Creating S3 FileSystem backed by Presto S3 FileSystem");
		LOG.debug("Loading Hadoop configuration for Presto S3 File System");

		try {
			// instantiate the presto file system
			org.apache.hadoop.conf.Configuration hadoopConfig = hadoopConfigLoader.getOrLoadHadoopConfig();
			org.apache.hadoop.fs.FileSystem fs = new PrestoS3FileSystem();
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

			return new S3PrestoFileSystem(fs, entropyInjectionKey, numEntropyChars);
		}
		catch (IOException e) {
			throw e;
		}
		catch (Exception e) {
			throw new IOException(e.getMessage(), e);
		}
	}

	@Override
	public String getScheme() {
		return "s3";
	}

	@VisibleForTesting
	static HadoopConfigLoader createHadoopConfigLoader() {
		return new HadoopConfigLoader(FLINK_CONFIG_PREFIXES, MIRRORED_CONFIG_KEYS,
			"presto.s3.", PACKAGE_PREFIXES_TO_SHADE, CONFIG_KEYS_TO_SHADE, FLINK_SHADING_PREFIX);
	}

	static URI getInitURI(URI fsUri, org.apache.hadoop.conf.Configuration hadoopConfig) {
		final String scheme = fsUri.getScheme();
		final String authority = fsUri.getAuthority();
		final URI initUri;

		if (scheme == null && authority == null) {
			initUri = createURI("s3://s3.amazonaws.com");
		}
		else if (scheme != null && authority == null) {
			initUri = createURI(scheme + "://s3.amazonaws.com");
		}
		else {
			initUri = fsUri;
		}
		return initUri;
	}

	static URI createURI(String str) {
		try {
			return new URI(str);
		}
		catch (URISyntaxException e) {
			throw new FlinkRuntimeException("Error in s3 aws URI - " + str, e);
		}
	}
}
