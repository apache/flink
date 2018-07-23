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
import org.apache.flink.runtime.util.HadoopUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/** This class lazily loads hadoop configuration from resettable Flink's configuration. */
public class HadoopConfigLoader {
	private static final Logger LOG = LoggerFactory.getLogger(HadoopConfigLoader.class);

	private static final Set<String> PACKAGE_PREFIXES_TO_SHADE =
		new HashSet<>(Collections.singletonList("com.amazonaws."));

	/** The prefixes that Flink adds to the Hadoop fs config. */
	private final String[] flinkConfigPrefixes;

	/** Keys that are replaced (after prefix replacement, to give a more uniform experience
	 * across different file system implementations. */
	private final String[][] mirroredConfigKeys;

	/** Hadoop config prefix to replace Flink prefix. */
	private final String hadoopConfigPrefix;

	private final Set<String> configKeysToShade;
	private final String flinkShadingPrefix;

	/** Flink's configuration object. */
	private Configuration flinkConfig;

	/** Hadoop's configuration for the file systems, lazily initialized. */
	private org.apache.hadoop.conf.Configuration hadoopConfig;

	public HadoopConfigLoader(
		@Nonnull String[] flinkConfigPrefixes,
		@Nonnull String[][] mirroredConfigKeys,
		@Nonnull String hadoopConfigPrefix,
		@Nonnull Set<String> configKeysToShade,
		@Nonnull String flinkShadingPrefix) {
		this.flinkConfigPrefixes = flinkConfigPrefixes;
		this.mirroredConfigKeys = mirroredConfigKeys;
		this.hadoopConfigPrefix = hadoopConfigPrefix;
		this.configKeysToShade = configKeysToShade;
		this.flinkShadingPrefix = flinkShadingPrefix;
	}

	public void setFlinkConfig(Configuration config) {
		flinkConfig = config;
		hadoopConfig = null;
	}

	/** get the loaded Hadoop config (or fall back to one loaded from the classpath). */
	public org.apache.hadoop.conf.Configuration getOrLoadHadoopConfig() {
		org.apache.hadoop.conf.Configuration hadoopConfig = this.hadoopConfig;
		if (hadoopConfig == null) {
			if (flinkConfig != null) {
				hadoopConfig = mirrorCertianHadoopConfig(loadHadoopConfigFromFlink());
			}
			else {
				LOG.warn("The factory has not been configured prior to loading the S3 file system."
					+ " Using Hadoop configuration from the classpath.");
				hadoopConfig = new org.apache.hadoop.conf.Configuration();
			}
		}
		this.hadoopConfig = hadoopConfig;
		return hadoopConfig;
	}

	// add additional config entries from the Flink config to the Hadoop config
	private org.apache.hadoop.conf.Configuration loadHadoopConfigFromFlink() {
		org.apache.hadoop.conf.Configuration hadoopConfig = HadoopUtils.getHadoopConfiguration(flinkConfig);
		for (String key : flinkConfig.keySet()) {
			for (String prefix : flinkConfigPrefixes) {
				if (key.startsWith(prefix)) {
					String value = flinkConfig.getString(key, null);
					String newKey = hadoopConfigPrefix + key.substring(prefix.length());
					String newValue = fixHadoopConfig(key, flinkConfig.getString(key, null));
					hadoopConfig.set(newKey, newValue);

					LOG.debug("Adding Flink config entry for {} as {}={} to Hadoop config", key, newKey, value);
				}
			}
		}
		return hadoopConfig;
	}

	// mirror certain keys to make use more uniform across implementations
	// with different keys
	private org.apache.hadoop.conf.Configuration mirrorCertianHadoopConfig(
		org.apache.hadoop.conf.Configuration hadoopConfig) {
		for (String[] mirrored : mirroredConfigKeys) {
			String value = hadoopConfig.get(mirrored[0], null);
			if (value != null) {
				hadoopConfig.set(mirrored[1], value);
			}
		}
		return hadoopConfig;
	}

	private String fixHadoopConfig(String key, String value) {
		return key != null && configKeysToShade.contains(key) ?
			shadeAwsClassConfig(value) : value;
	}

	private String shadeAwsClassConfig(String awsCredProvider) {
		return PACKAGE_PREFIXES_TO_SHADE.stream().anyMatch(awsCredProvider::startsWith) ?
			flinkShadingPrefix + awsCredProvider : awsCredProvider;
	}
}
