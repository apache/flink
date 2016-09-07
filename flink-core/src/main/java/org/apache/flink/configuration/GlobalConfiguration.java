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

package org.apache.flink.configuration;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.flink.annotation.Internal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Global configuration object for Flink. Similar to Java properties configuration
 * objects it includes key-value pairs which represent the framework's configuration.
 */
@Internal
public final class GlobalConfiguration {

	private static final Logger LOG = LoggerFactory.getLogger(GlobalConfiguration.class);

	public static final String FLINK_CONF_FILENAME = "flink-conf.yaml";

	// --------------------------------------------------------------------------------------------

	private GlobalConfiguration() {}

	// --------------------------------------------------------------------------------------------

	/**
	 * Loads the global configuration from the environment. Fails if an error occurs during loading. Returns an
	 * empty configuration object if the environment variable is not set. In production this variable is set but
	 * tests and local execution/debugging don't have this environment variable set. That's why we should fail
	 * if it is not set.
	 * @return Returns the Configuration
	 */
	public static Configuration loadConfiguration() {
		final String configDir = System.getenv(ConfigConstants.ENV_FLINK_CONF_DIR);
		if (configDir == null) {
			return new Configuration();
		}
		return loadConfiguration(configDir);
	}

	/**
	 * Loads the configuration files from the specified directory.
	 * <p>
	 * YAML files are supported as configuration files.
	 * 
	 * @param configDir
	 *        the directory which contains the configuration files
	 */
	public static Configuration loadConfiguration(final String configDir) {

		if (configDir == null) {
			throw new IllegalArgumentException("Given configuration directory is null, cannot load configuration");
		}

		final File confDirFile = new File(configDir);
		if (!(confDirFile.exists())) {
			throw new IllegalConfigurationException(
				"The given configuration directory name '" + configDir +
					"' (" + confDirFile.getAbsolutePath() + ") does not describe an existing directory.");
		}

		// get Flink yaml configuration file
		final File yamlConfigFile = new File(confDirFile, FLINK_CONF_FILENAME);

		if (!yamlConfigFile.exists()) {
			throw new IllegalConfigurationException(
				"The Flink config file '" + yamlConfigFile +
					"' (" + confDirFile.getAbsolutePath() + ") does not exist.");
		}

		return loadYAMLResource(yamlConfigFile);
	}

	/**
	 * Loads a YAML-file of key-value pairs.
	 * <p>
	 * Colon and whitespace ": " separate key and value (one per line). The hash tag "#" starts a single-line comment.
	 * <p>
	 * Example:
	 * 
	 * <pre>
	 * jobmanager.rpc.address: localhost # network address for communication with the job manager
	 * jobmanager.rpc.port   : 6123      # network port to connect to for communication with the job manager
	 * taskmanager.rpc.port  : 6122      # network port the task manager expects incoming IPC connections
	 * </pre>
	 * <p>
	 * This does not span the whole YAML specification, but only the *syntax* of simple YAML key-value pairs (see issue
	 * #113 on GitHub). If at any point in time, there is a need to go beyond simple key-value pairs syntax
	 * compatibility will allow to introduce a YAML parser library.
	 * 
	 * @param file the YAML file to read from
	 * @see <a href="http://www.yaml.org/spec/1.2/spec.html">YAML 1.2 specification</a>
	 */
	private static Configuration loadYAMLResource(File file) {
		final Configuration config = new Configuration();

		try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)))){

			String line;
			while ((line = reader.readLine()) != null) {

				// 1. check for comments
				String[] comments = line.split("#", 2);
				String conf = comments[0];

				// 2. get key and value
				if (conf.length() > 0) {
					String[] kv = conf.split(": ", 2);

					// skip line with no valid key-value pair
					if (kv.length == 1) {
						LOG.warn("Error while trying to split key and value in configuration file " + file + ": " + line);
						continue;
					}

					String key = kv[0].trim();
					String value = kv[1].trim();

					// sanity check
					if (key.length() == 0 || value.length() == 0) {
						LOG.warn("Error after splitting key and value in configuration file " + file + ": " + line);
						continue;
					}

					LOG.debug("Loading configuration property: {}, {}", key, value);
					config.setString(key, value);
				}
			}
		} catch (IOException e) {
			throw new RuntimeException("Error parsing YAML configuration.", e);
		}

		return config;
	}

}
