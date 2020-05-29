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

package org.apache.flink.yarn.configuration;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;

import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A class with utilities for setting the log config file.
 */
@Internal
public class YarnLogConfigUtil {

	private static final Logger LOG = LoggerFactory.getLogger(YarnLogConfigUtil.class);

	public static final String CONFIG_FILE_LOGBACK_NAME = "logback.xml";
	public static final String CONFIG_FILE_LOG4J_NAME = "log4j.properties";

	@VisibleForTesting
	public static Configuration setLogConfigFileInConfig(
			final Configuration configuration,
			final String configurationDirectory) {

		if (configuration.get(YarnConfigOptionsInternal.APPLICATION_LOG_CONFIG_FILE) != null) {
			return configuration;
		}

		discoverLogConfigFile(configurationDirectory).ifPresent(file ->
				configuration.set(YarnConfigOptionsInternal.APPLICATION_LOG_CONFIG_FILE, file.getPath()));
		return configuration;
	}

	private static Optional<File> discoverLogConfigFile(final String configurationDirectory) {
		Optional<File> logConfigFile = Optional.empty();

		final File log4jFile = new File(configurationDirectory + File.separator + CONFIG_FILE_LOG4J_NAME);
		if (log4jFile.exists()) {
			logConfigFile = Optional.of(log4jFile);
		}

		final File logbackFile = new File(configurationDirectory + File.separator + CONFIG_FILE_LOGBACK_NAME);
		if (logbackFile.exists()) {
			if (logConfigFile.isPresent()) {
				LOG.warn("The configuration directory ('" + configurationDirectory + "') already contains a LOG4J config file." +
						"If you want to use logback, then please delete or rename the log configuration file.");
			} else {
				logConfigFile = Optional.of(logbackFile);
			}
		}
		return logConfigFile;
	}

	public static String getLoggingYarnCommand(final Configuration configuration) {
		checkNotNull(configuration);

		final String logConfigFilePath = configuration.getString(YarnConfigOptionsInternal.APPLICATION_LOG_CONFIG_FILE);
		if (logConfigFilePath == null) {
			return "";
		}

		String logCommand = getLog4jCommand(logConfigFilePath);
		if (logCommand.isEmpty()) {
			logCommand = getLogBackCommand(logConfigFilePath);
		}
		return logCommand;
	}

	private static String getLogBackCommand(final String logConfigFilePath) {
		final boolean hasLogback = logConfigFilePath.endsWith(CONFIG_FILE_LOGBACK_NAME);
		if (!hasLogback) {
			return "";
		}

		return new StringBuilder("-Dlog.file=\"" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/jobmanager.log\"")
				.append(" -Dlogback.configurationFile=file:" + CONFIG_FILE_LOGBACK_NAME)
				.toString();
	}

	private static String getLog4jCommand(final String logConfigFilePath) {
		final boolean hasLog4j = logConfigFilePath.endsWith(CONFIG_FILE_LOG4J_NAME);
		if (!hasLog4j) {
			return "";
		}

		return new StringBuilder("-Dlog.file=\"" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/jobmanager.log\"")
				.append(" -Dlog4j.configuration=file:" + CONFIG_FILE_LOG4J_NAME)
				.append(" -Dlog4j.configurationFile=file:" + CONFIG_FILE_LOG4J_NAME)
				.toString();
	}
}
