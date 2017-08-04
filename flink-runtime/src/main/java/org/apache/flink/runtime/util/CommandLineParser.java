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

package org.apache.flink.runtime.util;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.ParseArgsException;
import org.apache.flink.runtime.clusterframework.BootstrapTools;

/**
 * Command line arguments parser
 */
public class CommandLineParser {
	private static final Option CONFIG_DIR_OPTION = new Option("c", "configDir", true, "Flink config file directory");

	private static final Options CONFIG_OPTIONS = getConfigOptions();

	private final CommandLine cmd;
	private final Configuration dynamicProperties;

	private CommandLineParser(CommandLine commandLine) {
		cmd = commandLine;
		dynamicProperties = BootstrapTools.parseDynamicProperties(cmd);
	}

	public static CommandLineParser parse(String[] args) throws ParseArgsException {
		DefaultParser parser = new DefaultParser();
		try {
			return new CommandLineParser(parser.parse(CONFIG_OPTIONS, args, true));
		} catch (ParseException e) {
			throw new ParseArgsException(e.getMessage(), e);
		}
	}

	public String getConfigDir() {
		return cmd.getOptionValue(CONFIG_DIR_OPTION.getOpt());
	}

	public String getConfigDir(String defaultValue) {
		return cmd.getOptionValue(CONFIG_DIR_OPTION.getOpt(), defaultValue);
	}

	public Configuration getDynamicProperties() {
		return dynamicProperties;
	}

	private static Options getConfigOptions() {
		Options options = new Options();
		options.addOption(CONFIG_DIR_OPTION);
		options.addOption(BootstrapTools.newDynamicPropertiesOption());

		return options;
	}
}
