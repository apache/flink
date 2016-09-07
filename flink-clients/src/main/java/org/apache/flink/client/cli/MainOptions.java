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
package org.apache.flink.client.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;

import static org.apache.flink.client.cli.CliFrontendParser.CONFIGDIR_OPTION;

public class MainOptions {

	private final String configDir;

	private final Option[] options;

	public MainOptions(CommandLine line) throws CliArgsException {

		if (line.hasOption(CONFIGDIR_OPTION.getLongOpt())) {
			configDir = line.getOptionValue(CONFIGDIR_OPTION.getLongOpt());
		} else {
			configDir = null;
		}

		this.options = line.getOptions();
	}

	public Option[] getOptions() {
		return options == null ? new Option[0] : options;
	}


	public String getConfigDir(){
		return configDir;
	}
}
