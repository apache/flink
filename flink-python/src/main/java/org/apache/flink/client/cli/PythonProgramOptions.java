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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.python.util.PythonDependencyUtils;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.flink.client.cli.CliFrontendParser.ARGS_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.PYMODULE_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.PY_OPTION;
import static org.apache.flink.client.cli.ProgramOptionsUtils.isPythonEntryPoint;

/**
 * The class for command line options that refer to a Python program or JAR program with Python command line options.
 */
public class PythonProgramOptions extends ProgramOptions {

	private final Configuration pythonConfiguration;

	private final boolean isPythonEntryPoint;

	public PythonProgramOptions(CommandLine line) throws CliArgsException {
		super(line);
		isPythonEntryPoint = isPythonEntryPoint(line);
		pythonConfiguration = PythonDependencyUtils.parsePythonDependencyConfiguration(line);
		// If the job is Python Shell job, the entry point class name is PythonGateWayServer.
		// Otherwise, the entry point class of python job is PythonDriver
		if (isPythonEntryPoint && entryPointClass == null) {
			entryPointClass = "org.apache.flink.client.python.PythonDriver";
		}
	}

	@Override
	protected String[] extractProgramArgs(CommandLine line) {
		String[] args;
		if (isPythonEntryPoint(line)) {
			String[] rawArgs = line.hasOption(ARGS_OPTION.getOpt()) ?
				line.getOptionValues(ARGS_OPTION.getOpt()) :
				line.getArgs();
			// copy python related parameters to program args and place them in front of user parameters
			List<String> pyArgList = new ArrayList<>();
			Set<Option> pyOptions = new HashSet<>();
			pyOptions.add(PY_OPTION);
			pyOptions.add(PYMODULE_OPTION);
			for (Option option : line.getOptions()) {
				if (pyOptions.contains(option)) {
					pyArgList.add("--" + option.getLongOpt());
					pyArgList.add(option.getValue());
				}
			}
			String[] newArgs = pyArgList.toArray(new String[rawArgs.length + pyArgList.size()]);
			System.arraycopy(rawArgs, 0, newArgs, pyArgList.size(), rawArgs.length);
			args = newArgs;
		} else {
			args = super.extractProgramArgs(line);
		}

		return args;
	}

	@Override
	public void validate() throws CliArgsException {
		if (!isPythonEntryPoint) {
			super.validate();
		}
	}

	@Override
	public void applyToConfiguration(Configuration configuration) {
		super.applyToConfiguration(configuration);
		configuration.addAll(pythonConfiguration);
	}
}
