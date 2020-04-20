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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.flink.client.cli.CliFrontendParser.ARGS_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.CLASSPATH_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.CLASS_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.DETACHED_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.JAR_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.PARALLELISM_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.PYARCHIVE_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.PYEXEC_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.PYFILES_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.PYMODULE_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.PYREQUIREMENTS_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.PY_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.SHUTDOWN_IF_ATTACHED_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.YARN_DETACHED_OPTION;

/**
 * Base class for command line options that refer to a JAR file program.
 */
public class ProgramOptions extends CommandLineOptions {

	private final String jarFilePath;

	private final String entryPointClass;

	private final List<URL> classpaths;

	private final String[] programArgs;

	private final int parallelism;

	private final boolean detachedMode;

	private final boolean shutdownOnAttachedExit;

	private final SavepointRestoreSettings savepointSettings;

	/**
	 * Flag indicating whether the job is a Python job.
	 */
	private final boolean isPython;

	public ProgramOptions(CommandLine line) throws CliArgsException {
		super(line);

		String[] args = line.hasOption(ARGS_OPTION.getOpt()) ?
			line.getOptionValues(ARGS_OPTION.getOpt()) :
			line.getArgs();

		this.entryPointClass = line.hasOption(CLASS_OPTION.getOpt()) ?
			line.getOptionValue(CLASS_OPTION.getOpt()) : null;

		isPython = line.hasOption(PY_OPTION.getOpt()) | line.hasOption(PYMODULE_OPTION.getOpt())
			| "org.apache.flink.client.python.PythonGatewayServer".equals(entryPointClass);
		if (isPython) {
			// copy python related parameters to program args and place them in front of user parameters
			List<String> pyArgList = new ArrayList<>();
			Set<Option> pyOptions = new HashSet<>();
			pyOptions.add(PY_OPTION);
			pyOptions.add(PYMODULE_OPTION);
			pyOptions.add(PYFILES_OPTION);
			pyOptions.add(PYREQUIREMENTS_OPTION);
			pyOptions.add(PYARCHIVE_OPTION);
			pyOptions.add(PYEXEC_OPTION);
			for (Option option: line.getOptions()) {
				if (pyOptions.contains(option)) {
					pyArgList.add("--" + option.getLongOpt());
					pyArgList.add(option.getValue());
				}
			}
			String[] newArgs = pyArgList.toArray(new String[args.length + pyArgList.size()]);
			System.arraycopy(args, 0, newArgs, pyArgList.size(), args.length);
			args = newArgs;
		}

		if (line.hasOption(JAR_OPTION.getOpt())) {
			this.jarFilePath = line.getOptionValue(JAR_OPTION.getOpt());
		} else if (!isPython && args.length > 0) {
			jarFilePath = args[0];
			args = Arrays.copyOfRange(args, 1, args.length);
		}
		else {
			jarFilePath = null;
		}

		this.programArgs = args;

		List<URL> classpaths = new ArrayList<URL>();
		if (line.hasOption(CLASSPATH_OPTION.getOpt())) {
			for (String path : line.getOptionValues(CLASSPATH_OPTION.getOpt())) {
				try {
					classpaths.add(new URL(path));
				} catch (MalformedURLException e) {
					throw new CliArgsException("Bad syntax for classpath: " + path);
				}
			}
		}
		this.classpaths = classpaths;

		if (line.hasOption(PARALLELISM_OPTION.getOpt())) {
			String parString = line.getOptionValue(PARALLELISM_OPTION.getOpt());
			try {
				parallelism = Integer.parseInt(parString);
				if (parallelism <= 0) {
					throw new NumberFormatException();
				}
			}
			catch (NumberFormatException e) {
				throw new CliArgsException("The parallelism must be a positive number: " + parString);
			}
		}
		else {
			parallelism = ExecutionConfig.PARALLELISM_DEFAULT;
		}

		detachedMode = line.hasOption(DETACHED_OPTION.getOpt()) || line.hasOption(YARN_DETACHED_OPTION.getOpt());
		shutdownOnAttachedExit = line.hasOption(SHUTDOWN_IF_ATTACHED_OPTION.getOpt());

		this.savepointSettings = CliFrontendParser.createSavepointRestoreSettings(line);
	}

	public String getJarFilePath() {
		return jarFilePath;
	}

	public String getEntryPointClassName() {
		return entryPointClass;
	}

	public List<URL> getClasspaths() {
		return classpaths;
	}

	public String[] getProgramArgs() {
		return programArgs;
	}

	public int getParallelism() {
		return parallelism;
	}

	public boolean getDetachedMode() {
		return detachedMode;
	}

	public boolean isShutdownOnAttachedExit() {
		return shutdownOnAttachedExit;
	}

	public SavepointRestoreSettings getSavepointRestoreSettings() {
		return savepointSettings;
	}

	/**
	 * Indicates whether the job is a Python job.
	 */
	public boolean isPython() {
		return isPython;
	}
}
