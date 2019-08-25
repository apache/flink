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

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.client.cli.CliFrontendParser.ARGS_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.CLASSPATH_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.CLASS_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.DETACHED_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.JAR_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.LOGGING_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.PARALLELISM_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.PYFILES_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.PYMODULE_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.PY_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.SHUTDOWN_IF_ATTACHED_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.YARN_DETACHED_OPTION;

/**
 * Base class for command line options that refer to a JAR file program.
 */
public abstract class ProgramOptions extends CommandLineOptions {

	private final String jarFilePath;

	private final String entryPointClass;

	private final List<URL> classpaths;

	private final String[] programArgs;

	private final int parallelism;

	private final boolean stdoutLogging;

	private final boolean detachedMode;

	private final boolean shutdownOnAttachedExit;

	private final SavepointRestoreSettings savepointSettings;

	/**
	 * Flag indicating whether the job is a Python job.
	 */
	private final boolean isPython;

	protected ProgramOptions(CommandLine line) throws CliArgsException {
		super(line);

		String[] args = line.hasOption(ARGS_OPTION.getOpt()) ?
			line.getOptionValues(ARGS_OPTION.getOpt()) :
			line.getArgs();

		this.entryPointClass = line.hasOption(CLASS_OPTION.getOpt()) ?
			line.getOptionValue(CLASS_OPTION.getOpt()) : null;

		isPython = line.hasOption(PY_OPTION.getOpt()) | line.hasOption(PYMODULE_OPTION.getOpt())
			| "org.apache.flink.client.python.PythonGatewayServer".equals(entryPointClass);
		// If specified the option -py(--python)
		if (line.hasOption(PY_OPTION.getOpt())) {
			// Cannot use option -py and -pym simultaneously.
			if (line.hasOption(PYMODULE_OPTION.getOpt())) {
				throw new CliArgsException("Cannot use option -py and -pym simultaneously.");
			}
			// The cli cmd args which will be transferred to PythonDriver will be transformed as follows:
			// CLI cmd : -py ${python.py} pyfs [optional] ${py-files} [optional] ${other args}.
			// PythonDriver args: py ${python.py} [optional] pyfs [optional] ${py-files} [optional] ${other args}.
			// -------------------------------transformed-------------------------------------------------------
			// e.g. -py wordcount.py(CLI cmd) -----------> py wordcount.py(PythonDriver args)
			// e.g. -py wordcount.py -pyfs file:///AAA.py,hdfs:///BBB.py --input in.txt --output out.txt(CLI cmd)
			// 	-----> -py wordcount.py -pyfs file:///AAA.py,hdfs:///BBB.py --input in.txt --output out.txt(PythonDriver args)
			String[] newArgs;
			int argIndex;
			if (line.hasOption(PYFILES_OPTION.getOpt())) {
				newArgs = new String[args.length + 4];
				newArgs[2] = "-" + PYFILES_OPTION.getOpt();
				newArgs[3] = line.getOptionValue(PYFILES_OPTION.getOpt());
				argIndex = 4;
			} else {
				newArgs = new String[args.length + 2];
				argIndex = 2;
			}
			newArgs[0] = "-" + PY_OPTION.getOpt();
			newArgs[1] = line.getOptionValue(PY_OPTION.getOpt());
			System.arraycopy(args, 0, newArgs, argIndex, args.length);
			args = newArgs;
		}

		// If specified the option -pym(--pyModule)
		if (line.hasOption(PYMODULE_OPTION.getOpt())) {
			// If you specify the option -pym, you should specify the option --pyFiles simultaneously.
			if (!line.hasOption(PYFILES_OPTION.getOpt())) {
				throw new CliArgsException("-pym must be used in conjunction with `--pyFiles`");
			}
			// The cli cmd args which will be transferred to PythonDriver will be transformed as follows:
			// CLI cmd : -pym ${py-module} -pyfs ${py-files} [optional] ${other args}.
			// PythonDriver args: -pym ${py-module} -pyfs ${py-files} [optional] ${other args}.
			// e.g. -pym AAA.fun -pyfs AAA.zip(CLI cmd) ----> -pym AAA.fun -pyfs AAA.zip(PythonDriver args)
			String[] newArgs = new String[args.length + 4];
			newArgs[0] = "-" + PYMODULE_OPTION.getOpt();
			newArgs[1] = line.getOptionValue(PYMODULE_OPTION.getOpt());
			newArgs[2] = "-" + PYFILES_OPTION.getOpt();
			newArgs[3] = line.getOptionValue(PYFILES_OPTION.getOpt());
			System.arraycopy(args, 0, newArgs, 4, args.length);
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

		stdoutLogging = !line.hasOption(LOGGING_OPTION.getOpt());
		detachedMode = line.hasOption(DETACHED_OPTION.getOpt()) || line.hasOption(
			YARN_DETACHED_OPTION.getOpt());
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

	public boolean getStdoutLogging() {
		return stdoutLogging;
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
