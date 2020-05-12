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

package org.apache.flink.client.python;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.client.program.ProgramAbortException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.entrypoint.parser.CommandLineParser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py4j.GatewayServer;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

/**
 * A main class used to launch Python applications. It executes python as a
 * subprocess and then has it connect back to the JVM to access system properties, etc.
 */
public final class PythonDriver {
	private static final Logger LOG = LoggerFactory.getLogger(PythonDriver.class);

	public static void main(String[] args) throws ExecutionException, InterruptedException {
		// The python job needs at least 2 args.
		// e.g. py a.py [user args]
		// e.g. pym a.b [user args]
		if (args.length < 2) {
			LOG.error("Required at least two arguments, only python file or python module is available.");
			System.exit(1);
		}

		// parse args
		final CommandLineParser<PythonDriverOptions> commandLineParser = new CommandLineParser<>(
			new PythonDriverOptionsParserFactory());
		PythonDriverOptions pythonDriverOptions = null;
		try {
			pythonDriverOptions = commandLineParser.parse(args);
		} catch (Exception e) {
			LOG.error("Could not parse command line arguments {}.", args, e);
			commandLineParser.printHelp(PythonDriver.class.getSimpleName());
			System.exit(1);
		}

		// Get configuration from ContextEnvironment/OptimizerPlanEnvironment. As the configurations of
		// streaming and batch environments are always set at the same time, for streaming jobs we can
		// also get its configuration from batch environments.
		Configuration config = ExecutionEnvironment.getExecutionEnvironment().getConfiguration();

		// start gateway server
		GatewayServer gatewayServer = PythonEnvUtils.startGatewayServer();
		PythonEnvUtils.setGatewayServer(gatewayServer);

		// commands which will be exec in python progress.
		final List<String> commands = constructPythonCommands(pythonDriverOptions);
		try {
			// prepare the exec environment of python progress.
			String tmpDir = System.getProperty("java.io.tmpdir") +
				File.separator + "pyflink" + File.separator + UUID.randomUUID();
			// start the python process.
			Process pythonProcess = PythonEnvUtils.launchPy4jPythonClient(
				gatewayServer,
				config,
				commands,
				pythonDriverOptions.getEntryPointScript().orElse(null),
				tmpDir);
			int exitCode = pythonProcess.waitFor();
			if (exitCode != 0) {
				throw new RuntimeException("Python process exits with code: " + exitCode);
			}
		} catch (Throwable e) {
			LOG.error("Run python process failed", e);

			// throw ProgramAbortException if the caller is interested in the program plan,
			// there is no harm to throw ProgramAbortException even if it is not the case.
			throw new ProgramAbortException();
		} finally {
			PythonEnvUtils.setGatewayServer(null);
			gatewayServer.shutdown();
		}
	}

	/**
	 * Constructs the Python commands which will be executed in python process.
	 *
	 * @param pythonDriverOptions parsed Python command options
	 */
	static List<String> constructPythonCommands(final PythonDriverOptions pythonDriverOptions) {
		final List<String> commands = new ArrayList<>();
		if (pythonDriverOptions.getEntryPointScript().isPresent()) {
			commands.add(pythonDriverOptions.getEntryPointScript().get());
		} else {
			commands.add("-m");
			commands.add(pythonDriverOptions.getEntryPointModule());
		}
		commands.addAll(pythonDriverOptions.getProgramArgs());
		return commands;
	}
}
