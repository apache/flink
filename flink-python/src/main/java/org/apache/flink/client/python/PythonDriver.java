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
import org.apache.flink.client.cli.CliArgsException;
import org.apache.flink.client.cli.CliFrontendParser;
import org.apache.flink.client.program.ProgramAbortException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.python.util.PythonDependencyUtils;
import org.apache.flink.runtime.entrypoint.FlinkParseException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py4j.GatewayServer;

import java.io.File;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * A main class used to launch Python applications. It executes python as a
 * subprocess and then has it connect back to the JVM to access system properties, etc.
 */
public final class PythonDriver {
	private static final Logger LOG = LoggerFactory.getLogger(PythonDriver.class);

	public static void main(String[] args) throws CliArgsException {
		Configuration config = new Configuration(ExecutionEnvironment.getExecutionEnvironment().getConfiguration());
		CommandLine line = CliFrontendParser.parse(PythonDependencyUtils.getPythonCommandLineOptions(), args, false);
		config.addAll(PythonDependencyUtils.parseCommandLine(line));

		List<String> commands = null;
		try {
			// commands which will be exec in python progress.
			commands = constructPythonCommands(config, line.getArgList());
		} catch (Exception e) {
			LOG.error("Could not construct python command from config {}.", config, e);
			HelpFormatter helpFormatter = new HelpFormatter();
			helpFormatter.setLeftPadding(5);
			helpFormatter.setWidth(80);
			helpFormatter.printHelp(
				PythonDriver.class.getSimpleName(),
				PythonDependencyUtils.getPythonCommandLineOptions(),
				true);
			System.exit(1);
		}

		// start gateway server
		GatewayServer gatewayServer = startGatewayServer();
		// prepare python env
		try {
			// prepare the exec environment of python progress.
			String tmpDir = System.getProperty("java.io.tmpdir") +
				File.separator + "pyflink" + File.separator + UUID.randomUUID();
			PythonDriverEnvUtils.PythonEnvironment pythonEnv = PythonDriverEnvUtils.preparePythonEnvironment(
				config, tmpDir);
			// set env variable PYFLINK_GATEWAY_PORT for connecting of python gateway in python progress.
			pythonEnv.systemEnv.put("PYFLINK_GATEWAY_PORT", String.valueOf(gatewayServer.getListeningPort()));
			// start the python process.
			Process pythonProcess = PythonDriverEnvUtils.startPythonProcess(pythonEnv, commands);
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
			gatewayServer.shutdown();
		}
	}

	/**
	 * Creates a GatewayServer run in a daemon thread.
	 *
	 * @return The created GatewayServer
	 */
	static GatewayServer startGatewayServer() {
		InetAddress localhost = InetAddress.getLoopbackAddress();
		GatewayServer gatewayServer = new GatewayServer.GatewayServerBuilder()
			.javaPort(0)
			.javaAddress(localhost)
			.build();
		Thread thread = new Thread(gatewayServer::start);
		thread.setName("py4j-gateway");
		thread.setDaemon(true);
		thread.start();
		try {
			thread.join();
		} catch (InterruptedException e) {
			LOG.error("The gateway server thread join failed.", e);
			System.exit(1);
		}
		return gatewayServer;
	}

	/**
	 * Constructs the Python commands which will be executed in python process.
	 *
	 * @param config The config object which contains python configurations.
	 * @param userArgs The user args.
	 */
	static List<String> constructPythonCommands(final Configuration config, List<String> userArgs)
			throws FlinkParseException {
		final String entryPointModule;
		if (config.contains(PythonDependencyUtils.PYTHON_ENTRY_POINT_MODULE) &&
			config.contains(PythonDependencyUtils.PYTHON_ENTRY_POINT_SCRIPT)) {
			throw new FlinkParseException("Cannot use options -py and -pym simultaneously.");
		} else if (config.contains(PythonDependencyUtils.PYTHON_ENTRY_POINT_SCRIPT)) {
			Path file = new Path(config.get(PythonDependencyUtils.PYTHON_ENTRY_POINT_SCRIPT));
			String fileName = file.getName();
			if (fileName.endsWith(".py")) {
				entryPointModule = fileName.substring(0, fileName.length() - 3);
			} else {
				throw new FlinkParseException("The entry point script is not end with \".py\".");
			}
		} else if (config.contains(PythonDependencyUtils.PYTHON_ENTRY_POINT_MODULE)) {
			entryPointModule = config.get(PythonDependencyUtils.PYTHON_ENTRY_POINT_MODULE);
		} else {
			throw new FlinkParseException("The Python entrypoint has not been specified. It can be specified with " +
				"options -py or -pym");
		}

		final List<String> commands = new ArrayList<>();
		commands.add("-m");
		commands.add(entryPointModule);
		commands.addAll(userArgs);
		return commands;
	}
}
