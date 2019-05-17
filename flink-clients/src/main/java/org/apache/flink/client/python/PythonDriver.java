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

import org.apache.flink.core.fs.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py4j.GatewayServer;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A main class used to launch Python applications. It executes python as a
 * subprocess and then has it connect back to the JVM to access system properties, etc.
 */
public class PythonDriver {
	private static final Logger LOG = LoggerFactory.getLogger(PythonDriver.class);

	public static void main(String[] args) {
		// the python job needs at least 2 args.
		// e.g. python a.py
		// e.g. py-module a.b
		if (args.length < 2) {
			LOG.error("Args is invalid");
			System.exit(1);
		}
		Map<String, List<String>> parseArgs = new HashMap<>();
		// parse args
		parseOption(parseArgs, args);
		// start gateway server
		GatewayServer gatewayServer = startGatewayServer();
		// prepare python env

		// map filename to its Path
		Map<String, Path> filePathMap = new HashMap<>();
		// command which will be exec in python progress.
		List<String> commands = new ArrayList<>();
		// construct the filePathMap and the commands
		constructCommand(filePathMap, commands, parseArgs);
		try {
			// prepare the exec environment of python progress.
			PythonUtil.PythonEnvironment pythonEnv = PythonUtil.preparePythonEnvironment(filePathMap);
			// set env variable PYFLINK_GATEWAY_PORT for connecting of python gateway in python progress.
			pythonEnv.systemEnv.put("PYFLINK_GATEWAY_PORT", String.valueOf(gatewayServer.getListeningPort()));
			// start the python process.
			Process pythonProcess = PythonUtil.startPythonProcess(pythonEnv, commands);
			int exitCode = pythonProcess.waitFor();
			if (exitCode != 0) {
				throw new RuntimeException("Python process exits with code: " + exitCode);
			}
		} catch (Exception e) {
			LOG.error("run python process failed {}", e.getMessage());
		} finally {
			gatewayServer.shutdown();
		}
	}

	/**
	 * Initial gateway server and run in a daemon thread.
	 *
	 * @return
	 */
	public static GatewayServer startGatewayServer() {
		InetAddress localhost = InetAddress.getLoopbackAddress();
		GatewayServer gatewayServer = new GatewayServer.GatewayServerBuilder()
			.javaPort(0)
			.javaAddress(localhost)
			.build();
		Thread thread = new Thread(gatewayServer::start);
		thread.setName("py4j-gateway-init");
		thread.setDaemon(true);
		thread.start();
		try {
			thread.join();
		} catch (InterruptedException e) {
			LOG.error("The gateway server thread join failed. {}", e.getMessage());
			System.exit(1);
		}
		return gatewayServer;
	}

	/**
	 * construct the filePathMap and the commands.
	 * filePathMap stores all python files.
	 *
	 * @param filePathMap python file name to its path
	 * @param commands
	 * @param parseArgs
	 */
	public static void constructCommand(Map<String, Path> filePathMap, List<String> commands, Map<String, List<String>> parseArgs) {
		if (parseArgs.containsKey("python")) {
			String pythonFile = parseArgs.get("python").get(0);
			Path pythonFilePath = new Path(pythonFile);
			filePathMap.put(pythonFilePath.getName(), pythonFilePath);
			commands.add(pythonFilePath.getName());
		}
		if (parseArgs.containsKey("py-module")) {
			String pyModule = parseArgs.get("py-module").get(0);
			commands.add("-m");
			commands.add(pyModule);
		}
		if (parseArgs.containsKey("py-files")) {
			List<String> pyFiles = parseArgs.get("py-files");
			for (String pyFile : pyFiles) {
				Path pyFilePath = new Path(pyFile);
				filePathMap.put(pyFilePath.getName(), pyFilePath);
			}
		}
		if (parseArgs.containsKey("args")) {
			commands.addAll(parseArgs.get("args"));
		}
	}

	/**
	 * parse the args to the map format.
	 *
	 * @param parseArgs {"python"->List("xxx.py"),
	 *                  "py-files"->List("a.py","b.py","c.py"),
	 *                  "args"->List("--input","in.txt")
	 *                  }
	 * @param args      ["python", "xxx.py",
	 *                  "py-files", "a.py,b.py,c.py",
	 *                  "--input", "in.txt"]
	 */
	public static void parseOption(Map<String, List<String>> parseArgs, String[] args) {
		int argIndex = 0;
		if (args[0].equals("python") || args[0].equals("py-module")) {
			parseArgs.put(args[0], Collections.singletonList(args[1]));
			argIndex = 2;
		}
		if (argIndex == 2 && args.length > 2 && args[2].equals("py-files")) {
			List<String> pyFilesList = new ArrayList<>(Arrays.asList(args[3].split(",")));
			parseArgs.put(args[2], pyFilesList);
			argIndex = 4;
		}
		if (argIndex == 0) {
			throw new RuntimeException("Args is invalid");
		}
		if (args.length > argIndex) {
			List<String> otherArgList = new ArrayList<>(args.length - argIndex);
			for (int i = argIndex; i < args.length; i++) {
				otherArgList.add(args[i]);
			}
			parseArgs.put("args", otherArgList);
		}
	}
}
