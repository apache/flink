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
		// e.g. python a.py ...
		// e.g. pyModule a.b -pyFiles a.py ...
		if (args.length < 2) {
			LOG.error("Required at least two arguments, only python file or python module is available.");
			System.exit(1);
		}
		// parse args
		Map<String, List<String>> parsedArgs = parseOptions(args);
		// start gateway server
		GatewayServer gatewayServer = startGatewayServer();
		// prepare python env

		// map filename to its Path
		Map<String, Path> filePathMap = new HashMap<>();
		// commands which will be exec in python progress.
		List<String> commands = constructCommands(filePathMap, parsedArgs);
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
		} catch (Throwable e) {
			LOG.error("Run python process failed", e);
		} finally {
			gatewayServer.shutdown();
		}
	}

	/**
	 * Creates a GatewayServer run in a daemon thread.
	 *
	 * @return The created GatewayServer
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
			LOG.error("The gateway server thread join failed.", e);
			System.exit(1);
		}
		return gatewayServer;
	}

	/**
	 * Constructs the commands which will be executed in python process.
	 *
	 * @param filePathMap stores python file name to its path
	 * @param parsedArgs  parsed args
	 */
	public static List<String> constructCommands(Map<String, Path> filePathMap, Map<String, List<String>> parsedArgs) {
		List<String> commands = new ArrayList<>();
		if (parsedArgs.containsKey("python")) {
			String pythonFile = parsedArgs.get("python").get(0);
			Path pythonFilePath = new Path(pythonFile);
			filePathMap.put(pythonFilePath.getName(), pythonFilePath);
			commands.add(pythonFilePath.getName());
		}
		if (parsedArgs.containsKey("pyModule")) {
			String pyModule = parsedArgs.get("pyModule").get(0);
			commands.add("-m");
			commands.add(pyModule);
		}
		if (parsedArgs.containsKey("pyFiles")) {
			List<String> pyFiles = parsedArgs.get("pyFiles");
			for (String pyFile : pyFiles) {
				Path pyFilePath = new Path(pyFile);
				filePathMap.put(pyFilePath.getName(), pyFilePath);
			}
		}
		if (parsedArgs.containsKey("args")) {
			commands.addAll(parsedArgs.get("args"));
		}
		return commands;
	}

	/**
	 * Parses the args to the map format.
	 *
	 * @param args ["python", "xxx.py",
	 *             "pyFiles", "a.py,b.py,c.py",
	 *             "--input", "in.txt"]
	 * @return {"python"->List("xxx.py"),"pyFiles"->List("a.py","b.py","c.py"),"args"->List("--input","in.txt")}
	 */
	public static Map<String, List<String>> parseOptions(String[] args) {
		Map<String, List<String>> parsedArgs = new HashMap<>();
		int argIndex = 0;
		boolean isValidPythonFile = false;
		// valid args should include python or pyModule field and their value.
		if (args[0].equals("python") || args[0].equals("pyModule")) {
			parsedArgs.put(args[0], Collections.singletonList(args[1]));
			argIndex = 2;
			isValidPythonFile = true;
		}
		if (isValidPythonFile && args.length > 2 && args[2].equals("pyFiles")) {
			List<String> pyFilesList = new ArrayList<>(Arrays.asList(args[3].split(",")));
			parsedArgs.put(args[2], pyFilesList);
			argIndex = 4;
		}
		if (!isValidPythonFile) {
			throw new RuntimeException("Args is invalid, the args is required to include python main file or pyModule");
		}
		// if arg include other args, the key "args" will map to other args.
		if (args.length > argIndex) {
			List<String> otherArgList = new ArrayList<>(args.length - argIndex);
			for (int i = argIndex; i < args.length; i++) {
				otherArgList.add(args[i]);
			}
			parsedArgs.put("args", otherArgList);
		}
		return parsedArgs;
	}
}
