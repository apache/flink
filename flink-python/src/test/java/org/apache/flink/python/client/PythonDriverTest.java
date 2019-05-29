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

package org.apache.flink.python.client;

import org.apache.flink.core.fs.Path;

import org.junit.Assert;
import org.junit.Test;
import py4j.GatewayServer;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests for the {@link PythonDriver}.
 */
public class PythonDriverTest {
	@Test
	public void testStartGatewayServer() {
		GatewayServer gatewayServer = PythonDriver.startGatewayServer();
		try {
			Socket socket = new Socket("localhost", gatewayServer.getListeningPort());
			assert socket.isConnected();
		} catch (IOException e) {
			throw new RuntimeException("Connect Gateway Server failed");
		} finally {
			gatewayServer.shutdown();
		}
	}

	@Test
	public void testConstructCommands() {
		Map<String, Path> filePathMap = new HashMap<>();
		Map<String, List<String>> parseArgs = new HashMap<>();
		parseArgs.put("py", Collections.singletonList("xxx.py"));
		List<String> pyFilesList = new ArrayList<>();
		pyFilesList.add("a.py");
		pyFilesList.add("b.py");
		pyFilesList.add("c.py");
		parseArgs.put("pyfs", pyFilesList);
		List<String> otherArgs = new ArrayList<>();
		otherArgs.add("--input");
		otherArgs.add("in.txt");
		parseArgs.put("args", otherArgs);
		List<String> commands = PythonDriver.constructPythonCommands(filePathMap, parseArgs);
		Path pythonPath = filePathMap.get("xxx.py");
		Assert.assertNotNull(pythonPath);
		Assert.assertEquals(pythonPath.getName(), "xxx.py");
		Path aPyFilePath = filePathMap.get("a.py");
		Assert.assertNotNull(aPyFilePath);
		Assert.assertEquals(aPyFilePath.getName(), "a.py");
		Path bPyFilePath = filePathMap.get("b.py");
		Assert.assertNotNull(bPyFilePath);
		Assert.assertEquals(bPyFilePath.getName(), "b.py");
		Path cPyFilePath = filePathMap.get("c.py");
		Assert.assertNotNull(cPyFilePath);
		Assert.assertEquals(cPyFilePath.getName(), "c.py");
		Assert.assertEquals(3, commands.size());
		Assert.assertEquals(commands.get(0), "xxx.py");
		Assert.assertEquals(commands.get(1), "--input");
		Assert.assertEquals(commands.get(2), "in.txt");
	}

	@Test
	public void testParseOptions() {
		String[] args = {"py", "xxx.py", "pyfs", "a.py,b.py,c.py", "--input", "in.txt"};
		Map<String, List<String>> parsedArgs = PythonDriver.parseOptions(args);
		List<String> pythonMainFile = parsedArgs.get("py");
		Assert.assertNotNull(pythonMainFile);
		Assert.assertEquals(1, pythonMainFile.size());
		Assert.assertEquals(pythonMainFile.get(0), args[1]);
		List<String> pyFilesList = parsedArgs.get("pyfs");
		Assert.assertEquals(3, pyFilesList.size());
		String[] pyFiles = args[3].split(",");
		for (int i = 0; i < pyFiles.length; i++) {
			assert pyFilesList.get(i).equals(pyFiles[i]);
		}
		List<String> otherArgs = parsedArgs.get("args");
		for (int i = 4; i < args.length; i++) {
			Assert.assertEquals(otherArgs.get(i - 4), args[i]);
		}
	}
}
