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

import org.junit.Assert;
import org.junit.Test;
import py4j.GatewayServer;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

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
		List<Path> pyFilesList = new ArrayList<>();
		pyFilesList.add(new Path("a.py"));
		pyFilesList.add(new Path("b.py"));
		pyFilesList.add(new Path("c.py"));
		List<String> args = new ArrayList<>();
		args.add("--input");
		args.add("in.txt");

		PythonDriverOptions pythonDriverOptions = new PythonDriverOptions(
			"xxx",
			pyFilesList,
			args,
			new ArrayList<>(),
			null,
			null,
			new ArrayList<>());
		List<String> commands = PythonDriver.constructPythonCommands(pythonDriverOptions);
		// verify the generated commands
		Assert.assertEquals(4, commands.size());
		Assert.assertEquals(commands.get(0), "-m");
		Assert.assertEquals(commands.get(1), "xxx");
		Assert.assertEquals(commands.get(2), "--input");
		Assert.assertEquals(commands.get(3), "in.txt");
	}
}
