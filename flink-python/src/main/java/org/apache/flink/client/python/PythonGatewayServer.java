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

import py4j.GatewayServer;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Files;

/**
 * The Py4j Gateway Server provides RPC service for user's python process.
 */
public class PythonGatewayServer {

	/**
	 * <p>
	 * Main method to start a local GatewayServer on a ephemeral port.
	 * It tells python side via a file.
	 *
	 * See: py4j.GatewayServer.main()
	 * </p>
	 */
	public static void main(String[] args) throws IOException {
		InetAddress localhost = InetAddress.getLoopbackAddress();
		GatewayServer gatewayServer = new GatewayServer.GatewayServerBuilder()
			.javaPort(0)
			.javaAddress(localhost)
			.build();
		gatewayServer.start();

		int boundPort = gatewayServer.getListeningPort();
		if (boundPort == -1) {
			System.out.println("GatewayServer failed to bind; exiting");
			System.exit(1);
		}

		// Tells python side the port of our java rpc server
		String handshakeFilePath = System.getenv("_PYFLINK_CONN_INFO_PATH");
		File handshakeFile = new File(handshakeFilePath);
		File tmpPath = Files.createTempFile(handshakeFile.getParentFile().toPath(),
			"connection", ".info").toFile();
		FileOutputStream fileOutputStream = new FileOutputStream(tmpPath);
		DataOutputStream stream = new DataOutputStream(fileOutputStream);
		stream.writeInt(boundPort);
		stream.close();
		fileOutputStream.close();

		if (!tmpPath.renameTo(handshakeFile)) {
			System.out.println("Unable to write connection information to handshake file: " + handshakeFilePath + ", now exit...");
			System.exit(1);
		}

		try {
			// Exit on EOF or broken pipe.  This ensures that the server dies
			// if its parent program dies.
			while (System.in.read() != -1) {
				// Do nothing
			}
			gatewayServer.shutdown();
			System.exit(0);
		} finally {
			System.exit(1);
		}
	}
}
