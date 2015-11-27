/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.util;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.test.testdata.WordCountData;
import org.apache.flink.util.NetUtils;

import org.junit.Assert;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Test base for streaming programs relying on an open server socket to write to.
 */
public abstract class SocketOutputTestBase extends StreamingProgramTestBase {

	protected static final String HOST = "localhost";
	protected static Integer port;
	protected Set<String> dataReadFromSocket = new HashSet<String>();

	@Override
	protected void preSubmit() throws Exception {
		port = NetUtils.getAvailablePort();
		temporarySocket = createLocalSocket(port);
	}

	@Override
	protected void postSubmit() throws Exception {
		Set<String> expectedData = new HashSet<String>(Arrays.asList(WordCountData.STREAMING_COUNTS_AS_TUPLES.split("\n")));
		Assert.assertEquals(expectedData, dataReadFromSocket);
		temporarySocket.close();
	}

	protected ServerSocket temporarySocket;

	public ServerSocket createLocalSocket(int port) throws Exception {
		ServerSocket serverSocket = new ServerSocket(port);
		ServerThread st = new ServerThread(serverSocket);
		st.start();
		return serverSocket;
	}

	protected class ServerThread extends Thread {

		private ServerSocket serverSocket;
		private Thread t;

		public ServerThread(ServerSocket serverSocket) {
			this.serverSocket = serverSocket;
			t = new Thread(this);
		}

		public void waitForAccept() throws Exception {
			Socket socket = serverSocket.accept();
			BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			DeserializationSchema<String> schema = new SimpleStringSchema();
			String rawData = in.readLine();
			while (rawData != null){
				String string = schema.deserialize(rawData.getBytes());
				dataReadFromSocket.add(string);
				rawData = in.readLine();
			}
			socket.close();
		}

		public void run() {
			try {
				waitForAccept();
			} catch (Exception e) {
				Assert.fail();
				throw new RuntimeException(e);
			}
		}

		@Override
		public void start() {
			t.start();
		}
	}
}
