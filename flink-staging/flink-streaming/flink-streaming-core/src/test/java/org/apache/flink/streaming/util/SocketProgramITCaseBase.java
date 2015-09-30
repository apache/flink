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

import org.apache.flink.test.testdata.WordCountData;
import org.apache.flink.util.NetUtils;

import org.junit.Assert;

import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

public abstract class SocketProgramITCaseBase extends StreamingProgramTestBase {

	protected static final String HOST = "localhost";
	protected static Integer port;
	protected String resultPath;

	private ServerSocket temporarySocket;

	@Override
	protected void preSubmit() throws Exception {
		port = NetUtils.getAvailablePort();
		temporarySocket = createSocket(HOST, port, WordCountData.TEXT);
		resultPath = getTempDirPath("result");
	}

	@Override
	protected void postSubmit() throws Exception {
		compareResultsByLinesInMemory(WordCountData.STREAMING_COUNTS_AS_TUPLES, resultPath);
		temporarySocket.close();
	}

	public ServerSocket createSocket(String host, int port, String contents) throws Exception {
		ServerSocket serverSocket = new ServerSocket(port);
		ServerThread st = new ServerThread(serverSocket, contents);
		st.start();
		return serverSocket;
	}

	private static class ServerThread extends Thread {

		private ServerSocket serverSocket;
		private String contents;
		private Thread t;

		public ServerThread(ServerSocket serverSocket, String contents) {
			this.serverSocket = serverSocket;
			this.contents = contents;
			t = new Thread(this);
		}

		public void waitForAccept() throws Exception {
			Socket socket = serverSocket.accept();
			PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
			writer.println(contents);
			writer.close();
			socket.close();
		}

		public void run() {
			try {
				waitForAccept();
			} catch (Exception e) {
				Assert.fail();
			}
		}

		@Override
		public void start() {
			t.start();
		}
	}
}