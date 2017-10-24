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

package org.apache.flink.streaming.test.socket;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.streaming.examples.socket.SocketWindowWordCount;
import org.apache.flink.test.testdata.WordCountData;
import org.apache.flink.test.util.AbstractTestBase;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

import static org.junit.Assert.fail;

/**
 * Tests for {@link SocketWindowWordCount}.
 */
public class SocketWindowWordCountITCase extends AbstractTestBase {

	@Test
	public void testJavaProgram() throws Exception {
		InetAddress localhost = InetAddress.getByName("localhost");

		// suppress sysout messages from this example
		final PrintStream originalSysout = System.out;
		final PrintStream originalSyserr = System.err;

		final ByteArrayOutputStream errorMessages = new ByteArrayOutputStream();

		System.setOut(new PrintStream(new NullStream()));
		System.setErr(new PrintStream(errorMessages));

		try {
			try (ServerSocket server = new ServerSocket(0, 10, localhost)) {

				final ServerThread serverThread = new ServerThread(server);
				serverThread.setDaemon(true);
				serverThread.start();

				final int serverPort = server.getLocalPort();

				SocketWindowWordCount.main(new String[] { "--port", String.valueOf(serverPort) });

				if (errorMessages.size() != 0) {
					fail("Found error message: " + new String(errorMessages.toByteArray(), ConfigConstants.DEFAULT_CHARSET));
				}

				serverThread.join();
				serverThread.checkError();
			}
		}
		finally {
			System.setOut(originalSysout);
			System.setErr(originalSyserr);
		}
	}

	@Test
	public void testScalaProgram() throws Exception {
		InetAddress localhost = InetAddress.getByName("localhost");

		// suppress sysout messages from this example
		final PrintStream originalSysout = System.out;
		final PrintStream originalSyserr = System.err;

		final ByteArrayOutputStream errorMessages = new ByteArrayOutputStream();

		System.setOut(new PrintStream(new NullStream()));
		System.setErr(new PrintStream(errorMessages));

		try {
			try (ServerSocket server = new ServerSocket(0, 10, localhost)) {

				final ServerThread serverThread = new ServerThread(server);
				serverThread.setDaemon(true);
				serverThread.start();

				final int serverPort = server.getLocalPort();

				org.apache.flink.streaming.scala.examples.socket.SocketWindowWordCount.main(
						new String[] { "--port", String.valueOf(serverPort) });

				if (errorMessages.size() != 0) {
					fail("Found error message: " + new String(errorMessages.toByteArray(), ConfigConstants.DEFAULT_CHARSET));
				}

				serverThread.join();
				serverThread.checkError();
			}
		}
		finally {
			System.setOut(originalSysout);
			System.setErr(originalSyserr);
		}
	}

	// ------------------------------------------------------------------------

	private static class ServerThread extends Thread {

		private final ServerSocket serverSocket;

		private volatile Throwable error;

		public ServerThread(ServerSocket serverSocket) {
			super("Socket Server Thread");

			this.serverSocket = serverSocket;
		}

		@Override
		public void run() {
			try {
				try (Socket socket = serverSocket.accept();
						PrintWriter writer = new PrintWriter(socket.getOutputStream(), true)) {

					writer.println(WordCountData.TEXT);
				}
			}
			catch (Throwable t) {
				this.error = t;
			}
		}

		public void checkError() throws IOException {
			if (error != null) {
				throw new IOException("Error in server thread: " + error.getMessage(), error);
			}
		}
	}

	private static final class NullStream extends OutputStream {

		@Override
		public void write(int b) {}
	}
}
