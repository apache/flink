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

package org.apache.flink.streaming.api.functions.sink;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.TestLogger;

import org.apache.commons.io.IOUtils;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for the {@link org.apache.flink.streaming.api.functions.sink.SocketClientSink}.
 */
@SuppressWarnings("serial")
public class SocketClientSinkTest extends TestLogger {

	private static final String TEST_MESSAGE = "testSocketSinkInvoke";

	private static final String EXCEPTION_MESSGAE = "Failed to send message '" + TEST_MESSAGE + "\n'";

	private static final String host = "127.0.0.1";

	private SerializationSchema<String> simpleSchema = new SerializationSchema<String>() {
		@Override
		public byte[] serialize(String element) {
			return element.getBytes(ConfigConstants.DEFAULT_CHARSET);
		}
	};

	@Test
	public void testSocketSink() throws Exception {
		final ServerSocket server = new ServerSocket(0);
		final int port = server.getLocalPort();

		final AtomicReference<Throwable> error = new AtomicReference<Throwable>();

		Thread sinkRunner = new Thread("Test sink runner") {
			@Override
			public void run() {
				try {
					SocketClientSink<String> simpleSink = new SocketClientSink<>(host, port, simpleSchema, 0);
					simpleSink.open(new Configuration());
					simpleSink.invoke(TEST_MESSAGE + '\n', SinkContextUtil.forTimestamp(0));
					simpleSink.close();
				}
				catch (Throwable t) {
					error.set(t);
				}
			}
		};

		sinkRunner.start();

		Socket sk = server.accept();
		BufferedReader rdr = new BufferedReader(new InputStreamReader(sk.getInputStream()));

		String value = rdr.readLine();

		sinkRunner.join();
		server.close();

		if (error.get() != null) {
			Throwable t = error.get();
			t.printStackTrace();
			fail("Error in spawned thread: " + t.getMessage());
		}

		assertEquals(TEST_MESSAGE, value);
	}

	@Test
	public void testSinkAutoFlush() throws Exception {
		final ServerSocket server = new ServerSocket(0);
		final int port = server.getLocalPort();

		final SocketClientSink<String> simpleSink = new SocketClientSink<>(host, port, simpleSchema, 0, true);
		simpleSink.open(new Configuration());

		final AtomicReference<Throwable> error = new AtomicReference<Throwable>();

		Thread sinkRunner = new Thread("Test sink runner") {
			@Override
			public void run() {
				try {
					// need two messages here: send a fin to cancel the client state:FIN_WAIT_2 while the server is CLOSE_WAIT
					simpleSink.invoke(TEST_MESSAGE + '\n', SinkContextUtil.forTimestamp(0));
				}
				catch (Throwable t) {
					error.set(t);
				}
			}
		};

		sinkRunner.start();

		Socket sk = server.accept();
		BufferedReader rdr = new BufferedReader(new InputStreamReader(sk.getInputStream()));
		String value = rdr.readLine();

		sinkRunner.join();
		simpleSink.close();
		server.close();

		if (error.get() != null) {
			Throwable t = error.get();
			t.printStackTrace();
			fail("Error in spawned thread: " + t.getMessage());
		}

		assertEquals(TEST_MESSAGE, value);
	}

	@Test
	public void testSocketSinkNoRetry() throws Exception {
		final ServerSocket server = new ServerSocket(0);
		final int port = server.getLocalPort();

		try {
			final AtomicReference<Throwable> error = new AtomicReference<Throwable>();

			Thread serverRunner = new Thread("Test server runner") {

				@Override
				public void run() {
					try {
						Socket sk = server.accept();
						sk.close();
					}
					catch (Throwable t) {
						error.set(t);
					}
				}
			};
			serverRunner.start();

			SocketClientSink<String> simpleSink = new SocketClientSink<>(host, port, simpleSchema, 0, true);
			simpleSink.open(new Configuration());

			// wait socket server to close
			serverRunner.join();
			if (error.get() != null) {
				Throwable t = error.get();
				t.printStackTrace();
				fail("Error in server thread: " + t.getMessage());
			}

			try {
				// socket should be closed, so this should trigger a re-try
				// need two messages here: send a fin to cancel the client state:FIN_WAIT_2 while the server is CLOSE_WAIT
				while (true) { // we have to do this more often as the server side closed is not guaranteed to be noticed immediately
					simpleSink.invoke(TEST_MESSAGE + '\n', SinkContextUtil.forTimestamp(0));
				}
			}
			catch (IOException e) {
				// check whether throw a exception that reconnect failed.
				assertTrue("Wrong exception", e.getMessage().contains(EXCEPTION_MESSGAE));
			}
			catch (Exception e) {
				fail("wrong exception: " + e.getClass().getName() + " - " + e.getMessage());
			}

			assertEquals(0, simpleSink.getCurrentNumberOfRetries());
		}
		finally {
			IOUtils.closeQuietly(server);
		}
	}

	@Test
	public void testRetry() throws Exception {

		final ServerSocket[] serverSocket = new ServerSocket[1];
		final ExecutorService[] executor = new ExecutorService[1];

		try {
			serverSocket[0] = new ServerSocket(0);
			executor[0] = Executors.newCachedThreadPool();

			int port = serverSocket[0].getLocalPort();

			Callable<Void> serverTask = new Callable<Void>() {
				@Override
				public Void call() throws Exception {
					Socket socket = serverSocket[0].accept();

					BufferedReader reader = new BufferedReader(new InputStreamReader(
							socket.getInputStream()));

					String value = reader.readLine();
					assertEquals("0", value);

					socket.close();
					return null;
				}
			};

			Future<Void> serverFuture = executor[0].submit(serverTask);

			final SocketClientSink<String> sink = new SocketClientSink<>(
					host, serverSocket[0].getLocalPort(), simpleSchema, -1, true);

			// Create the connection
			sink.open(new Configuration());

			// Initial payload => this will be received by the server an then the socket will be
			// closed.
			sink.invoke("0\n", SinkContextUtil.forTimestamp(0));

			// Get future an make sure there was no problem. This will rethrow any Exceptions from
			// the server.
			serverFuture.get();

			// Shutdown the server socket
			serverSocket[0].close();
			assertTrue(serverSocket[0].isClosed());

			// No retries expected at this point
			assertEquals(0, sink.getCurrentNumberOfRetries());

			final CountDownLatch retryLatch = new CountDownLatch(1);
			final CountDownLatch again = new CountDownLatch(1);

			Callable<Void> sinkTask = new Callable<Void>() {
				@Override
				public Void call() throws Exception {
					// Send next payload => server is down, should try to reconnect.

					// We need to send more than just one packet to notice the closed connection.
					while (retryLatch.getCount() != 0) {
						sink.invoke("1\n");
					}

					return null;
				}
			};

			Future<Void> sinkFuture = executor[0].submit(sinkTask);

			while (sink.getCurrentNumberOfRetries() == 0) {
				// Wait for a retry
				Thread.sleep(100);
			}

			// OK the poor guy retried to write
			retryLatch.countDown();

			// Restart the server
			serverSocket[0] = new ServerSocket(port);
			Socket socket = serverSocket[0].accept();

			BufferedReader reader = new BufferedReader(new InputStreamReader(
					socket.getInputStream()));

			// Wait for the reconnect
			String value = reader.readLine();

			assertEquals("1", value);

			// OK the sink re-connected. :)
		}
		finally {
			if (serverSocket[0] != null) {
				serverSocket[0].close();
			}

			if (executor[0] != null) {
				executor[0].shutdown();
			}
		}
	}
}
