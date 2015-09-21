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

import org.apache.commons.io.IOUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
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
	

	private SerializationSchema<String, byte[]> simpleSchema = new SerializationSchema<String, byte[]>() {
		@Override
		public byte[] serialize(String element) {
			return element.getBytes();
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
					simpleSink.invoke(TEST_MESSAGE + '\n');
					simpleSink.close();
				}
				catch (Throwable t){
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
					simpleSink.invoke(TEST_MESSAGE + '\n');
				}
				catch (Throwable t){
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
				simpleSink.invoke(TEST_MESSAGE + '\n');
				simpleSink.invoke(TEST_MESSAGE + '\n');
				fail("This should have failed with an exception");
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
	public void testSocketSinkRetryThreeTimes() throws Exception {
		final ServerSocket server = new ServerSocket(0);
		final int port = server.getLocalPort();
		
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
				finally {
					// close the server now to prevent reconnects
					IOUtils.closeQuietly(server);
				}
			}
		};

		serverRunner.start();

		SocketClientSink<String> simpleSink = new SocketClientSink<>(host, port, simpleSchema, 3);
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
			simpleSink.invoke(TEST_MESSAGE + '\n');
			simpleSink.invoke(TEST_MESSAGE + '\n');
		}
		catch (IOException e) {
			// check whether throw a exception that reconnect failed.
			assertTrue("Wrong exception", e.getMessage().contains(EXCEPTION_MESSGAE));
		}
		catch (Exception e) {
			fail("wrong exception: " + e.getClass().getName() + " - " + e.getMessage());
		}

		assertEquals(3, simpleSink.getCurrentNumberOfRetries());
	}

	/**
	 * This test the reconnect to server success.
	 * First close the server and let the sink get reconnecting.
	 * Meanwhile, reopen the server to let the sink reconnect success to socket.
	 */
	@Test
	public void testSocketSinkRetryAccess() throws Exception {
		final ServerSocket server1 = new ServerSocket(0);
		final int port = server1.getLocalPort();

		final AtomicReference<Throwable> error = new AtomicReference<Throwable>();

		// start a server, for the sink's open() method to connect against
		// the server is immediately shut down again
		Thread serverRunner = new Thread("Test server runner") {

			@Override
			public void run() {
				try {
					Socket sk = server1.accept();
					sk.close();
				}
				catch (Throwable t) {
					error.set(t);
				}
				finally {
					IOUtils.closeQuietly(server1);
				}
			}
		};
		serverRunner.start();

		final SocketClientSink<String> simpleSink = new SocketClientSink<>(host, port, simpleSchema, -1, true);
		simpleSink.open(new Configuration());

		// wait until the server is shut down
		serverRunner.join();
		if (error.get() != null) {
			Throwable t = error.get();
			t.printStackTrace();
			fail("Error in server thread: " + t.getMessage());
		}

		// run some data output on the sink. this should fail due to the inactive server, but retry
		Thread sinkRunner = new Thread("Test sink runner") {

			@Override
			public void run() {
				try {
					// need two messages here: send a fin to cancel the client state:FIN_WAIT_2 while the server is CLOSE_WAIT
					simpleSink.invoke(TEST_MESSAGE + '\n');
					simpleSink.invoke(TEST_MESSAGE + '\n');
				}
				catch (Throwable t) {
					error.set(t);
				}
			}
		};
		sinkRunner.start();
		
		// we start another server now, which will make the sink complete its task
		ServerSocket server2 = new ServerSocket(port);
		Socket sk = server2.accept();
		BufferedReader rdr = new BufferedReader(new InputStreamReader(sk.getInputStream()));
		String value = rdr.readLine();
		int retry = simpleSink.getCurrentNumberOfRetries();
		
		// let the sink finish
		sinkRunner.join();

		// make sure that the sink did not throw an error
		if (error.get() != null) {
			Throwable t = error.get();
			t.printStackTrace();
			fail("Error in spawned thread: " + t.getMessage());
		}

		// validate state and results
		assertEquals(TEST_MESSAGE, value);
		assertTrue(retry > 0);
	}
}