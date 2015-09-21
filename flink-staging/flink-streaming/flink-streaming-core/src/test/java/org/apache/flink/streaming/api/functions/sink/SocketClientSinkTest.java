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

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.Socket;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.ServerSocket;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Tests for the {@link org.apache.flink.streaming.api.functions.sink.SocketClientSink}.
 */
public class SocketClientSinkTest{

	final AtomicReference<Throwable> error = new AtomicReference<Throwable>();
	private final String host = "127.0.0.1";
	private int port;
	private long retry;
	private String value;
	private Thread t1;
	private ServerSocket server;
	private SocketClientSink<String> simpleSink;
	final private String except = "Cannot send message testSocketSinkInvoke";

	private SerializationSchema<String, byte[]> simpleSchema = new SerializationSchema<String, byte[]>() {
		@Override
		public byte[] serialize(String element) {
			return element.getBytes();
		}
	};

	public SocketClientSinkTest() {
	}

	@Test
	public void testSocketSink() throws Exception{
		error.set(null);
		value = "";
		server = new ServerSocket(0);
		port = server.getLocalPort();

		new Thread(new Runnable() {
			@Override
			public void run() {
				t1 = Thread.currentThread();

				try {
					simpleSink = new SocketClientSink<String>(host, port, simpleSchema, 0);
					simpleSink.open(new Configuration());
					simpleSink.invoke("testSocketSinkInvoke");
					simpleSink.close();
				} catch (Exception e){
					error.set(e);
				}
			}
		}).start();

		Socket sk = server.accept();
		BufferedReader rdr = new BufferedReader(new InputStreamReader(sk
				.getInputStream()));
		value = rdr.readLine();

		t1.join();
		server.close();

		if (error.get() != null) {
			Throwable t = error.get();
			t.printStackTrace();
			fail("Error in spawned thread: " + t.getMessage());
		}

		assertEquals("testSocketSinkInvoke", value);
	}

	@Test
	public void testSocketSinkNoRetry() throws Exception{
		retry = -1L;
		error.set(null);
		server = new ServerSocket(0);
		port = server.getLocalPort();

		t1 = new Thread(new Runnable() {

			@Override
			public void run() {
				try {
					t1 = Thread.currentThread();
					Socket sk = server.accept();
					sk.close();
					server.close();
				} catch (Exception e) {
					error.set(e);
				}
			}
		});
		t1.start();

		simpleSink = new SocketClientSink<String>(host, port, simpleSchema, 0);

		try {
			simpleSink.open(new Configuration());

			//wait socket server to close
			t1.join();
			if (error.get() == null) {

				//firstly send a fin to cancel the client state:FIN_WAIT_2 while the server is CLOSE_WAIT
				simpleSink.invoke("testSocketSinkInvoke");

				//socket is closed then test "retry"
				simpleSink.invoke("testSocketSinkInvoke");
				simpleSink.close();
			}
		} catch (Exception e) {

			//check whether throw a exception that reconnect failed.
			retry = simpleSink.retries;
			if (!(e instanceof RuntimeException) || e.toString().indexOf(except) == -1){
				error.set(e);
			}
		}

		if (error.get() != null) {
			Throwable t = error.get();
			t.printStackTrace();
			fail("Error in spawned thread: " + t.getMessage());
		}
		assertEquals(0, retry);
	}

	@Test
	public void testSocketSinkRetryTenTimes() throws Exception{
		retry = -1L;
		error.set(null);
		server = new ServerSocket(0);
		port = server.getLocalPort();

		t1 = new Thread(new Runnable() {

			@Override
			public void run() {
				try {
					t1 = Thread.currentThread();
					Socket sk = server.accept();
					sk.close();
					server.close();
				} catch (Exception e) {
					error.set(e);
				}
			}
		});
		t1.start();

		simpleSink = new SocketClientSink<String>(host, port, simpleSchema, 10);

		try {
			simpleSink.open(new Configuration());

			//wait socket server to close
			t1.join();
			if (error.get() == null) {

				//firstly send a fin to cancel the client state:FIN_WAIT_2 while the server is CLOSE_WAIT
				simpleSink.invoke("testSocketSinkInvoke");

				//socket is closed then test "retry"
				simpleSink.invoke("testSocketSinkInvoke");
				simpleSink.close();
			}
		} catch (Exception e) {

			//check whether throw a exception that reconnect failed.
			retry = simpleSink.retries;
			if (!(e instanceof RuntimeException) || e.toString().indexOf(except) == -1){
				error.set(e);
			}
		}

		if (error.get() != null) {
			Throwable t = error.get();
			t.printStackTrace();
			fail("Error in spawned thread: " + t.getMessage());
		}
		assertEquals(10, retry);
	}

	@Test
	public void testSocketSinkRetryAccess() throws Exception{

		//This test the reconnect to server success.
		//First close the server and let the sink get reconnecting.
		//Meanwhile, reopen the server to let the sink reconnect success to socket.

		retry = -1L;
		error.set(null);
		server = new ServerSocket(0);
		port = server.getLocalPort();

		t1 = new Thread(new Runnable() {

			@Override
			public void run() {
				try {
					t1 = Thread.currentThread();
					Socket sk = server.accept();
					sk.close();
					server.close();
					server = null;
				} catch (Exception e) {
					error.set(e);
				}
			}
		});
		t1.start();

		try {
			simpleSink = new SocketClientSink<String>(host, port, simpleSchema, -1);
			simpleSink.open(new Configuration());

			t1.join();
			if (error.get() == null) {

				//firstly send a fin to cancel the client state:FIN_WAIT_2 while the server is CLOSE_WAIT
				simpleSink.invoke("testSocketSinkInvoke");

				new Thread(new Runnable() {

					@Override
					public void run() {
						try {
							//socket is closed then test "retry"
							simpleSink.invoke("testSocketSinkInvoke");
							simpleSink.close();
						} catch (Exception e) {
							error.set(e);
						}
					}

				}).start();

				//set a new server to let the retry success.
				while (simpleSink.retries == 0) {
					Thread.sleep(1000);
				}

				//reopen socket server for sink access
				value = "";
				server = new ServerSocket(port);
				Socket sk = server.accept();
				BufferedReader rdr = new BufferedReader(new InputStreamReader(sk
						.getInputStream()));
				value = rdr.readLine();

				retry = simpleSink.retries;
			}

		} catch (Exception e) {
			error.set(e);
		}

		if (error.get() != null) {
			Throwable t = error.get();
			t.printStackTrace();
			fail("Error in spawned thread: " + t.getMessage());
		}

		assertEquals("testSocketSinkInvoke", value);
		assertTrue(retry > 0);
	}
}