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

package org.apache.flink.streaming.api.functions;

import java.io.IOException;
import java.net.Socket;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.SocketClientSink;
import org.apache.flink.streaming.util.serialization.SerializationSchema;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Tests for the {@link org.apache.flink.streaming.api.functions.sink.SocketClientSink}.
 */
public class SocketClientSinkTest{

	final AtomicReference<Throwable> error = new AtomicReference<Throwable>();
	private final String host = "127.0.0.1";
	private int port;
	private String access;
	private String value;
	public SocketServer.ServerThread th;

	public SocketClientSinkTest() {
	}

	class SocketServer extends Thread {

		private ServerSocket server;
		private Socket sk;
		private BufferedReader rdr;

		private SocketServer() {
			try {
				this.server = new ServerSocket(0);
				port = server.getLocalPort();
			} catch (Exception e) {
				error.set(e);
			}
		}

		public void run() {
			try {
				sk = server.accept();
				access = "Connected";
				th = new ServerThread(sk);
				th.start();
			} catch (Exception e) {
				error.set(e);
			}
		}

		class ServerThread extends Thread {
			Socket sk;

			public ServerThread(Socket sk) {
				this.sk = sk;
			}

			public void run() {
				try {
					rdr = new BufferedReader(new InputStreamReader(sk
							.getInputStream()));
					value = rdr.readLine();
				} catch (IOException e) {
					error.set(e);
				}
			}
		}
	}

	@Test
	public void testSocketSink() throws Exception{

		SocketServer server = new SocketServer();
		server.start();

		SerializationSchema<String, byte[]> simpleSchema = new SerializationSchema<String, byte[]>() {
			@Override
			public byte[] serialize(String element) {
				return element.getBytes();
			}
		};

		SocketClientSink<String> simpleSink = new SocketClientSink<String>(host, port, simpleSchema);
		simpleSink.open(new Configuration());
		simpleSink.invoke("testSocketSinkInvoke");
		simpleSink.close();

		server.join();
		th.join();

		if (error.get() != null) {
			Throwable t = error.get();
			t.printStackTrace();
			fail("Error in spawned thread: " + t.getMessage());
		}

		assertEquals("Connected", this.access);
		assertEquals("testSocketSinkInvoke", value);
	}
}