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

import org.junit.Test;

import static java.lang.Thread.sleep;
import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;

/**
 * Tests for the {@link org.apache.flink.streaming.api.functions.sink.SocketClientSink}.
 */
public class SocketClientSinkTest{

	private final String host = "127.0.0.1";
	private int port = 9999;
	private String access;
	public SocketServer.ServerThread th = null;

	class SocketServer extends Thread {

		private ServerSocket server = null;
		private Socket sk = null;
		private BufferedReader rdr = null;
		private PrintWriter wtr = null;

		private SocketServer(int port) {
			while (port > 0) {
				try {
					this.server = new ServerSocket(port);
					break;
				} catch (Exception e) {
					--port;
					if (port > 0) {
						continue;
					}
					else{
						e.printStackTrace();
					}
				}
			}
		}

		public void run() {
			System.out.println("Listenning...");
			try {
				sk = server.accept();
				access = "Connected";
				th = new ServerThread(sk);
				th.start();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		class ServerThread extends Thread {

			Socket sk = null;

			public ServerThread(Socket sk) {
				this.sk = sk;
			}

			public void run() {
				try {
					access = "Invoked";
					wtr = new PrintWriter(sk.getOutputStream());
					rdr = new BufferedReader(new InputStreamReader(sk
							.getInputStream()));
					String line = rdr.readLine();
					System.out.println("Info from clients: " + line);
					wtr.println("Server received info: " + line + "'\n");
					wtr.flush();
					System.out.println("Return to client!");
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	@Test
	public void testSocketSink(){
		SocketServer server = new SocketServer(port);
		server.start();

		SerializationSchema<String, byte[]> simpleSchema = new SerializationSchema<String, byte[]>() {
			@Override
			public byte[] serialize(String element) {
				return new byte[0];
			}
		};

		SocketClientSink<String> simpleSink = new SocketClientSink<String>(host, port, simpleSchema);
		simpleSink.open(new Configuration());
		simpleSink.invoke("testSocketSinkInvoke");

		try {
			server.join();
			sleep(1000);
		}
		catch (Exception e){
			e.printStackTrace();
		}
		assertEquals(this.access, "Invoked");
		simpleSink.close();
	}
}