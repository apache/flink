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

package org.apache.flink.runtime.ipc;

import static org.junit.Assert.*;

import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;

import org.apache.flink.core.protocols.VersionedProtocol;
import org.apache.flink.runtime.net.NetUtils;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.StringValue;


public class RpcTest {
	
	@Test
	public void testRpc() {
		try { 
			Server server = null;
			TestProtocol proxy = null;
			
			try {
				// setup the RPCs
				int port = getAvailablePort();
				server = RPC.getServer(new TestProtocolImpl(), "localhost", port, 4);
				server.start();
				
				proxy = RPC.getProxy(TestProtocol.class, new InetSocketAddress("localhost", port), NetUtils.getSocketFactory());
				
				// make a few calls with various types
//				proxy.methodWithNoParameters();
				
				assertEquals(19, proxy.methodWithPrimitives(16, new StringValue("abc")));
				assertEquals(new DoubleValue(17.0), proxy.methodWithWritables(new LongValue(17)));
			}
			finally {
				if (proxy != null) {
					RPC.stopProxy(proxy);
				}
				if (server != null) {
					server.stop();
				}
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	
	private static final int getAvailablePort() throws IOException {
		ServerSocket serverSocket = null;
		for (int i = 0; i < 50; i++){
			try {
				serverSocket = new ServerSocket(0);
				int port = serverSocket.getLocalPort();
				if (port != 0) {
					return port;
				}
			}
			catch (IOException e) {}
			finally {
				if (serverSocket != null) {
					serverSocket.close();
				}
			}
		}
		
		throw new IOException("Could not find a free port.");
	}
	
	// --------------------------------------------------------------------------------------------
	
	public static interface TestProtocol extends VersionedProtocol {
		
		public void methodWithNoParameters();
		
		public int methodWithPrimitives(int intParam, StringValue writableParam);
		
		public DoubleValue methodWithWritables(LongValue writableParam);
	}
	
	
	public static final class TestProtocolImpl implements TestProtocol {

		@Override
		public void methodWithNoParameters() {}

		@Override
		public int methodWithPrimitives(int intParam, StringValue writableParam) {
			return intParam + writableParam.length();
		}

		@Override
		public DoubleValue methodWithWritables(LongValue writableParam) {
			return new DoubleValue(writableParam.getValue());
		}
	}
}
