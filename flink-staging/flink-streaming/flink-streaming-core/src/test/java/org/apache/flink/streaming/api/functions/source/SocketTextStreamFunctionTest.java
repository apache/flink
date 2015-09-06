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

package org.apache.flink.streaming.api.functions.source;

import java.io.DataOutputStream;
import java.net.Socket;

import org.apache.flink.configuration.Configuration;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import static java.lang.Thread.sleep;
import static org.junit.Assert.*;
import static org.mockito.Mockito.verify;

import java.net.ServerSocket;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Tests for the {@link org.apache.flink.streaming.api.functions.source.SocketTextStreamFunction}.
 */
public class SocketTextStreamFunctionTest{

	final AtomicReference<Throwable> error = new AtomicReference<Throwable>();
	private final String host = "127.0.0.1";
	private final SourceFunction.SourceContext<String> ctx = Mockito.mock(SourceFunction.SourceContext.class);

	public SocketTextStreamFunctionTest() {
	}

	class SocketSource extends Thread {

		SocketTextStreamFunction socketSource;

		public SocketSource(ServerSocket serverSo, int maxRetry) throws Exception {
			this.socketSource =  new SocketTextStreamFunction(host, serverSo.getLocalPort(), '\n', maxRetry);
		}

		public void run() {
			try {
				this.socketSource.open(new Configuration());
				this.socketSource.run(ctx);
			}catch(Exception e){
				error.set(e);
			}
		}

		public void cancel(){
			this.socketSource.cancel();
		}
	}

	@Test
	public void testSocketSourceRetryForever() throws Exception{
		error.set(null);
		ServerSocket serverSo = new ServerSocket(0);
		SocketSource source = new SocketSource(serverSo, -1);
		source.start();

		int count = 0;
		Socket channel;
		while (count < 100) {
			channel = serverSo.accept();
			count++;
			channel.close();
			assertEquals(0, source.socketSource.retries);
		}
		source.cancel();

		if (error.get() != null) {
			Throwable t = error.get();
			t.printStackTrace();
			fail("Error in spawned thread: " + t.getMessage());
		}

		assertEquals(100, count);
	}

	@Test
	public void testSocketSourceRetryTenTimes() throws Exception{
		error.set(null);
		ServerSocket serverSo = new ServerSocket(0);
		SocketSource source = new SocketSource(serverSo, 10);
		source.socketSource.CONNECTION_RETRY_SLEEP = 200;

		assertEquals(0, source.socketSource.retries);

		source.start();

		Socket channel;
		channel = serverSo.accept();
		channel.close();
		serverSo.close();
		while(source.socketSource.retries < 10){
			long lastRetry = source.socketSource.retries;
			sleep(100);
			assertTrue(source.socketSource.retries >= lastRetry);
		};
		assertEquals(10, source.socketSource.retries);
		source.cancel();

		if (error.get() != null) {
			Throwable t = error.get();
			t.printStackTrace();
			fail("Error in spawned thread: " + t.getMessage());
		}

		assertEquals(10, source.socketSource.retries);
	}

	@Test
	public void testSocketSourceNeverRetry() throws Exception{
		error.set(null);
		ServerSocket serverSo = new ServerSocket(0);
		SocketSource source = new SocketSource(serverSo, 0);
		source.start();

		Socket channel;
		channel = serverSo.accept();
		channel.close();
		serverSo.close();
		sleep(2000);
		source.cancel();

		if (error.get() != null) {
			Throwable t = error.get();
			t.printStackTrace();
			fail("Error in spawned thread: " + t.getMessage());
		}

		assertEquals(0, source.socketSource.retries);
	}

	@Test
	public void testSocketSourceRetryTenTimesWithFirstPass() throws Exception{
		ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);

		error.set(null);
		ServerSocket serverSo = new ServerSocket(0);
		SocketSource source = new SocketSource(serverSo, 10);
		source.socketSource.CONNECTION_RETRY_SLEEP = 200;

		assertEquals(0, source.socketSource.retries);

		source.start();

		Socket channel;
		channel = serverSo.accept();
		DataOutputStream dataOutputStream = new DataOutputStream(channel.getOutputStream());
		dataOutputStream.write("testFirstSocketpass\n".getBytes());
		channel.close();
		serverSo.close();
		while(source.socketSource.retries < 10){
			long lastRetry = source.socketSource.retries;
			sleep(100);
			assertTrue(source.socketSource.retries >= lastRetry);
		};
		assertEquals(10, source.socketSource.retries);
		source.cancel();

		verify(ctx).collect(argument.capture());

		if (error.get() != null) {
			Throwable t = error.get();
			t.printStackTrace();
			fail("Error in spawned thread: " + t.getMessage());
		}

		assertEquals("testFirstSocketpass", argument.getValue());
		assertEquals(10, source.socketSource.retries);
	}
}