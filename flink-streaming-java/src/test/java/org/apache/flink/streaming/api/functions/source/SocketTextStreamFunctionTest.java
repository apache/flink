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

import org.apache.flink.streaming.api.watermark.Watermark;

import org.apache.commons.io.IOUtils;
import org.junit.Test;

import java.io.EOFException;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the {@link org.apache.flink.streaming.api.functions.source.SocketTextStreamFunction}.
 */
public class SocketTextStreamFunctionTest {

	private static final String LOCALHOST = "127.0.0.1";

	@Test
	public void testSocketSourceSimpleOutput() throws Exception {
		ServerSocket server = new ServerSocket(0);
		Socket channel = null;

		try {
			SocketTextStreamFunction source = new SocketTextStreamFunction(LOCALHOST, server.getLocalPort(), "\n", 0);

			SocketSourceThread runner = new SocketSourceThread(source, "test1", "check");
			runner.start();

			channel = server.accept();
			OutputStreamWriter writer = new OutputStreamWriter(channel.getOutputStream());

			writer.write("test1\n");
			writer.write("check\n");
			writer.flush();
			runner.waitForNumElements(2);

			runner.cancel();
			runner.interrupt();

			runner.waitUntilDone();

			channel.close();
		}
		finally {
			if (channel != null) {
				IOUtils.closeQuietly(channel);
			}
			IOUtils.closeQuietly(server);
		}
	}

	@Test
	public void testExitNoRetries() throws Exception {
		ServerSocket server = new ServerSocket(0);
		Socket channel = null;

		try {
			SocketTextStreamFunction source = new SocketTextStreamFunction(LOCALHOST, server.getLocalPort(), "\n", 0);

			SocketSourceThread runner = new SocketSourceThread(source);
			runner.start();

			channel = server.accept();
			channel.close();

			try {
				runner.waitUntilDone();
			}
			catch (Exception e) {
				assertTrue(e.getCause() instanceof EOFException);
			}
		}
		finally {
			if (channel != null) {
				IOUtils.closeQuietly(channel);
			}
			IOUtils.closeQuietly(server);
		}
	}

	@Test
	public void testSocketSourceOutputWithRetries() throws Exception {
		ServerSocket server = new ServerSocket(0);
		Socket channel = null;

		try {
			SocketTextStreamFunction source = new SocketTextStreamFunction(LOCALHOST, server.getLocalPort(), "\n", 10, 100);

			SocketSourceThread runner = new SocketSourceThread(source, "test1", "check");
			runner.start();

			// first connection: nothing
			channel = server.accept();
			channel.close();

			// second connection: first string
			channel = server.accept();
			OutputStreamWriter writer = new OutputStreamWriter(channel.getOutputStream());
			writer.write("test1\n");
			writer.close();
			channel.close();

			// third connection: nothing
			channel = server.accept();
			channel.close();

			// forth connection: second string
			channel = server.accept();
			writer = new OutputStreamWriter(channel.getOutputStream());
			writer.write("check\n");
			writer.flush();

			runner.waitForNumElements(2);
			runner.cancel();
			runner.waitUntilDone();
		}
		finally {
			if (channel != null) {
				IOUtils.closeQuietly(channel);
			}
			IOUtils.closeQuietly(server);
		}
	}

	@Test
	public void testSocketSourceOutputInfiniteRetries() throws Exception {
		ServerSocket server = new ServerSocket(0);
		Socket channel = null;

		try {
			SocketTextStreamFunction source = new SocketTextStreamFunction(LOCALHOST, server.getLocalPort(), "\n", -1, 100);

			SocketSourceThread runner = new SocketSourceThread(source, "test1", "check");
			runner.start();

			// first connection: nothing
			channel = server.accept();
			channel.close();

			// second connection: first string
			channel = server.accept();
			OutputStreamWriter writer = new OutputStreamWriter(channel.getOutputStream());
			writer.write("test1\n");
			writer.close();
			channel.close();

			// third connection: nothing
			channel = server.accept();
			channel.close();

			// forth connection: second string
			channel = server.accept();
			writer = new OutputStreamWriter(channel.getOutputStream());
			writer.write("check\n");
			writer.flush();

			runner.waitForNumElements(2);
			runner.cancel();
			runner.waitUntilDone();
		}
		finally {
			if (channel != null) {
				IOUtils.closeQuietly(channel);
			}
			IOUtils.closeQuietly(server);
		}
	}

	@Test
	public void testSocketSourceOutputAcrossRetries() throws Exception {
		ServerSocket server = new ServerSocket(0);
		Socket channel = null;

		try {
			SocketTextStreamFunction source = new SocketTextStreamFunction(LOCALHOST, server.getLocalPort(), "\n", 10, 100);

			SocketSourceThread runner = new SocketSourceThread(source, "test1", "check1", "check2");
			runner.start();

			// first connection: nothing
			channel = server.accept();
			channel.close();

			// second connection: first string
			channel = server.accept();
			OutputStreamWriter writer = new OutputStreamWriter(channel.getOutputStream());
			writer.write("te");
			writer.close();
			channel.close();

			// third connection: nothing
			channel = server.accept();
			channel.close();

			// forth connection: second string
			channel = server.accept();
			writer = new OutputStreamWriter(channel.getOutputStream());
			writer.write("st1\n");
			writer.write("check1\n");
			writer.write("check2\n");
			writer.flush();

			runner.waitForNumElements(2);
			runner.cancel();
			runner.waitUntilDone();
		}
		finally {
			if (channel != null) {
				IOUtils.closeQuietly(channel);
			}
			IOUtils.closeQuietly(server);
		}
	}

	// ------------------------------------------------------------------------

	private static class SocketSourceThread extends Thread {

		private final Object sync = new Object();

		private final SocketTextStreamFunction socketSource;

		private final String[] expectedData;

		private volatile Throwable error;
		private volatile int numElementsReceived;
		private volatile boolean canceled;
		private volatile boolean done;

		public SocketSourceThread(SocketTextStreamFunction socketSource, String... expectedData) {
			this.socketSource = socketSource;
			this.expectedData = expectedData;
		}

		public void run() {
			try {
				SourceFunction.SourceContext<String> ctx = new SourceFunction.SourceContext<String>() {

					private final Object lock = new Object();

					@Override
					public void collect(String element) {
						int pos = numElementsReceived;

						// make sure waiter know of us
						synchronized (sync) {
							numElementsReceived++;
							sync.notifyAll();
						}

						if (expectedData != null && expectedData.length > pos) {
							assertEquals(expectedData[pos], element);
						}
					}

					@Override
					public void collectWithTimestamp(String element, long timestamp) {
						collect(element);
					}

					@Override
					public void emitWatermark(Watermark mark) {
						throw new UnsupportedOperationException();
					}

					@Override
					public void markAsTemporarilyIdle() {
						throw new UnsupportedOperationException();
					}

					@Override
					public Object getCheckpointLock() {
						return lock;
					}

					@Override
					public void close() {}
				};

				socketSource.run(ctx);
			}
			catch (Throwable t) {
				synchronized (sync) {
					if (!canceled) {
						error = t;
					}
					sync.notifyAll();
				}
			}
			finally {
				synchronized (sync) {
					done = true;
					sync.notifyAll();
				}
			}
		}

		public void cancel() {
			synchronized (sync) {
				canceled = true;
				socketSource.cancel();
				interrupt();
			}
		}

		public void waitForNumElements(int numElements) throws InterruptedException {
			synchronized (sync) {
				while (error == null && !canceled && !done && numElementsReceived < numElements) {
					sync.wait();
				}

				if (error != null) {
					throw new RuntimeException("Error in source thread", error);
				}
				if (canceled) {
					throw new RuntimeException("canceled");
				}
				if (done) {
					throw new RuntimeException("Exited cleanly before expected number of elements");
				}
			}
		}

		public void waitUntilDone() throws InterruptedException {
			join();

			if (error != null) {
				throw new RuntimeException("Error in source thread", error);
			}
		}
	}
}
