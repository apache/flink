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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.runtime.io.disk.iomanager.BufferFileWriter;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.io.network.util.TestConsumerCallback;
import org.apache.flink.runtime.io.network.util.TestInfiniteBufferProvider;
import org.apache.flink.runtime.io.network.util.TestPooledBufferProvider;
import org.apache.flink.runtime.io.network.util.TestSubpartitionConsumer;
import org.junit.AfterClass;
import org.junit.Test;

import static org.mockito.Mockito.mock;

public class SpilledSubpartitionViewAsyncIOTest {

	private static final IOManager ioManager = new IOManagerAsync();

	@AfterClass
	public static void shutdown() {
		ioManager.shutdown();
	}

	@Test
	public void testWriteConsume() throws Exception {
		// Config
		final int numberOfBuffersToWrite = 1024;

		// Setup
		final BufferFileWriter writer = SpilledSubpartitionViewTest
				.createWriterAndWriteBuffers(ioManager, new TestInfiniteBufferProvider(), numberOfBuffersToWrite);

		writer.close();

		final TestPooledBufferProvider viewBufferPool = new TestPooledBufferProvider(1);

		final SpilledSubpartitionViewAsyncIO view = new SpilledSubpartitionViewAsyncIO(
				mock(ResultSubpartition.class), viewBufferPool, ioManager,
				writer.getChannelID(), 0);

		final TestSubpartitionConsumer consumer = new TestSubpartitionConsumer(view, false,
				new TestConsumerCallback.RecyclingCallback());

		// Consume subpartition
		consumer.call();
	}
}
