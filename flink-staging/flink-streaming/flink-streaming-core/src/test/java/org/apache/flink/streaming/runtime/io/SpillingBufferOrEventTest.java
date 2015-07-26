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

package org.apache.flink.streaming.runtime.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class SpillingBufferOrEventTest {
	
	private static IOManager IO_MANAGER;
	
	@BeforeClass
	public static void createIOManager() {
		IO_MANAGER = new IOManagerAsync();
	}
	
	@AfterClass
	public static void shutdownIOManager() {
		IO_MANAGER.shutdown();
	}

	// ------------------------------------------------------------------------
	
	@Test
	public void testSpilling() throws IOException, InterruptedException {
		BufferSpiller bsp = new BufferSpiller(IO_MANAGER);
		SpillReader spr = new SpillReader();

		BufferPool pool1 = new NetworkBufferPool(10, 256).createBufferPool(2, true);
		BufferPool pool2 = new NetworkBufferPool(10, 256).createBufferPool(2, true);

		Buffer b1 = pool1.requestBuffer();
		b1.getMemorySegment().putInt(0, 10000);
		BufferOrEvent boe1 = new BufferOrEvent(b1, 2);
		SpillingBufferOrEvent sboe1 = new SpillingBufferOrEvent(boe1, bsp, spr);

		assertTrue(sboe1.isSpilled());

		Buffer b2 = pool2.requestBuffer();
		b2.getMemorySegment().putInt(0, 10000);
		BufferOrEvent boe2 = new BufferOrEvent(b2, 4);
		SpillingBufferOrEvent sboe2 = new SpillingBufferOrEvent(boe2, bsp, spr);

		assertTrue(sboe2.isSpilled());

		Buffer b3 = pool1.requestBuffer();
		b3.getMemorySegment().putInt(0, 50000);
		BufferOrEvent boe3 = new BufferOrEvent(b3, 0);
		SpillingBufferOrEvent sboe3 = new SpillingBufferOrEvent(boe3, bsp, spr);

		assertTrue(sboe3.isSpilled());

		Buffer b4 = pool2.requestBuffer();
		b4.getMemorySegment().putInt(0, 60000);
		BufferOrEvent boe4 = new BufferOrEvent(b4, 0);
		SpillingBufferOrEvent sboe4 = new SpillingBufferOrEvent(boe4, bsp, spr);

		assertTrue(sboe4.isSpilled());

		bsp.close();

		spr.setSpillFile(bsp.getSpillFile());

		Buffer b1ret = sboe1.getBufferOrEvent().getBuffer();
		assertEquals(10000, b1ret.getMemorySegment().getInt(0));
		assertEquals(2, sboe1.getBufferOrEvent().getChannelIndex());
		b1ret.recycle();

		Buffer b2ret = sboe2.getBufferOrEvent().getBuffer();
		assertEquals(10000, b2ret.getMemorySegment().getInt(0));
		assertEquals(4, sboe2.getBufferOrEvent().getChannelIndex());
		b2ret.recycle();

		Buffer b3ret = sboe3.getBufferOrEvent().getBuffer();
		assertEquals(50000, b3ret.getMemorySegment().getInt(0));
		assertEquals(0, sboe3.getBufferOrEvent().getChannelIndex());
		b3ret.recycle();

		Buffer b4ret = sboe4.getBufferOrEvent().getBuffer();
		assertEquals(60000, b4ret.getMemorySegment().getInt(0));
		b4ret.recycle();

		spr.close();
		bsp.getSpillFile().delete();

	}
}
