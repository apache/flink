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

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.streaming.runtime.io.BarrierBuffer;
import org.apache.flink.streaming.runtime.io.CoRecordReader;
import org.apache.flink.streaming.runtime.io.BarrierBufferTest.MockInputGate;
import org.junit.Test;

public class CoRecordReaderTest {

	@Test
	public void test() throws InterruptedException, IOException {

		List<BufferOrEvent> input1 = new LinkedList<BufferOrEvent>();
		input1.add(BarrierBufferTest.createBuffer(0));
		input1.add(BarrierBufferTest.createSuperstep(1, 0));
		input1.add(BarrierBufferTest.createBuffer(0));

		InputGate ig1 = new MockInputGate(1, input1);

		List<BufferOrEvent> input2 = new LinkedList<BufferOrEvent>();
		input2.add(BarrierBufferTest.createBuffer(0));
		input2.add(BarrierBufferTest.createBuffer(0));
		input2.add(BarrierBufferTest.createSuperstep(1, 0));
		input2.add(BarrierBufferTest.createBuffer(0));

		InputGate ig2 = new MockInputGate(1, input2);

		CoRecordReader<?, ?> coReader = new CoRecordReader<IOReadableWritable, IOReadableWritable>(
				ig1, ig2);
		BarrierBuffer b1 = coReader.barrierBuffer1;
		BarrierBuffer b2 = coReader.barrierBuffer2;

		coReader.addToAvailable(ig1);
		coReader.addToAvailable(ig2);
		coReader.addToAvailable(ig2);
		coReader.addToAvailable(ig1);

		assertEquals(1, coReader.getNextReaderIndexBlocking());
		b1.getNextNonBlocked();

		assertEquals(2, coReader.getNextReaderIndexBlocking());
		b2.getNextNonBlocked();

		assertEquals(2, coReader.getNextReaderIndexBlocking());
		b2.getNextNonBlocked();

		assertEquals(1, coReader.getNextReaderIndexBlocking());
		b1.getNextNonBlocked();
		b1.processSuperstep(input1.get(1));

		coReader.addToAvailable(ig1);
		coReader.addToAvailable(ig2);
		coReader.addToAvailable(ig2);

		assertEquals(2, coReader.getNextReaderIndexBlocking());
		b2.getNextNonBlocked();
		b2.processSuperstep(input2.get(2));

		assertEquals(1, coReader.getNextReaderIndexBlocking());
		b1.getNextNonBlocked();

		assertEquals(2, coReader.getNextReaderIndexBlocking());
		b2.getNextNonBlocked();
	}

}
