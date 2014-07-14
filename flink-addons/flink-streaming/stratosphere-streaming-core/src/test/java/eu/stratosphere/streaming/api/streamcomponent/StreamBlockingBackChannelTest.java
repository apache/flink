/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.streaming.api.streamcomponent;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.junit.Test;

import com.google.common.collect.Lists;

import eu.stratosphere.api.java.tuple.Tuple;
import eu.stratosphere.api.java.tuple.Tuple1;
import eu.stratosphere.api.java.typeutils.TupleTypeInfo;
import eu.stratosphere.api.java.typeutils.TypeExtractor;
import eu.stratosphere.api.java.typeutils.runtime.TupleSerializer;
import eu.stratosphere.core.memory.DataInputView;
import eu.stratosphere.core.memory.DataOutputView;
import eu.stratosphere.core.memory.MemorySegment;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryAllocationException;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.services.memorymanager.spi.DefaultMemoryManager;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.pact.runtime.iterative.concurrent.BlockingBackChannel;
import eu.stratosphere.pact.runtime.iterative.concurrent.Broker;
import eu.stratosphere.pact.runtime.iterative.io.SerializedUpdateBuffer;
import eu.stratosphere.pact.runtime.plugable.DeserializationDelegate;
import eu.stratosphere.pact.runtime.plugable.SerializationDelegate;
import eu.stratosphere.streaming.api.streamrecord.ArrayStreamRecord;
import eu.stratosphere.streaming.api.streamrecord.StreamRecord;

public class StreamBlockingBackChannelTest {

	private static final int NUM_ITERATIONS = 3;
	TupleSerializer<Tuple> ts = (TupleSerializer<Tuple>) (new TupleTypeInfo<Tuple>(
			TypeExtractor.getForClass(String.class))).createSerializer();
	SerializationDelegate<Tuple> sd = new SerializationDelegate<Tuple>(ts);
	DeserializationDelegate<Tuple> dd = new DeserializationDelegate<Tuple>(ts);
	private StreamRecord message = new ArrayStreamRecord(1).setId(1).setTuple(0,
			new Tuple1<String>("hi"));

	@Test
	public void multiThreaded() throws InterruptedException, MemoryAllocationException {

		
		
		message.setSeralizationDelegate(sd);
		BlockingQueue<StreamRecord> dataChannel = new ArrayBlockingQueue<StreamRecord>(1);
		
		BlockingQueueBroker.instance().handIn("dc", dataChannel);
		
		BlockingQueue<StreamRecord> dc=BlockingQueueBroker.instance().getAndRemove("dc");

		
		List<String> actionLog = Lists.newArrayList();
		List<MemorySegment> segments = new ArrayList<MemorySegment>();
		IOManager io = new IOManager();
		MemoryManager mm = new DefaultMemoryManager(1024000000, 1,  8192);
		mm.allocatePages(new AbstractInvokable() {

			@Override
			public void registerInputOutput() {
				// TODO Auto-generated method stub

			}

			@Override
			public void invoke() throws Exception {
				// TODO Auto-generated method stub

			}
		}, segments, mm.getPageSize());

		SerializedUpdateBuffer buffer = new SerializedUpdateBuffer(segments, mm.getPageSize(), io);
		BlockingBackChannel channel = new BlockingBackChannel(buffer);

		Thread head = new Thread(new IterationHead(channel, dc, actionLog));
		Thread tail = new Thread(new IterationTail(channel, dc, actionLog));

		tail.start();
		head.start();

		head.join();
		tail.join();

		// int action = 0;
		// for (String log : actionLog) {
		// System.out.println("ACTION " + (++action) + ": " + log);
		// }

		assertEquals(12, actionLog.size());

		assertEquals("head sends data", actionLog.get(0));
		assertEquals("tail receives data", actionLog.get(1));
		assertEquals("tail writes in iteration 0", actionLog.get(2));
		assertEquals("head reads in iteration 0", actionLog.get(3));
		assertEquals("head sends data", actionLog.get(4));
		assertEquals("tail receives data", actionLog.get(5));
		assertEquals("tail writes in iteration 1", actionLog.get(6));
		assertEquals("head reads in iteration 1", actionLog.get(7));
		assertEquals("head sends data", actionLog.get(8));
		assertEquals("tail receives data", actionLog.get(9));
		assertEquals("tail writes in iteration 2", actionLog.get(10));
		assertEquals("head reads in iteration 2", actionLog.get(11));
	}

	class IterationHead implements Runnable {

		private final BlockingBackChannel backChannel;

		private final BlockingQueue<StreamRecord> dataChannel;

		private final Random random;

		private final List<String> actionLog;

		IterationHead(BlockingBackChannel backChannel, BlockingQueue<StreamRecord> dataChannel,
				List<String> actionLog) {
			this.backChannel = backChannel;
			this.dataChannel = dataChannel;
			this.actionLog = actionLog;
			random = new Random();
		}

		@Override
		public void run() {
			processInputAndSendMessageThroughDataChannel();
			for (int n = 0; n < NUM_ITERATIONS; n++) {
				try {
					
					DataInputView di = backChannel.getReadEndAfterSuperstepEnded();
					StreamRecord sr = new ArrayStreamRecord();
					sr.setDeseralizationDelegate(dd, ts);
					sr.read(di);
					System.out.println(sr);
					System.out.println(di.readInt());
					actionLog.add("head reads in iteration " + n);
					Thread.sleep(random.nextInt(100));
					// we don't send through the data channel in the last
					// iteration, we would send to the output task
					if (n != NUM_ITERATIONS - 1) {
						processInputAndSendMessageThroughDataChannel();
					}
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

		void processInputAndSendMessageThroughDataChannel() {
			actionLog.add("head sends data");
			dataChannel.offer(message);
		}
	}

	class IterationTail implements Runnable {

		private final BlockingBackChannel backChannel;

		private final BlockingQueue<StreamRecord> dataChannel;

		private final Random random;

		private final List<String> actionLog;

		IterationTail(BlockingBackChannel backChannel, BlockingQueue<StreamRecord> dataChannel,
				List<String> actionLog) {
			this.backChannel = backChannel;
			this.dataChannel = dataChannel;
			this.actionLog = actionLog;
			random = new Random();
		}

		@Override
		public void run() {
			try {
				for (int n = 0; n < NUM_ITERATIONS; n++) {
					DataOutputView writeEnd = backChannel.getWriteEnd();
					message.write(writeEnd);
					writeEnd.writeInt(1);
					readInputFromDataChannel();
					Thread.sleep(random.nextInt(10));
					// DataInputView inputView =
					// Mockito.mock(DataInputView.class);
					actionLog.add("tail writes in iteration " + n);
					// writeEnd.write(inputView, 1);
					backChannel.notifyOfEndOfSuperstep();
				}
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}

		void readInputFromDataChannel() throws InterruptedException {
			dataChannel.take();
			actionLog.add("tail receives data");
		}
	}
	

}
