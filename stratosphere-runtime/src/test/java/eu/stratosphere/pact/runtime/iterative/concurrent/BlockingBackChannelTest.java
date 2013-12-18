/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.pact.runtime.iterative.concurrent;

import com.google.common.collect.Lists;

import eu.stratosphere.core.memory.DataInputView;
import eu.stratosphere.core.memory.DataOutputView;
import eu.stratosphere.pact.runtime.iterative.io.SerializedUpdateBuffer;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static org.junit.Assert.assertEquals;

public class BlockingBackChannelTest {

	private static final int NUM_ITERATIONS = 3;

	private static final Integer INPUT_COMPLETELY_PROCESSED_MESSAGE = 1;

	@Test
	public void multiThreaded() throws InterruptedException {

		BlockingQueue<Integer> dataChannel = new ArrayBlockingQueue<Integer>(1);
		List<String> actionLog = Lists.newArrayList();

		SerializedUpdateBuffer buffer = Mockito.mock(SerializedUpdateBuffer.class);
		BlockingBackChannel channel = new BlockingBackChannel(buffer);

		Thread head = new Thread(new IterationHead(channel, dataChannel, actionLog));
		Thread tail = new Thread(new IterationTail(channel, dataChannel, actionLog));

		tail.start();
		head.start();

		head.join();
		tail.join();

//		int action = 0;
//		for (String log : actionLog) {
//			System.out.println("ACTION " + (++action) + ": " + log);
//		}

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

		private final BlockingQueue<Integer> dataChannel;

		private final Random random;

		private final List<String> actionLog;

		IterationHead(BlockingBackChannel backChannel, BlockingQueue<Integer> dataChannel, List<String> actionLog) {
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
					backChannel.getReadEndAfterSuperstepEnded();
					actionLog.add("head reads in iteration " + n);
					Thread.sleep(random.nextInt(100));
					// we don't send through the data channel in the last iteration, we would send to the output task
					if (n != NUM_ITERATIONS - 1) {
						processInputAndSendMessageThroughDataChannel();
					}
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			}
		}

		void processInputAndSendMessageThroughDataChannel() {
			actionLog.add("head sends data");
			dataChannel.offer(INPUT_COMPLETELY_PROCESSED_MESSAGE);
		}
	}

	class IterationTail implements Runnable {

		private final BlockingBackChannel backChannel;

		private final BlockingQueue<Integer> dataChannel;

		private final Random random;

		private final List<String> actionLog;

		IterationTail(BlockingBackChannel backChannel, BlockingQueue<Integer> dataChannel, List<String> actionLog) {
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
					readInputFromDataChannel();
					Thread.sleep(random.nextInt(10));
					DataInputView inputView = Mockito.mock(DataInputView.class);
					actionLog.add("tail writes in iteration " + n);
					writeEnd.write(inputView, 1);
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
