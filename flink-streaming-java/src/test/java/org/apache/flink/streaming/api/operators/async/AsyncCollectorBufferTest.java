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

package org.apache.flink.streaming.api.operators.async;

import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Tests for {@link AsyncCollectorBuffer}. These test that:
 *
 * <ul>
 *     <li>Add a new item into the buffer</li>
 *     <li>Ordered mode processing</li>
 *     <li>Unordered mode processing</li>
 *     <li>Error handling</li>
 * </ul>
 */
public class AsyncCollectorBufferTest {
	private AsyncFunction<Integer, Integer> function;

	private AsyncWaitOperator<Integer, Integer> operator;

	private AsyncCollectorBuffer<Integer, Integer> buffer;

	private Output<StreamRecord<Integer>> output;

	@Before
	public void setUp() throws Exception {
		function = new AsyncFunction<Integer, Integer>() {
			@Override
			public void asyncInvoke(Integer input, AsyncCollector<Integer, Integer> collector) throws Exception {

			}
		};

		operator = new AsyncWaitOperator<>(function);
		Class<?>[] classes = AbstractStreamOperator.class.getDeclaredClasses();
		Class<?> latencyClass = null;
		for (Class<?> c : classes) {
			if (c.getName().indexOf("LatencyGauge") != -1) {
				latencyClass = c;
			}
		}

		Constructor<?> explicitConstructor = latencyClass.getDeclaredConstructors()[0];
		explicitConstructor.setAccessible(true);
		Whitebox.setInternalState(operator, "latencyGauge", explicitConstructor.newInstance(10));

		output = new FakedOutput(new ArrayList<Long>());
		TimestampedCollector<Integer> collector =new TimestampedCollector(output);
		buffer =
			new AsyncCollectorBuffer<>(3, AsyncDataStream.OutputMode.ORDERED, operator);
		buffer.setOutput(collector, output);

		Whitebox.setInternalState(operator, "output", output);
	}

	@Test
	public void testAdd() throws Exception {
		Thread.sleep(1000);
		buffer.add(new Watermark(0l));
		buffer.add(new LatencyMarker(111L, 1, 1));
		Assert.assertEquals(((SimpleLinkedList) Whitebox.getInternalState(buffer, "queue")).size(), 2);
		Assert.assertEquals(((Map) Whitebox.getInternalState(buffer, "collectorToStreamElement")).size(), 2);
		Assert.assertEquals(((Map) Whitebox.getInternalState(buffer, "collectorToQueue")).size(), 2);

		AsyncCollector collector = (AsyncCollector)((SimpleLinkedList) Whitebox.getInternalState(buffer, "queue")).get(0);
		Watermark ret = ((StreamElement)((Map) Whitebox.getInternalState(buffer, "collectorToStreamElement")).get(collector)).asWatermark();
		Assert.assertEquals(ret.getTimestamp(), 0l);

		AsyncCollector collector2 = (AsyncCollector)((SimpleLinkedList) Whitebox.getInternalState(buffer, "queue")).get(1);
		LatencyMarker latencyMarker = ((StreamElement)((Map) Whitebox.getInternalState(buffer, "collectorToStreamElement")).get(collector2)).asLatencyMarker();
		Assert.assertEquals(latencyMarker.getMarkedTime(), 111l);

		SimpleLinkedList list = (SimpleLinkedList) Whitebox.getInternalState(buffer, "queue");
		Assert.assertEquals(list.node(0), ((Map) Whitebox.getInternalState(buffer, "collectorToQueue")).get(collector));

		List<StreamElement> elements = buffer.getStreamElementsInBuffer();
		Assert.assertEquals(elements.size(), 2);
	}

	public class OrderedPutThread extends Thread {
		final int ASYNC_COLLECTOR_NUM = 7;
		int[] orderedSeq = new int[] {0, 1, 2, 3, 4, 5, 6};
		int[] sleepTimeArr = new int[] {5, 7, 3, 0, 1, 9, 9};

		AsyncCollectorBuffer<Integer, Integer> buffer;
		ExecutorService service = Executors.newFixedThreadPool(10);

		boolean throwExcept = false;
		boolean orderedMode = false;

		public OrderedPutThread(AsyncCollectorBuffer buffer, boolean except, boolean orderedMode) {
			this.buffer = buffer;
			this.throwExcept = except;
			this.orderedMode = orderedMode;
		}

		public OrderedPutThread(AsyncCollectorBuffer buffer, boolean except) {
			this(buffer, except, true);
		}

		public OrderedPutThread(AsyncCollectorBuffer buffer) {
			this(buffer, false);
		}

		@Override
		public void run() {
			try {
				for (int idx = 0; idx < ASYNC_COLLECTOR_NUM; ++idx) {
					int i = orderedSeq[idx];
					final int sleepTS = sleepTimeArr[idx]*1000;

					if (i == 3)
						buffer.add(new Watermark(333l));
					else if (i == 6)
						buffer.add(new LatencyMarker(111L, 0, 0));
					else {
						StreamRecord record = new StreamRecord(i);
						record.setTimestamp(i*i);
						final AsyncCollector collector = buffer.add(record);

						final int v = i;
						service.submit(new Runnable() {
							@Override
							public void run() {
								try {
									int sleep = sleepTS;
									Thread.sleep(sleep);
									if (throwExcept)
										collector.collect(new Exception("wahahaha..."));
									else {
										List<Integer> ret = new ArrayList<Integer>();
										ret.add(v);
										collector.collect(ret);
									}
								} catch (Exception e) {
									e.printStackTrace();
								}
							}
						});
					}
				}
			}
			catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public class FakedOutput implements Output<StreamRecord<Integer>> {
		private Collection<Long> outputs;
		private StringBuilder sb = new StringBuilder();

		public FakedOutput(Collection<Long> outputs) {
			this.outputs = outputs;
		}

		@Override
		public void collect(StreamRecord<Integer> record) {
			outputs.add(record.getValue().longValue());
			if (record.hasTimestamp())
				sb.append(record.getTimestamp()+",");
		}

		@Override
		public void emitWatermark(Watermark mark) {
			outputs.add(mark.getTimestamp());
		}

		@Override
		public void emitLatencyMarker(LatencyMarker latencyMarker) {
			outputs.add(latencyMarker.getMarkedTime());
		}

		@Override
		public void close() {
		}

		public String getResult() {
			StringBuilder sb = new StringBuilder();
			for (Long i : outputs)
				sb.append(i).append(",");
			return sb.toString();
		}

		public String getTimestamp() {
			return this.sb.toString();
		}
	}

	@Test
	public void testOrderedBuffer() throws Exception {
		buffer.startEmitterThread();
		Watermark watermark = new Watermark(0L);
		buffer.add(watermark);

		OrderedPutThread orderedPutThread = new OrderedPutThread(buffer);
		Thread.sleep(2000);

		orderedPutThread.start();

		orderedPutThread.join();

		buffer.waitEmpty();

		buffer.stopEmitterThread();

		Assert.assertEquals(((FakedOutput)output).getResult(), "0,0,1,2,333,4,5,111,");
		Assert.assertEquals(((FakedOutput)output).getTimestamp(), "0,1,4,16,25,");
	}

	private String sort(String s) {
		String[] arr = s.split(",");
		int[] ints = new int[arr.length];
		for (int i = 0; i < arr.length; ++i)
			ints[i] = Integer.valueOf(arr[i]);
		Arrays.sort(ints);
		return Arrays.toString(ints);
	}

	@Test
	public void testUnorderedBuffer() throws Exception {
		buffer.startEmitterThread();
		Watermark watermark = new Watermark(0L);
		buffer.add(watermark);

		OrderedPutThread orderedPutThread = new OrderedPutThread(buffer, false, false);
		Thread.sleep(2000);

		orderedPutThread.start();

		orderedPutThread.join();

		buffer.waitEmpty();

		buffer.stopEmitterThread();

		Assert.assertEquals(sort(((FakedOutput)output).getResult()), "[0, 0, 1, 2, 4, 5, 111, 333]");
		Assert.assertEquals(sort(((FakedOutput)output).getTimestamp()), "[0, 1, 4, 16, 25]");
	}

	@Test(expected = IOException.class)
	public void testBufferWithException() throws Exception {
		buffer.startEmitterThread();

		OrderedPutThread putThread = new OrderedPutThread(buffer, true);
		Thread.sleep(2000);

		putThread.start();

		putThread.join();

		buffer.waitEmpty();
		buffer.stopEmitterThread();
	}
}
