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
import java.util.Iterator;
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

	private TimestampedCollector<Integer> collector;

	private Object lock = new Object();

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
		collector =new TimestampedCollector(output);

		Whitebox.setInternalState(operator, "output", output);
	}

	@Test
	public void testAdd() throws Exception {
		buffer =
				new AsyncCollectorBuffer<>(3, AsyncDataStream.OutputMode.UNORDERED, output, collector, lock, operator);

		buffer.addWatermark(new Watermark(0l));
		buffer.addLatencyMarker(new LatencyMarker(111L, 1, 1));
		Assert.assertEquals(buffer.getQueue().size(), 2);

		Iterator<Map.Entry<AsyncCollector<Integer, Integer>, StreamElement>> iterator =
				buffer.getQueue().entrySet().iterator();
		Watermark watermark = iterator.next().getValue().asWatermark();
		Assert.assertEquals(watermark.getTimestamp(), 0l);

		LatencyMarker latencyMarker = iterator.next().getValue().asLatencyMarker();
		Assert.assertEquals(latencyMarker.getMarkedTime(), 111l);

		List<StreamElement> elements = buffer.getStreamElementsInBuffer();
		Assert.assertEquals(elements.size(), 2);
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
			if (record.hasTimestamp()) {
				sb.append(record.getTimestamp() + ",");
			}
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
			for (Long i : outputs) {
				sb.append(i).append(",");
			}
			return sb.toString();
		}

		public Collection<Long> getOutputs() {
			return outputs;
		}

		public String getTimestamp() {
			return this.sb.toString();
		}
	}

	private void work(final boolean throwExcept) throws Exception {
		final int ASYNC_COLLECTOR_NUM = 7;

		final int[] orderedSeq = new int[] {0, 1, 2, 3, 4, 5, 6};
		final int[] sleepTimeArr = new int[] {5, 7, 3, 0, 1, 9, 9};

		ExecutorService service = Executors.newFixedThreadPool(10);

		for (int idx = 0; idx < ASYNC_COLLECTOR_NUM; ++idx) {
			int i = orderedSeq[idx];
			final int sleepTS = sleepTimeArr[idx]*100;

			if (i == 3) {
				synchronized (lock) {
					buffer.addWatermark(new Watermark(333l));
				}
			}
			else if (i == 6) {
				synchronized (lock) {
					buffer.addLatencyMarker(new LatencyMarker(111L, 0, 0));
				}
			}
			else {
				StreamRecord record = new StreamRecord(i);
				record.setTimestamp(i*i);

				AsyncCollector tmp;
				synchronized (lock) {
					tmp = buffer.addStreamRecord(record);
				}

				final AsyncCollector collector = tmp;

				final int v = i;
				service.submit(new Runnable() {
					@Override
					public void run() {
						try {
							int sleep = sleepTS;
							Thread.sleep(sleep);

							if (throwExcept) {
								collector.collect(new Exception("wahahahaha..."));
							}
							else {
								List<Integer> ret = new ArrayList<Integer>();

								ret.add(v);
								collector.collect(ret);
							}
						} catch (InterruptedException e) {
						}
					}
				});
			}
		}

		synchronized (lock) {
			buffer.waitEmpty();
		}
	}

	@Test
	public void testOrderedBuffer() throws Exception {
		buffer =
				new AsyncCollectorBuffer<>(3, AsyncDataStream.OutputMode.ORDERED, output, collector, lock, operator);
		buffer.startEmitterThread();

		work(false);

		buffer.waitEmpty();
		buffer.stopEmitterThread();

		Assert.assertEquals(((FakedOutput)output).getResult(), "0,1,2,333,4,5,111,");
		Assert.assertEquals(((FakedOutput)output).getTimestamp(), "0,1,4,16,25,");
	}

	private String sort(String s) {
		String[] arr = s.split(",");
		int[] ints = new int[arr.length];
		for (int i = 0; i < arr.length; ++i) {
			ints[i] = Integer.valueOf(arr[i]);
		}
		Arrays.sort(ints);
		return Arrays.toString(ints);
	}

	@Test
	public void testUnorderedBuffer() throws Exception {
		buffer =
				new AsyncCollectorBuffer<>(3, AsyncDataStream.OutputMode.UNORDERED, output, collector, lock, operator);
		buffer.startEmitterThread();

		work(false);

		buffer.waitEmpty();
		buffer.stopEmitterThread();

		Assert.assertEquals(((FakedOutput)output).getOutputs().toArray()[3], 333L);
		Assert.assertEquals(sort(((FakedOutput)output).getResult()), "[0, 1, 2, 4, 5, 111, 333]");
		Assert.assertEquals(sort(((FakedOutput)output).getTimestamp()), "[0, 1, 4, 16, 25]");
	}

	@Test(expected = IOException.class)
	public void testBufferWithException() throws Exception {
		buffer =
				new AsyncCollectorBuffer<>(3, AsyncDataStream.OutputMode.UNORDERED, output, collector, lock, operator);
		buffer.startEmitterThread();

		work(true);

		buffer.waitEmpty();
	}
}
