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

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.functions.async.buffer.StreamElementEntry;
import org.apache.flink.streaming.api.functions.async.collector.AsyncCollector;
import org.apache.flink.streaming.api.functions.async.buffer.AsyncCollectorBuffer;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
	private final static ExecutorService EXECUTOR_SERVICE = Executors.newFixedThreadPool(10);

	private final Random RANDOM = new Random();

	private AsyncFunction<Integer, Integer> function;

	private AsyncWaitOperator<Integer, Integer> operator;

	private AsyncCollectorBuffer<Integer, Integer> buffer;

	private Output<StreamRecord<Integer>> output;

	private Object lock = new Object();

	public AsyncCollectorBuffer<Integer, Integer> getBuffer(int bufferSize, AsyncDataStream.OutputMode mode) throws Exception {
		function = new AsyncFunction<Integer, Integer>() {
			@Override
			public void asyncInvoke(Integer input, AsyncCollector<Integer> collector) throws Exception {

			}
		};

		operator = new AsyncWaitOperator<>(function, bufferSize, mode);

		StreamConfig cfg = new StreamConfig(new Configuration());
		cfg.setTypeSerializerIn1(IntSerializer.INSTANCE);

		StreamTask<?, ?> mockTask = mock(StreamTask.class);

		when(mockTask.getCheckpointLock()).thenReturn(lock);

		Environment env = new DummyEnvironment("DUMMY;-D", 1, 0);
		when(mockTask.getEnvironment()).thenReturn(env);

		output = new FakedOutput();

		operator.setup(mockTask, cfg, output);

		buffer = operator.getBuffer();

		return buffer;
	}

	@Test
	public void testAdd() throws Exception {
		buffer = getBuffer(3, AsyncDataStream.OutputMode.ORDERED);

		synchronized (lock) {
			buffer.addWatermark(new Watermark(0l));
			buffer.addLatencyMarker(new LatencyMarker(111L, 1, 1));
		}

		Assert.assertEquals(2, buffer.getQueue().size());

		Iterator<StreamElementEntry<Integer>> iterator = buffer.getQueue().iterator();
		Watermark watermark = iterator.next().getStreamElement().asWatermark();
		Assert.assertEquals(0l, watermark.getTimestamp());

		LatencyMarker latencyMarker = iterator.next().getStreamElement().asLatencyMarker();
		Assert.assertEquals(111l, latencyMarker.getMarkedTime());

		buffer.setExtraStreamElement(new Watermark(222l));

		Iterator<StreamElement> elementIterator = buffer.getStreamElementsInBuffer();
		Assert.assertEquals(0l, elementIterator.next().asWatermark().getTimestamp());
		Assert.assertEquals(111l, elementIterator.next().asLatencyMarker().getMarkedTime());
		Assert.assertEquals(222l, elementIterator.next().asWatermark().getTimestamp());
		Assert.assertFalse(elementIterator.hasNext());
	}

	private void work(final boolean throwExcept) throws Exception {
		final int ASYNC_COLLECTOR_NUM = 7;

		Iterator<StreamElement> iterator = new Iterator<StreamElement>() {
			private int idx = 0;

			@Override
			public boolean hasNext() {
				return idx < ASYNC_COLLECTOR_NUM;
			}

			@Override
			public StreamElement next() {
				++idx;

				if (idx == 4) {
					return new Watermark(333l);
				}
				else if (idx == 7) {
					return new LatencyMarker(111L, 0, 0);
				}
				else {
					StreamRecord<Integer> ret = new StreamRecord<>(idx);
					ret.setTimestamp(idx * idx);

					return ret;
				}
			}

			@Override
			public void remove() {
				// do nothing
			}
		};

		while (iterator.hasNext()) {
			final StreamElement record = iterator.next();

			if (record.isRecord()) {
				AsyncCollector tmp;

				synchronized (lock) {
					tmp = buffer.addStreamRecord(record.<Integer>asRecord());
				}

				final AsyncCollector collector = tmp;

				EXECUTOR_SERVICE.submit(new Runnable() {
					@Override
					public void run() {
						try {
							Thread.sleep(RANDOM.nextInt(100));

							if (throwExcept) {
								collector.collect(new Exception("wahahahaha..."));
							}
							else {
								collector.collect(Collections.singletonList(record.asRecord().getValue()));
							}
						} catch (InterruptedException e) {
							// do nothing
						}
					}
				});
			}
			else if (record.isWatermark()) {
				synchronized (lock) {
					buffer.addWatermark(record.asWatermark());
				}
			}
			else {
				synchronized (lock) {
					buffer.addLatencyMarker(record.asLatencyMarker());
				}
			}
		}
	}

	@Test
	public void testOrderedBuffer() throws Exception {
		buffer = getBuffer(3, AsyncDataStream.OutputMode.ORDERED);

		buffer.startEmitterThread();

		work(false);

		synchronized (lock) {
			buffer.waitEmpty();
		}

		buffer.stopEmitterThread();

		Assert.assertEquals("1,2,3,5,6,", ((FakedOutput)output).getValue());
		Assert.assertEquals("1,4,9,333,25,36,111,", ((FakedOutput)output).getTimestamp());
	}

	@Test
	public void testUnorderedBuffer() throws Exception {
		buffer = getBuffer(3, AsyncDataStream.OutputMode.UNORDERED);

		buffer.startEmitterThread();

		work(false);

		synchronized (lock) {
			buffer.waitEmpty();
		}

		buffer.stopEmitterThread();

		Assert.assertEquals(333L, ((FakedOutput)output).getRawTimestamp().toArray()[3]);

		List<Long> result = ((FakedOutput)output).getRawValue();
		Collections.sort(result);
		Assert.assertEquals("[1, 2, 3, 5, 6]", result.toString());

		result = ((FakedOutput)output).getRawTimestamp();
		Collections.sort(result);
		Assert.assertEquals("[1, 4, 9, 25, 36, 111, 333]", result.toString());
	}

	@Test
	public void testOrderedBufferWithManualTriggering() throws Exception {
		// test AsyncCollectorBuffer with different combinations of StreamElements in the buffer.
		// by triggering completion of each AsyncCollector one by one manually, we can verify
		// the output one by one accurately.

		FakedOutput fakedOutput;
		AsyncCollector<Integer> collector1, collector2;

		// 1. head element is a Watermark or LatencyMarker
		buffer = getBuffer(3, AsyncDataStream.OutputMode.ORDERED);
		fakedOutput = (FakedOutput)output;

		fakedOutput.expect(1);

		buffer.startEmitterThread();

		synchronized (lock) {
			buffer.addWatermark(new Watermark(1L));
		}

		fakedOutput.waitToFinish();

		Assert.assertEquals("", fakedOutput.getValue());
		Assert.assertEquals("1,", fakedOutput.getTimestamp());


		fakedOutput.expect(1);

		synchronized (lock) {
			buffer.addLatencyMarker(new LatencyMarker(2L, 0, 0));
		}

		fakedOutput.waitToFinish();

		Assert.assertEquals("", fakedOutput.getValue());
		Assert.assertEquals("1,2,", fakedOutput.getTimestamp());

		synchronized (lock) {
			buffer.waitEmpty();
			buffer.stopEmitterThread();
		}


		// 2. buffer layout: WM -> SR1 -> LM -> SR2, where SR2 finishes first, then SR1.
		buffer = getBuffer(5, AsyncDataStream.OutputMode.ORDERED);
		fakedOutput = (FakedOutput)output;

		synchronized (lock) {
			buffer.addWatermark(new Watermark(1L));
			collector1 = buffer.addStreamRecord(new StreamRecord<>(111, 2L));
			buffer.addLatencyMarker(new LatencyMarker(3L, 0, 0));
			collector2 = buffer.addStreamRecord(new StreamRecord<>(222, 4L));
		}

		fakedOutput.expect(1);

		buffer.startEmitterThread();

		fakedOutput.waitToFinish();

		// in ORDERED mode, the result of completed SR2 will not be emitted right now.
		collector2.collect(Collections.singletonList(222));

		Thread.sleep(1000);

		Assert.assertEquals("", fakedOutput.getValue());
		Assert.assertEquals("1,", fakedOutput.getTimestamp());

		fakedOutput.expect(3);

		collector1.collect(Collections.singletonList(111));

		fakedOutput.waitToFinish();

		Assert.assertEquals("111,222,", fakedOutput.getValue());
		Assert.assertEquals("1,2,3,4,", fakedOutput.getTimestamp());

		synchronized (lock) {
			buffer.waitEmpty();
			buffer.stopEmitterThread();
		}

		// 3. buffer layout: WM -> SR1 -> LM -> S2, where SR1 completes first, then SR2.
		buffer = getBuffer(5, AsyncDataStream.OutputMode.ORDERED);
		fakedOutput = (FakedOutput)output;

		synchronized (lock) {
			buffer.addWatermark(new Watermark(1L));
			collector1 = buffer.addStreamRecord(new StreamRecord<>(111, 2L));
			buffer.addLatencyMarker(new LatencyMarker(3L, 0, 0));
			collector2 = buffer.addStreamRecord(new StreamRecord<>(222, 4L));
		}

		fakedOutput.expect(1);

		buffer.startEmitterThread();

		fakedOutput.waitToFinish();

		fakedOutput.expect(2);

		// in ORDERED mode, the result of completed SR1 will be emitted asap.
		collector1.collect(Collections.singletonList(111));

		fakedOutput.waitToFinish();

		Assert.assertEquals("111,", fakedOutput.getValue());
		Assert.assertEquals("1,2,3,", fakedOutput.getTimestamp());

		fakedOutput.expect(1);

		collector2.collect(Collections.singletonList(222));

		fakedOutput.waitToFinish();

		Assert.assertEquals("111,222,", fakedOutput.getValue());
		Assert.assertEquals("1,2,3,4,", fakedOutput.getTimestamp());

		synchronized (lock) {
			buffer.waitEmpty();
			buffer.stopEmitterThread();
		}

		// 4. buffer layout: SR1 -> SR2 -> WM -> LM, where SR2 finishes first.
		buffer = getBuffer(5, AsyncDataStream.OutputMode.ORDERED);
		fakedOutput = (FakedOutput)output;

		synchronized (lock) {
			collector1 = buffer.addStreamRecord(new StreamRecord<>(111, 1L));
			collector2 = buffer.addStreamRecord(new StreamRecord<>(222, 2L));
			buffer.addWatermark(new Watermark(3L));
			buffer.addLatencyMarker(new LatencyMarker(4L, 0, 0));
		}

		buffer.startEmitterThread();

		// in ORDERED mode, the result of completed SR2 will not be emitted right now.
		collector2.collect(Collections.singletonList(222));

		Thread.sleep(1000);

		Assert.assertEquals("", fakedOutput.getValue());
		Assert.assertEquals("", fakedOutput.getTimestamp());

		fakedOutput.expect(4);

		collector1.collect(Collections.singletonList(111));

		fakedOutput.waitToFinish();

		Assert.assertEquals("111,222,", fakedOutput.getValue());
		Assert.assertEquals("1,2,3,4,", fakedOutput.getTimestamp());

		synchronized (lock) {
			buffer.waitEmpty();
			buffer.stopEmitterThread();
		}
	}

	@Test
	public void testUnorderedWithManualTriggering() throws Exception {
		// verify the output in UNORDERED mode by manual triggering.

		FakedOutput fakedOutput;
		AsyncCollector<Integer> collector1, collector2, collector3;

		// 1. head element is a Watermark or LatencyMarker
		buffer = getBuffer(5, AsyncDataStream.OutputMode.UNORDERED);
		fakedOutput = (FakedOutput)output;

		fakedOutput.expect(1);

		buffer.startEmitterThread();

		synchronized (lock) {
			buffer.addWatermark(new Watermark(1L));
		}

		fakedOutput.waitToFinish();

		Assert.assertEquals("", fakedOutput.getValue());
		Assert.assertEquals("1,", fakedOutput.getTimestamp());


		fakedOutput.expect(1);

		synchronized (lock) {
			buffer.addLatencyMarker(new LatencyMarker(2L, 0, 0));
		}

		fakedOutput.waitToFinish();

		Assert.assertEquals("", fakedOutput.getValue());
		Assert.assertEquals("1,2,", fakedOutput.getTimestamp());

		synchronized (lock) {
			buffer.waitEmpty();
			buffer.stopEmitterThread();
		}


		// 2. buffer layout: LM -> SR1 -> SR2 -> WM1 -> SR3 -> WM2, where the order of completion is SR3, SR2, SR1
		buffer = getBuffer(6, AsyncDataStream.OutputMode.UNORDERED);
		fakedOutput = (FakedOutput)output;

		synchronized (lock) {
			buffer.addLatencyMarker(new LatencyMarker(1L, 0, 0));
			collector1 = buffer.addStreamRecord(new StreamRecord<>(111, 2L));
			collector2 = buffer.addStreamRecord(new StreamRecord<>(222, 3L));
			buffer.addWatermark(new Watermark(4L));
			collector3 = buffer.addStreamRecord(new StreamRecord<>(333, 5L));
			buffer.addWatermark(new Watermark(6L));
		}

		fakedOutput.expect(1);

		buffer.startEmitterThread();

		fakedOutput.waitToFinish();

		// in UNORDERED mode, the result of completed SR3 will not be emitted right now.
		collector3.collect(Collections.singletonList(333));

		Thread.sleep(1000);

		Assert.assertEquals("", fakedOutput.getValue());
		Assert.assertEquals("1,", fakedOutput.getTimestamp());

		fakedOutput.expect(1);

		// SR2 will be emitted
		collector2.collect(Collections.singletonList(222));

		fakedOutput.waitToFinish();

		Assert.assertEquals("222,", fakedOutput.getValue());
		Assert.assertEquals("1,3,", fakedOutput.getTimestamp());

		// SR1 will be emitted first, then WM, and then SR3 and WM2
		fakedOutput.expect(4);
		collector1.collect(Collections.singletonList(111));

		fakedOutput.waitToFinish();

		Assert.assertEquals("222,111,333,", fakedOutput.getValue());
		Assert.assertEquals("1,3,2,4,5,6,", fakedOutput.getTimestamp());

		synchronized (lock) {
			buffer.waitEmpty();
			buffer.stopEmitterThread();
		}

		// 3. buffer layout: WM1 -> SR1 -> SR2 -> LM -> SR3 -> WM2, where the order of completion is SR2, SR1, SR3
		buffer = getBuffer(6, AsyncDataStream.OutputMode.UNORDERED);
		fakedOutput = (FakedOutput)output;

		synchronized (lock) {
			buffer.addWatermark(new Watermark(1L));
			collector1 = buffer.addStreamRecord(new StreamRecord<>(111, 2L));
			collector2 = buffer.addStreamRecord(new StreamRecord<>(222, 3L));
			buffer.addLatencyMarker(new LatencyMarker(4L, 0, 0));
			collector3 = buffer.addStreamRecord(new StreamRecord<>(333, 5L));
			buffer.addWatermark(new Watermark(6L));
		}

		// the result of SR2 will be emitted following WM1
		collector2.collect(Collections.singletonList(222));

		fakedOutput.expect(2);

		buffer.startEmitterThread();

		fakedOutput.waitToFinish();

		Assert.assertEquals("222,", fakedOutput.getValue());
		Assert.assertEquals("1,3,", fakedOutput.getTimestamp());

		// SR1 and LM will be emitted
		fakedOutput.expect(2);
		collector1.collect(Collections.singletonList(111));

		fakedOutput.waitToFinish();

		Assert.assertEquals("222,111,", fakedOutput.getValue());
		Assert.assertEquals("1,3,2,4,", fakedOutput.getTimestamp());

		// SR3 and WM2 will be emitted
		fakedOutput.expect(2);
		collector3.collect(Collections.singletonList(333));

		fakedOutput.waitToFinish();

		Assert.assertEquals("222,111,333,", fakedOutput.getValue());
		Assert.assertEquals("1,3,2,4,5,6,", fakedOutput.getTimestamp());

		synchronized (lock) {
			buffer.waitEmpty();
			buffer.stopEmitterThread();
		}

	}



	@Test
	public void testBufferWithException() throws Exception {
		buffer = getBuffer(3, AsyncDataStream.OutputMode.UNORDERED);

		buffer.startEmitterThread();

		IOException expected = null;
		try {
			work(true);
		}
		catch (IOException e) {
			expected = e;
		}

		Assert.assertNotNull(expected);
		Assert.assertEquals(expected.getMessage(), "wahahahaha...");

		synchronized (lock) {
			buffer.waitEmpty();
		}

		buffer.stopEmitterThread();
	}

	public class FakedOutput implements Output<StreamRecord<Integer>> {
		private List<Long> outputs;
		private List<Long> timestamps;

		private CountDownLatch latch;

		public FakedOutput() {
			this.outputs = new ArrayList<>();
			this.timestamps = new ArrayList<>();
		}

		@Override
		public void collect(StreamRecord<Integer> record) {
			outputs.add(record.getValue().longValue());
			if (record.hasTimestamp()) {
				timestamps.add(record.getTimestamp());
			}

			if (latch != null) {
				latch.countDown();
			}
		}

		@Override
		public void emitWatermark(Watermark mark) {
			timestamps.add(mark.getTimestamp());

			if (latch != null) {
				latch.countDown();
			}
		}

		@Override
		public void emitLatencyMarker(LatencyMarker latencyMarker) {
			timestamps.add(latencyMarker.getMarkedTime());

			if (latch != null) {
				latch.countDown();
			}
		}

		@Override
		public void close() {
		}

		public String getValue() {
			StringBuilder sb = new StringBuilder();
			for (Long i : outputs) {
				sb.append(i).append(",");
			}
			return sb.toString();
		}

		public String getTimestamp() {
			StringBuilder sb = new StringBuilder();
			for (Long i : timestamps) {
				sb.append(i).append(",");
			}
			return sb.toString();
		}

		public List<Long> getRawValue() {
			return outputs;
		}

		public List<Long> getRawTimestamp() {
			return timestamps;
		}

		public void expect(int count) {
			latch = new CountDownLatch(count);
		}

		public void waitToFinish() throws InterruptedException {
			latch.await();
		}
	}
}
