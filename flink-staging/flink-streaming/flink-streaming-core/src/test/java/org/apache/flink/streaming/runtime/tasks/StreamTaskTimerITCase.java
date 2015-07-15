/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.operators.Triggerable;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.Semaphore;

/**
 * Tests for the timer service of {@code StreamTask}.
 *
 * <p>
 * These tests ensure that exceptions are properly forwarded from the timer thread to
 * the task thread and that operator methods are not invoked concurrently.
 */
public class StreamTaskTimerITCase extends StreamingMultipleProgramsTestBase {

	/**
	 * Note: this test fails if we don't have the synchronized block in
	 * {@link org.apache.flink.streaming.runtime.tasks.SourceStreamTask.SourceOutput}
	 *
	 * <p>
	 * This test never finishes if exceptions from the timer thread are not forwarded. Thus
	 * a success here means that the exception forwarding works.
	 */
	@Test
	public void testOperatorChainedToSource() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		DataStream<String> source = env.addSource(new InfiniteTestSource());

		source.transform("Custom Operator", BasicTypeInfo.STRING_TYPE_INFO, new TimerOperator(StreamOperator.ChainingStrategy.ALWAYS));

		boolean testSuccess = false;
		try {
			env.execute("Timer test");
		} catch (JobExecutionException e) {
			if (e.getCause() instanceof TimerException) {
				TimerException te = (TimerException) e.getCause();
				if (te.getCause() instanceof RuntimeException) {
					RuntimeException re = (RuntimeException) te.getCause();
					if (re.getMessage().equals("TEST SUCCESS")) {
						testSuccess = true;
					} else {
						throw e;
					}
				} else {
					throw e;
				}
			} else {
				throw e;
			}
		}
		Assert.assertTrue(testSuccess);
	}

	/**
	 * Note: this test fails if we don't have the synchronized block in
	 * {@link org.apache.flink.streaming.runtime.tasks.SourceStreamTask.SourceOutput}
	 */
	@Test
	public void testOneInputOperatorWithoutChaining() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		DataStream<String> source = env.addSource(new InfiniteTestSource());

		source.transform("Custom Operator", BasicTypeInfo.STRING_TYPE_INFO, new TimerOperator(StreamOperator.ChainingStrategy.NEVER));

		boolean testSuccess = false;
		try {
			env.execute("Timer test");
		} catch (JobExecutionException e) {
			if (e.getCause() instanceof TimerException) {
				TimerException te = (TimerException) e.getCause();
				if (te.getCause() instanceof RuntimeException) {
					RuntimeException re = (RuntimeException) te.getCause();
					if (re.getMessage().equals("TEST SUCCESS")) {
						testSuccess = true;
					} else {
						throw e;
					}
				} else {
					throw e;
				}
			} else {
				throw e;
			}
		}
		Assert.assertTrue(testSuccess);
	}

	/**
	 * Note: this test fails if we don't have the synchronized block in
	 * {@link org.apache.flink.streaming.runtime.tasks.SourceStreamTask.SourceOutput}
	 */
	@Test
	public void testTwoInputOperatorWithoutChaining() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		DataStream<String> source = env.addSource(new InfiniteTestSource());

		source.connect(source).transform(
				"Custom Operator",
				BasicTypeInfo.STRING_TYPE_INFO,
				new TwoInputTimerOperator(StreamOperator.ChainingStrategy.NEVER));

		boolean testSuccess = false;
		try {
			env.execute("Timer test");
		} catch (JobExecutionException e) {
			if (e.getCause() instanceof TimerException) {
				TimerException te = (TimerException) e.getCause();
				if (te.getCause() instanceof RuntimeException) {
					RuntimeException re = (RuntimeException) te.getCause();
					if (re.getMessage().equals("TEST SUCCESS")) {
						testSuccess = true;
					} else {
						throw e;
					}
				} else {
					throw e;
				}
			} else {
				throw e;
			}
		}
		Assert.assertTrue(testSuccess);
	}

	public static class TimerOperator extends AbstractStreamOperator<String> implements OneInputStreamOperator<String, String>, Triggerable {
		private static final long serialVersionUID = 1L;

		int numTimers = 0;
		int numElements = 0;

		private boolean first = true;

		private Semaphore semaphore = new Semaphore(1);

		public TimerOperator(ChainingStrategy chainingStrategy) {
			setChainingStrategy(chainingStrategy);
		}

		@Override
		public void processElement(StreamRecord<String> element) throws Exception {
			if (!semaphore.tryAcquire()) {
				Assert.fail("Concurrent invocation of operator functions.");
			}

			if (first) {
				getRuntimeContext().registerTimer(System.currentTimeMillis() + 100, this);
				first = false;
			}
			numElements++;

			semaphore.release();
		}

		@Override
		public void trigger(long time) throws Exception {
			if (!semaphore.tryAcquire()) {
				Assert.fail("Concurrent invocation of operator functions.");
			}

			try {
				numTimers++;
				throwIfDone();
				getRuntimeContext().registerTimer(System.currentTimeMillis() + 1, this);
			} finally {
				semaphore.release();
			}
		}

		private void throwIfDone() {
			if (numTimers > 1000 && numElements > 10_000) {
				throw new RuntimeException("TEST SUCCESS");
			}
		}

		@Override
		public void processWatermark(Watermark mark) throws Exception {
			//ignore
		}
	}

	public static class TwoInputTimerOperator extends AbstractStreamOperator<String> implements TwoInputStreamOperator<String, String, String>, Triggerable {
		private static final long serialVersionUID = 1L;

		int numTimers = 0;
		int numElements = 0;

		private boolean first = true;

		private Semaphore semaphore = new Semaphore(1);

		public TwoInputTimerOperator(ChainingStrategy chainingStrategy) {
			setChainingStrategy(chainingStrategy);
		}

		@Override
		public void processElement1(StreamRecord<String> element) throws Exception {
			if (!semaphore.tryAcquire()) {
				Assert.fail("Concurrent invocation of operator functions.");
			}

			if (first) {
				getRuntimeContext().registerTimer(System.currentTimeMillis() + 100, this);
				first = false;
			}
			numElements++;

			semaphore.release();
		}

		@Override
		public void processElement2(StreamRecord<String> element) throws Exception {
			if (!semaphore.tryAcquire()) {
				Assert.fail("Concurrent invocation of operator functions.");
			}

			if (first) {
				getRuntimeContext().registerTimer(System.currentTimeMillis() + 100, this);
				first = false;
			}
			numElements++;

			semaphore.release();
		}


		@Override
		public void trigger(long time) throws Exception {
			if (!semaphore.tryAcquire()) {
				Assert.fail("Concurrent invocation of operator functions.");
			}

			try {
				numTimers++;
				throwIfDone();
				getRuntimeContext().registerTimer(System.currentTimeMillis() + 1, this);
			} finally {
				semaphore.release();
			}
		}

		private void throwIfDone() {
			if (numTimers > 1000 && numElements > 10_000) {
				throw new RuntimeException("TEST SUCCESS");
			}
		}

		@Override
		public void processWatermark1(Watermark mark) throws Exception {
			//ignore
		}

		@Override
		public void processWatermark2(Watermark mark) throws Exception {
			//ignore
		}
	}


	private static class InfiniteTestSource implements SourceFunction<String> {
		private static final long serialVersionUID = 1L;
		private volatile boolean running = true;

		@Override
		public void run(SourceContext<String> ctx) throws Exception {
			while (running) {
				ctx.collect("hello");
			}
		}

		@Override
		public void cancel() {
			running = false;
		}
	}
}
