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

package org.apache.flink.streaming.api.functions.source.datagen;

import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.functions.StatefulSequenceSourceTest.BlockingSourceContext;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tests for {@link DataGeneratorSource}.
 */
public class DataGeneratorSourceTest {

	@Test
	public void testRandomGenerator() throws Exception {
		long min = 10;
		long max = 20;
		DataGeneratorSource<Long> source = new DataGeneratorSource<>(
				RandomGenerator.longGenerator(min, max));
		StreamSource<Long, DataGeneratorSource<Long>> src = new StreamSource<>(source);
		AbstractStreamOperatorTestHarness<Long> testHarness =
				new AbstractStreamOperatorTestHarness<>(src, 1, 1, 0);
		testHarness.open();

		int totalNumber = 1000;
		List<Long> results = new ArrayList<>();

		source.run(new SourceFunction.SourceContext<Long>() {

			private Object lock = new Object();
			private int emitNumber = 0;

			@Override
			public void collect(Long element) {
				if (++emitNumber > totalNumber) {
					source.isRunning = false;
				}
				results.add(element);
			}

			@Override
			public void collectWithTimestamp(Long element, long timestamp) {
			}

			@Override
			public void emitWatermark(Watermark mark) {
			}

			@Override
			public void markAsTemporarilyIdle() {
			}

			@Override
			public Object getCheckpointLock() {
				return lock;
			}

			@Override
			public void close() {
			}
		});

		for (Long l : results) {
			Assert.assertTrue(l >= min && l <= max);
		}
	}

	@Test
	public void testSequenceCheckpointRestore() throws Exception {
		final int initElement = 0;
		final int maxElement = 100;
		final int maxParallelsim = 2;

		final Set<Long> expectedOutput = new HashSet<>();
		for (long i = initElement; i <= maxElement; i++) {
			expectedOutput.add(i);
		}

		final ConcurrentHashMap<String, List<Long>> outputCollector = new ConcurrentHashMap<>();
		final OneShotLatch latchToTrigger1 = new OneShotLatch();
		final OneShotLatch latchToWait1 = new OneShotLatch();
		final OneShotLatch latchToTrigger2 = new OneShotLatch();
		final OneShotLatch latchToWait2 = new OneShotLatch();

		final DataGeneratorSource<Long> source1 = new DataGeneratorSource<>(
				SequenceGenerator.longGenerator(initElement, maxElement));
		StreamSource<Long, DataGeneratorSource<Long>> src1 = new StreamSource<>(source1);

		final AbstractStreamOperatorTestHarness<Long> testHarness1 =
			new AbstractStreamOperatorTestHarness<>(src1, maxParallelsim, 2, 0);
		testHarness1.open();

		final DataGeneratorSource<Long> source2 = new DataGeneratorSource<>(
				SequenceGenerator.longGenerator(initElement, maxElement));
		StreamSource<Long, DataGeneratorSource<Long>> src2 = new StreamSource<>(source2);

		final AbstractStreamOperatorTestHarness<Long> testHarness2 =
			new AbstractStreamOperatorTestHarness<>(src2, maxParallelsim, 2, 1);
		testHarness2.open();

		// run the source asynchronously
		Thread runner1 = new Thread(() -> {
			try {
				source1.run(new BlockingSourceContext(
						"1", latchToTrigger1, latchToWait1, outputCollector, 21));
			} catch (Throwable t) {
				t.printStackTrace();
			}
		});

		// run the source asynchronously
		Thread runner2 = new Thread(() -> {
			try {
				source2.run(new BlockingSourceContext(
						"2", latchToTrigger2, latchToWait2, outputCollector, 32));
			}
			catch (Throwable t) {
				t.printStackTrace();
			}
		});

		runner1.start();
		runner2.start();

		if (!latchToTrigger1.isTriggered()) {
			latchToTrigger1.await();
		}

		if (!latchToTrigger2.isTriggered()) {
			latchToTrigger2.await();
		}

		OperatorSubtaskState snapshot = AbstractStreamOperatorTestHarness.repackageState(
			testHarness1.snapshot(0L, 0L),
			testHarness2.snapshot(0L, 0L)
		);

		final DataGeneratorSource<Long> source3 = new DataGeneratorSource<>(
				SequenceGenerator.longGenerator(initElement, maxElement));
		StreamSource<Long, DataGeneratorSource<Long>> src3 = new StreamSource<>(source3);

		final OperatorSubtaskState initState =
				AbstractStreamOperatorTestHarness.repartitionOperatorState(
						snapshot, maxParallelsim, 2, 1, 0);

		final AbstractStreamOperatorTestHarness<Long> testHarness3 =
			new AbstractStreamOperatorTestHarness<>(src3, maxParallelsim, 1, 0);
		testHarness3.setup();
		testHarness3.initializeState(initState);
		testHarness3.open();

		final OneShotLatch latchToTrigger3 = new OneShotLatch();
		final OneShotLatch latchToWait3 = new OneShotLatch();
		latchToWait3.trigger();

		// run the source asynchronously
		Thread runner3 = new Thread(() -> {
			try {
				source3.run(new BlockingSourceContext(
						"3", latchToTrigger3, latchToWait3, outputCollector, 3));
			}
			catch (Throwable t) {
				t.printStackTrace();
			}
		});
		runner3.start();
		runner3.join();

		Assert.assertEquals(3, outputCollector.size()); // we have 3 tasks.

		// test for at-most-once
		Set<Long> dedupRes = new HashSet<>(Math.abs(maxElement - initElement) + 1);
		for (Map.Entry<String, List<Long>> elementsPerTask: outputCollector.entrySet()) {
			String key = elementsPerTask.getKey();
			List<Long> elements = outputCollector.get(key);

			// this tests the correctness of the latches in the test
			Assert.assertTrue(elements.size() > 0);

			for (Long elem : elements) {
				if (!dedupRes.add(elem)) {
					Assert.fail("Duplicate entry: " + elem);
				}

				if (!expectedOutput.contains(elem)) {
					Assert.fail("Unexpected element: " + elem);
				}
			}
		}

		// test for exactly-once
		Assert.assertEquals(Math.abs(initElement - maxElement) + 1, dedupRes.size());

		latchToWait1.trigger();
		latchToWait2.trigger();

		// wait for everybody ot finish.
		runner1.join();
		runner2.join();
	}
}
