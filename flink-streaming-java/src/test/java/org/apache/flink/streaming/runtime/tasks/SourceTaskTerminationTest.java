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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.core.testutils.MultiShotLatch;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.concurrent.BlockingQueue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * A test verifying the termination process
 * (synchronous checkpoint and task termination) at the {@link SourceStreamTask}.
 */
public class SourceTaskTerminationTest extends TestLogger {

	private static OneShotLatch ready;
	private static MultiShotLatch runLoopStart;
	private static MultiShotLatch runLoopEnd;

	@Rule
	public final Timeout timeoutPerTest = Timeout.seconds(20);

	@Before
	public void initialize() {
		ready = new OneShotLatch();
		runLoopStart = new MultiShotLatch();
		runLoopEnd = new MultiShotLatch();
	}

	@Test
	public void testStopWithSavepointWithMaxWatermark() throws Exception {
		stopWithSavepointStreamTaskTestHelper(true);
	}

	@Test
	public void testStopWithSavepointWithoutMaxWatermark() throws Exception {
		stopWithSavepointStreamTaskTestHelper(false);
	}

	private void stopWithSavepointStreamTaskTestHelper(final boolean withMaxWatermark) throws Exception {
		final long syncSavepointId = 34L;

		final StreamTaskTestHarness<Long> srcTaskTestHarness = getSourceStreamTaskTestHarness();
		final Thread executionThread = srcTaskTestHarness.invoke();
		final StreamTask<Long, ?> srcTask = srcTaskTestHarness.getTask();
		final SynchronousSavepointLatch syncSavepointLatch = srcTask.getSynchronousSavepointLatch();

		ready.await();

		// step by step let the source thread emit elements
		emitAndVerifyWatermarkAndElement(srcTaskTestHarness, 1L);
		emitAndVerifyWatermarkAndElement(srcTaskTestHarness, 2L);

		srcTask.triggerCheckpoint(
				new CheckpointMetaData(31L, 900),
				CheckpointOptions.forCheckpointWithDefaultLocation(),
				false);

		assertFalse(syncSavepointLatch.isSet());
		assertFalse(syncSavepointLatch.isCompleted());
		assertFalse(syncSavepointLatch.isWaiting());

		verifyCheckpointBarrier(srcTaskTestHarness.getOutput(), 31L);

		emitAndVerifyWatermarkAndElement(srcTaskTestHarness, 3L);

		srcTask.triggerCheckpoint(
				new CheckpointMetaData(syncSavepointId, 900),
				new CheckpointOptions(CheckpointType.SYNC_SAVEPOINT, CheckpointStorageLocationReference.getDefault()),
				withMaxWatermark);

		assertTrue(syncSavepointLatch.isSet());
		assertFalse(syncSavepointLatch.isCompleted());
		assertFalse(syncSavepointLatch.isWaiting());

		if (withMaxWatermark) {
			// if we are in TERMINATE mode, we expect the source task
			// to emit MAX_WM before the SYNC_SAVEPOINT barrier.
			verifyWatermark(srcTaskTestHarness.getOutput(), Watermark.MAX_WATERMARK);
		}

		verifyCheckpointBarrier(srcTaskTestHarness.getOutput(), syncSavepointId);

		srcTask.notifyCheckpointComplete(syncSavepointId);
		assertTrue(syncSavepointLatch.isCompleted());

		executionThread.join();
	}

	private StreamTaskTestHarness<Long> getSourceStreamTaskTestHarness() {
		final StreamTaskTestHarness<Long> testHarness = new StreamTaskTestHarness<>(
				SourceStreamTask::new,
				BasicTypeInfo.LONG_TYPE_INFO);

		final LockStepSourceWithOneWmPerElement source = new LockStepSourceWithOneWmPerElement();

		testHarness.setupOutputForSingletonOperatorChain();
		testHarness.getExecutionConfig().setLatencyTrackingInterval(-1);

		StreamConfig streamConfig = testHarness.getStreamConfig();
		StreamSource<Long, ?> sourceOperator = new StreamSource<>(source);
		streamConfig.setStreamOperator(sourceOperator);
		streamConfig.setOperatorID(new OperatorID());
		return testHarness;
	}

	private void emitAndVerifyWatermarkAndElement(
			final StreamTaskTestHarness<Long> srcTaskTestHarness,
			final long expectedElement) throws InterruptedException {

		runLoopStart.trigger();
		verifyWatermark(srcTaskTestHarness.getOutput(), new Watermark(expectedElement));
		verifyNextElement(srcTaskTestHarness.getOutput(), expectedElement);
		runLoopEnd.await();
	}

	private void verifyNextElement(BlockingQueue<Object> output, long expectedElement) throws InterruptedException {
		Object next = output.take();
		assertTrue("next element is not an event", next instanceof StreamRecord);
		assertEquals("wrong event", expectedElement, ((StreamRecord<Long>) next).getValue().longValue());
	}

	private void verifyWatermark(BlockingQueue<Object> output, Watermark expectedWatermark) throws InterruptedException {
		Object next = output.take();
		assertTrue("next element is not a watermark", next instanceof Watermark);
		assertEquals("wrong watermark", expectedWatermark, next);
	}

	private void verifyCheckpointBarrier(BlockingQueue<Object> output, long checkpointId) throws InterruptedException {
		Object next = output.take();
		assertTrue("next element is not a checkpoint barrier", next instanceof CheckpointBarrier);
		assertEquals("wrong checkpoint id", checkpointId, ((CheckpointBarrier) next).getId());
	}

	private static class LockStepSourceWithOneWmPerElement implements SourceFunction<Long> {

		private volatile boolean isRunning;

		@Override
		public void run(SourceContext<Long> ctx) throws Exception {
			long element = 1L;
			isRunning = true;

			ready.trigger();

			while (isRunning) {
				runLoopStart.await();
				ctx.emitWatermark(new Watermark(element));
				ctx.collect(element++);
				runLoopEnd.trigger();
			}
		}

		@Override
		public void cancel() {
			isRunning = false;
			runLoopStart.trigger();
		}
	}
}
