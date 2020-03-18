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

import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.event.RuntimeEvent;
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.util.function.ThrowingRunnable;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * {@link CheckpointBarrierUnaligner} cancellation test.
 */
@RunWith(Parameterized.class)
public class CheckpointBarrierUnalignerCancellationTest {
	private final List<RuntimeEvent> events;
	private final boolean expectTriggerCheckpoint;
	private final boolean expectAbortCheckpoint;
	private final int numChannels;
	private final int channel;

	public CheckpointBarrierUnalignerCancellationTest(boolean expectTriggerCheckpoint, boolean expectAbortCheckpoint, List<RuntimeEvent> events, int numChannels, int channel) {
		this.events = events;
		this.expectTriggerCheckpoint = expectTriggerCheckpoint;
		this.expectAbortCheckpoint = expectAbortCheckpoint;
		this.numChannels = numChannels;
		this.channel = channel;
	}

	@Parameterized.Parameters(name = "expect trigger: {0}, expect abort {1}, numChannels: {3}, chan: {4}, events: {2}")
	public static Object[][] parameters() {
		return new Object[][]{
				new Object[]{false, true, Arrays.asList(cancel(10), cancel(20)), 1, 0},
				new Object[]{false, true, Arrays.asList(cancel(20), cancel(10)), 1, 0},
				new Object[]{false, true, Arrays.asList(cancel(10), checkpoint(10)), 1, 0},
				new Object[]{true, true, Arrays.asList(cancel(10), checkpoint(20)), 1, 0},
				new Object[]{false, true, Arrays.asList(cancel(20), checkpoint(10)), 1, 0},
				new Object[]{true, false, Arrays.asList(checkpoint(10), checkpoint(10)), 1, 0},
				new Object[]{true, false, Arrays.asList(checkpoint(10), checkpoint(20)), 1, 0},
				new Object[]{true, true, Arrays.asList(checkpoint(10), checkpoint(20)), 2, 0},
				new Object[]{true, false, Arrays.asList(checkpoint(20), checkpoint(10)), 1, 0},
				new Object[]{true, true, Arrays.asList(checkpoint(10), cancel(10)), 1, 0},
				new Object[]{true, true, Arrays.asList(checkpoint(10), cancel(20)), 1, 0},
				new Object[]{true, true, Arrays.asList(checkpoint(20), cancel(10)), 1, 0},
		};
	}

	@Test
	public void test() throws Exception {
		TestInvokable invokable = new TestInvokable();
		CheckpointBarrierUnaligner unaligner = new CheckpointBarrierUnaligner(new int[]{numChannels}, ChannelStateWriter.NO_OP, "test", invokable);

		for (RuntimeEvent e : events) {
			if (e instanceof CancelCheckpointMarker) {
				unaligner.processCancellationBarrier((CancelCheckpointMarker) e);
			} else if (e instanceof CheckpointBarrier) {
				unaligner.processBarrier((CheckpointBarrier) e, channel);
			} else {
				throw new IllegalArgumentException("unexpected event type: " + e);
			}
		}

		assertEquals("expectAbortCheckpoint", expectAbortCheckpoint, invokable.checkpointAborted);
		assertEquals("expectTriggerCheckpoint", expectTriggerCheckpoint, invokable.checkpointTriggered);
	}

	private static CheckpointBarrier checkpoint(int checkpointId) {
		return new CheckpointBarrier(checkpointId, 1, CheckpointOptions.forCheckpointWithDefaultLocation());
	}

	private static CancelCheckpointMarker cancel(int checkpointId) {
		return new CancelCheckpointMarker(checkpointId);
	}

	private static class TestInvokable extends AbstractInvokable {
		TestInvokable() {
			super(new DummyEnvironment());
		}

		private boolean checkpointAborted;
		private boolean checkpointTriggered;

		@Override
		public void invoke() {
		}

		@Override
		public void triggerCheckpointOnBarrier(CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions, CheckpointMetrics checkpointMetrics) {
			checkpointTriggered = true;
		}

		@Override
		public void abortCheckpointOnBarrier(long checkpointId, Throwable cause) {
			checkpointAborted = true;
		}

		@Override
		public <E extends Exception> void executeInTaskThread(
				ThrowingRunnable<E> runnable,
				String descriptionFormat,
				Object... descriptionArgs) {
			try {
				runnable.run();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}
}
