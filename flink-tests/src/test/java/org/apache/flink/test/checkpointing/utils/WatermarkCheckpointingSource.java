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

package org.apache.flink.test.checkpointing.utils;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;

/**
 * Source used for watermark checkpointing tests.
 */
public class WatermarkCheckpointingSource extends RichSourceFunction<Tuple2<Integer, Integer>>
	implements ListCheckpointed<Integer>, CheckpointListener {

	public static final int PERIOD_MODE = 1;
	public static final int PUNCTUATE_MODE = 2;

	private static final long INITIAL = Long.MIN_VALUE;
	private static final long STATEFUL_CHECKPOINT_COMPLETED = Long.MAX_VALUE;

	private final AtomicLong checkpointStatus;

	private volatile boolean running;

	private int emitCallCount;
	private final int upperWatermark;
	private final int lowerWatermark;
	private final int sendCount;
	private final int failAfterNumElements;
	private final int mode;

	public WatermarkCheckpointingSource(int upperWatermark, int lowerWatermark, int sendCount, int mode) {
		this.running = true;
		this.emitCallCount = 0;
		this.upperWatermark = upperWatermark;
		this.lowerWatermark = lowerWatermark;
		this.sendCount = sendCount;
		this.failAfterNumElements = sendCount / 2;
		this.checkpointStatus = new AtomicLong(INITIAL);
		this.mode = mode;
	}

	@Override
	public void open(Configuration parameters) {
		// non-parallel source
		assertEquals(1, getRuntimeContext().getNumberOfParallelSubtasks());
	}

	@Override
	public void run(SourceContext<Tuple2<Integer, Integer>> ctx) throws Exception {
		final RuntimeContext runtimeContext = getRuntimeContext();

		final boolean failThisTask =
			runtimeContext.getAttemptNumber() == 0 && runtimeContext.getIndexOfThisSubtask() == 0;

		while (running && emitCallCount < sendCount) {

			emitCallCount++;
			// the function failed before, or we are in the elements before the failure
			synchronized (ctx.getCheckpointLock()) {
				collectRecord(ctx, failThisTask);
			}

			if (emitCallCount <= sendCount) {
				Thread.sleep(1);
			}
			if (failThisTask && emitCallCount == failAfterNumElements) {
				// wait for a pending checkpoint that fulfills our requirements if needed
				while (checkpointStatus.get() != STATEFUL_CHECKPOINT_COMPLETED) {
					Thread.sleep(1);
				}
				// wait for watermark emit
				Thread.sleep(200);
				throw new Exception("Artificial Failure");
			}
		}
	}

	private void collectRecord(SourceContext<Tuple2<Integer, Integer>> ctx, boolean failThisTask) {
		if (mode == PERIOD_MODE) {
			if (failThisTask) {
				ctx.collect(Tuple2.of(upperWatermark, upperWatermark));
			} else {
				ctx.collect(Tuple2.of(lowerWatermark, lowerWatermark));
			}
		} else if (mode == PUNCTUATE_MODE) {
			if (failThisTask) {
				ctx.collect(Tuple2.of(upperWatermark + emitCallCount, upperWatermark + emitCallCount));
			} else {
				ctx.collect(Tuple2.of(lowerWatermark + emitCallCount, lowerWatermark + emitCallCount));
			}
		} else {
			throw new RuntimeException("Fail to identify mode " + String.valueOf(mode) + ".");
		}
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) {
		// This will unblock the task for failing, if this is the checkpoint we are waiting for
		checkpointStatus.compareAndSet(checkpointId, STATEFUL_CHECKPOINT_COMPLETED);
	}

	@Override
	public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {
		// We accept a checkpoint as basis if it should have a "decent amount" of state
		if (emitCallCount == failAfterNumElements) {
			// This means we are waiting for notification of this checkpoint to completed now.
			checkpointStatus.compareAndSet(INITIAL, checkpointId);
		}
		return Collections.singletonList(this.emitCallCount);
	}

	@Override
	public void restoreState(List<Integer> state) throws Exception {
		if (state.isEmpty() || state.size() > 1) {
			throw new RuntimeException("Test failed due to unexpected recovered state size " + state.size());
		}
		this.emitCallCount = state.get(0);
	}

	@Override
	public void cancel() {
		running = false;
	}
}
