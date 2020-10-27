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

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import javax.annotation.Nullable;

import static java.util.Collections.singletonList;
import static org.apache.flink.shaded.guava18.com.google.common.collect.Iterables.getOnlyElement;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Integer source that cancels itself after a specified number emitted and subsequent checkpoint is fully completed.
 */
public class CancellingIntegerSource extends RichSourceFunction<Integer> implements CheckpointedFunction, CheckpointListener {

	private final int count;
	private final Integer cancelAfter;

	@Nullable private transient Long cancelAfterCheckpointId;
	private transient volatile boolean isCanceled;
	private transient volatile int sentCount;
	private transient ListState<Integer> lastSentStored;

	private CancellingIntegerSource(int count, @Nullable Integer cancelAfter) {
		checkArgument(count > 0);
		checkArgument(cancelAfter == null || cancelAfter > 0);
		this.cancelAfter = cancelAfter;
		this.count = count;
	}

	@Override
	public void run(SourceContext<Integer> ctx) throws InterruptedException {
		emitInLoop(ctx);
		awaitCancellation();
	}

	private void emitInLoop(SourceContext<Integer> ctx) throws InterruptedException {
		while (sentCount < count && !isCanceled) {
			synchronized (ctx.getCheckpointLock()) {
				if (sentCount < count && !isCanceled) {
					ctx.collect(sentCount++);
				}
			}
			Thread.sleep(10); // allow to snapshot state (Thread.yield() doesn't always work)
		}
	}

	private void awaitCancellation() {
		while (!isCanceled) {
			try {
				Thread.sleep(50);
			} catch (InterruptedException e) {
				if (isCanceled) {
					Thread.currentThread().interrupt();
				}
			}
		}
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
		lastSentStored = context.getOperatorStateStore().getListState(new ListStateDescriptor<>("counter", Integer.class));
		if (context.isRestored()) {
			sentCount = getOnlyElement(lastSentStored.get());
		}
		checkState(cancelAfter == null || sentCount < cancelAfter);
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		lastSentStored.update(singletonList(sentCount));
		if (cancelAfter != null && cancelAfter <= sentCount && cancelAfterCheckpointId == null) {
			cancelAfterCheckpointId = context.getCheckpointId();
		}
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) {
		if (cancelAfterCheckpointId != null && cancelAfterCheckpointId <= checkpointId) {
			cancel();
		}
	}

	@Override
	public void notifyCheckpointAborted(long checkpointId) {
	}

	@Override
	public void cancel() {
		isCanceled = true;
	}

	public static CancellingIntegerSource upTo(int max, boolean continueAfterCount) {
		return new CancellingIntegerSource(max, continueAfterCount ? null : max);
	}

}
