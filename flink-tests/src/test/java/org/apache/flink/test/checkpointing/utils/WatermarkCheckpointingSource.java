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
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;

/**
 * Source used for watermark checkpointing tests.
 * Here is the source's logic:
 *   1. Let every subtask regard the index of task as the sending data.
 *   2. Every subtask creates its own temporary file after they finish the first half sending.
 *   3. The fail subtask needs to make sure that all the subtasks finish the checkpointing by counting the temporary files.
 *   4. The fail subtask throws an exception and fails.
 */
public class WatermarkCheckpointingSource extends RichParallelSourceFunction<Tuple2<Integer, Integer>>
	implements ListCheckpointed<Integer>, CheckpointListener {

	private static final long INITIAL = Long.MIN_VALUE;
	private static final long STATEFUL_CHECKPOINT_COMPLETED = Long.MAX_VALUE;

	private final AtomicLong checkpointStatus;

	private volatile boolean running;

	private int emitCallCount;
	private final int sendCount;
	private final int failAfterNumElements;
	private final int failTaskIndex;

	private File tempFolder;

	private final AtomicBoolean snapshotWriteFlag = new AtomicBoolean(false);

	public WatermarkCheckpointingSource(int failTaskIndex, File tempFolder) {
		this.running = true;
		this.emitCallCount = 0;
		this.sendCount = 2;
		this.failAfterNumElements = 1;
		this.checkpointStatus = new AtomicLong(INITIAL);
		this.failTaskIndex = failTaskIndex;
		this.tempFolder = tempFolder;
	}

	@Override
	public void open(Configuration parameters) {
		// non-parallel source
		assertEquals(4, getRuntimeContext().getNumberOfParallelSubtasks());
	}

	@Override
	public void run(SourceContext<Tuple2<Integer, Integer>> ctx) throws Exception {
		final RuntimeContext runtimeContext = getRuntimeContext();

		final boolean failThisTask = runtimeContext.getAttemptNumber() == 0 && runtimeContext.getIndexOfThisSubtask() == failTaskIndex;

		while (running && emitCallCount < sendCount) {

			// the function failed before, or we are in the elements before the failure
			synchronized (ctx.getCheckpointLock()) {
				collectRecord(ctx);
			}
			emitCallCount++;

			if (emitCallCount <= sendCount) {
				Thread.sleep(1);
			}
			if (emitCallCount == failAfterNumElements) {
				while (checkpointStatus.get() != STATEFUL_CHECKPOINT_COMPLETED) {
					Thread.sleep(1);
				}
				if (failThisTask) {
					while (!snapshotWriteFlag.get() || readSnapshotTempFiles() != getRuntimeContext().getNumberOfParallelSubtasks()) {
						Thread.sleep(1);
					}
					Thread.sleep(200);
					throw new Exception("Artificial Failure");
				}
			}
		}
	}

	private void collectRecord(SourceContext<Tuple2<Integer, Integer>> ctx) {
		int output = getRuntimeContext().getIndexOfThisSubtask();
		ctx.collect(Tuple2.of(output, output));
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws IOException {
		// This will unblock the task for failing, if this is the checkpoint we are waiting for
		checkpointStatus.compareAndSet(checkpointId, STATEFUL_CHECKPOINT_COMPLETED);
	}

	@Override
	public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {
		// We accept a checkpoint as basis if it should have a "decent amount" of state
		if (emitCallCount == failAfterNumElements) {
			writeSnapshotToTempFile();
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

	private void writeSnapshotToTempFile() throws IOException {
		File tempFile = new File(tempFolder + "/.watermark-"
			+ String.valueOf(getRuntimeContext().getIndexOfThisSubtask()));
		tempFile.createNewFile();
		try (FileOutputStream fos = new FileOutputStream(tempFile)) {
			fos.write(1);
		}
		snapshotWriteFlag.compareAndSet(false, true);
	}

	private int readSnapshotTempFiles() {
		List<File> files = Arrays.asList(Objects.requireNonNull(tempFolder.listFiles()));

		List<File> filterFiles = new ArrayList<>();
		files.forEach(file -> {
			if (file.getName().contains(".watermark-")) {
				filterFiles.add(file);
			}
		});

		return filterFiles.size();
	}
}
