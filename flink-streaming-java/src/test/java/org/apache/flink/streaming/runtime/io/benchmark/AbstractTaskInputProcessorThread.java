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

package org.apache.flink.streaming.runtime.io.benchmark;

import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.runtime.io.network.partition.consumer.IterableInputChannel;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.ExceptionUtils.firstOrSuppressed;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Base class for all task-input processor threads.
 */
public abstract class AbstractTaskInputProcessorThread extends CheckedThread {

	private final List<IterableInputChannel> inputChannels;

	private volatile boolean taskRunning = true;

	/**
	 * Future to wait on a definition of the number of records to process.
	 */
	private CompletableFuture<Long> recordsToProcess = new CompletableFuture<>();

	private CompletableFuture<Long> processedRecords = new CompletableFuture<>();

	public AbstractTaskInputProcessorThread(List<IterableInputChannel> inputChannels) {
		this.inputChannels = checkNotNull(inputChannels);
	}

	@Override
	public void go() throws Exception {
		try {
			while (taskRunning) {
				long records = getRecordsToProcess().get();

				startIteration();
				long processedRecords = processTaskInput(records);

				finishProcessingRecords(processedRecords, null);
			}
		} catch (Throwable t) {
			finishProcessingRecords(-1, t);
		} finally {
			cleanup();
		}
	}

	/**
	 * Initializes the record writer thread with this many numbers to send.
	 *
	 * <p>If the thread was already started, it may now continue.
	 *
	 * @param records
	 * 		number of records to send
	 */
	public synchronized void setRecordsToProcess(long records) {
		checkState(!recordsToProcess.isDone());
		recordsToProcess.complete(records);
	}

	public synchronized CompletableFuture<Long> getProcessedRecords() {
		return processedRecords;
	}

	public synchronized void shutdown() {
		taskRunning = false;
		recordsToProcess.complete(0L);
	}

	private synchronized CompletableFuture<Long> getRecordsToProcess() {
		return recordsToProcess;
	}

	private synchronized void finishProcessingRecords(long numRecords, @Nullable Throwable throwable) {
		// reset the input channels for the next iteration
		try {
			for (int i = 0; i < inputChannels.size(); i++) {
				inputChannels.get(i).reset();
			}
		} catch (Throwable t){
			throwable = firstOrSuppressed(t, throwable);
		}

		// reset all futures
		recordsToProcess = new CompletableFuture<>();

		CompletableFuture<Long> lastProcessedRecords = processedRecords;
		processedRecords = new CompletableFuture<>();

		// finally, set the finished state
		checkState(!lastProcessedRecords.isDone());
		if (throwable == null) {
			lastProcessedRecords.complete(numRecords);
		} else {
			lastProcessedRecords.completeExceptionally(throwable);
		}
	}

	protected abstract void startIteration();

	protected abstract void endIteration();

	protected abstract long processTaskInput(long records) throws Exception;

	protected abstract void cleanup() throws IOException;
}
