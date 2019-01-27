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

package org.apache.flink.test.benchmark.network;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.streaming.runtime.io.StreamRecordWriter;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Wrapping thread around {@link RecordWriter} that sends a fixed number of <tt>LongValue(0)</tt>
 * records.
 */
public class RecordWriterThread<T extends IOReadableWritable> extends CheckedThread {
	protected StreamRecordWriter<T> recordWriter;
	protected ResultPartition resultPartition;
	private final boolean broadcastMode;
	private final T[] recordsSet;

	/**
	 * Future to wait on a definition of the number of records to send.
	 */
	private CompletableFuture<Long> recordsToSend = new CompletableFuture<>();

	private volatile boolean running = true;

	public RecordWriterThread(boolean broadcastMode, T[] recordsSet) {
		this.broadcastMode = broadcastMode;
		this.recordsSet = checkNotNull(recordsSet);
	}

	public void setupWriter(StreamRecordWriter<T> recordWriter, ResultPartition resultPartition) {
		this.recordWriter = checkNotNull(recordWriter);
		this.resultPartition = checkNotNull(resultPartition);
	}

	public synchronized void shutdown() {
		running = false;
		recordsToSend.complete(0L);
		resultPartition.release();
	}

	/**
	 * Initializes the record writer thread with this many numbers to send.
	 *
	 * <p>If the thread was already started, if may now continue.
	 *
	 * @param records
	 * 		number of records to send
	 */
	public synchronized void setRecordsToSend(long records) {
		checkState(!recordsToSend.isDone());
		recordsToSend.complete(records);
	}

	private synchronized CompletableFuture<Long> getRecordsToSend() {
		return recordsToSend;
	}

	protected synchronized void finishSendingRecords() throws IOException {
		recordsToSend = new CompletableFuture<>();
	}

	@Override
	public void go() throws Exception {
		try {
			while (running) {
				sendRecords(getRecordsToSend().get());
			}
		}
		finally {
			recordWriter.close();
		}
	}

	private void sendRecords(long records) throws IOException, InterruptedException {
		int currentIndex = 0;
		for (long i = 0; i < records; i++) {
			currentIndex = currentIndex % recordsSet.length;
			if (broadcastMode) {
				recordWriter.broadcastEmit(recordsSet[currentIndex++]);
			} else {
				recordWriter.emit(recordsSet[currentIndex++]);
			}
		}
		recordWriter.flushAll();
		finishSendingRecords();
	}
}
