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
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.streaming.runtime.io.StreamRecordWriter;
import org.apache.flink.types.LongValue;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Wrapping thread around {@link RecordWriter} that sends a fixed number of <tt>LongValue(0)</tt>
 * records.
 */
public class LongRecordWriterThread extends CheckedThread {
	private final StreamRecordWriter<LongValue> recordWriter;
	private final boolean broadcastMode;

	/**
	 * Future to wait on a definition of the number of records to send.
	 */
	private CompletableFuture<Long> recordsToSend = new CompletableFuture<>();

	private volatile boolean running = true;

	public LongRecordWriterThread(
			StreamRecordWriter<LongValue> recordWriter,
			boolean broadcastMode) {
		this.recordWriter = checkNotNull(recordWriter);
		this.broadcastMode = broadcastMode;
	}

	public synchronized void shutdown() {
		running = false;
		recordsToSend.complete(0L);
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

	private synchronized void finishSendingRecords() {
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
		LongValue value = new LongValue(0);

		for (int i = 1; i < records; i++) {
			if (broadcastMode) {
				recordWriter.broadcastEmit(value);
			}
			else {
				recordWriter.emit(value);
			}
		}
		value.setValue(records);
		recordWriter.broadcastEmit(value);
		recordWriter.flushAll();

		finishSendingRecords();
	}
}
