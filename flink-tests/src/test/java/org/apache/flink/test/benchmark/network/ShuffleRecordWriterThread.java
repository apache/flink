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

import java.io.IOException;
import java.util.concurrent.BlockingQueue;

/**
 * Wrapping thread around {@link RecordWriter} that sends a fixed number of <tt>LongValue(0)</tt>
 * records.
 */
public class ShuffleRecordWriterThread<T extends IOReadableWritable> extends RecordWriterThread<T> {
	private final BlockingQueue<CheckedThread> finishedWriters;

	public ShuffleRecordWriterThread(boolean broadcastMode, T[] recordsSet, BlockingQueue<CheckedThread> finishedWriters) {
		super(broadcastMode, recordsSet);
		this.finishedWriters = finishedWriters;
	}

	@Override
	protected synchronized void finishSendingRecords() throws IOException {
		super.finishSendingRecords();
		recordWriter.close();
		resultPartition.finish();
		resultPartition.release();
		finishedWriters.add(this);
	}
}
