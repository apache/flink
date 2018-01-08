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

package org.apache.flink.runtime.io.network.benchmark;

import org.apache.flink.types.LongValue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Context for network benchmarks executed in flink-benchmark.
 */
public class NetworkBenchmark {
	private static final long RECEIVER_TIMEOUT = 30_000;

	protected NetworkBenchmarkEnvironment<LongValue> environment;
	protected ReceiverThread receiver;
	protected RecordWriterThread[] writerThreads;

	public void executeThroughputBenchmark(long records) throws Exception {
		final LongValue value = new LongValue();
		value.setValue(0);

		long lastRecord = records / writerThreads.length;
		CompletableFuture<?> recordsReceived = receiver.setExpectedRecord(lastRecord);

		for (RecordWriterThread writerThread : writerThreads) {
			writerThread.setRecordsToSend(lastRecord);
		}

		recordsReceived.get(RECEIVER_TIMEOUT, TimeUnit.MILLISECONDS);
	}

	public void setUp(int recordWriters, int channels) throws Exception {
		environment = new NetworkBenchmarkEnvironment<>();
		environment.setUp(recordWriters, channels);
		receiver = environment.createReceiver(SerializingLongReceiver.class);
		writerThreads = new RecordWriterThread[recordWriters];
		for (int writer = 0; writer < recordWriters; writer++) {
			writerThreads[writer] = new RecordWriterThread(environment.createRecordWriter(writer));
			writerThreads[writer].start();
		}
	}

	public void tearDown() throws Exception {
		for (RecordWriterThread writerThread : writerThreads) {
			writerThread.shutdown();
			writerThread.sync(5000);
		}
		environment.tearDown();
		receiver.shutdown();
	}
}
