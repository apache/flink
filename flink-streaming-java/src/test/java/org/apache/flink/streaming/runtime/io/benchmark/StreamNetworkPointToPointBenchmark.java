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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.io.network.api.writer.RecordWriterBuilder;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.types.LongValue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Streaming point-to-point latency network benchmarks executed by the external
 * <a href="https://github.com/dataArtisans/flink-benchmarks">flink-benchmarks</a> project.
 */
public class StreamNetworkPointToPointBenchmark {
	private static final long RECEIVER_TIMEOUT = 2000;

	private StreamNetworkBenchmarkEnvironment<LongValue> environment;
	private ReceiverThread receiver;
	private RecordWriter<LongValue> recordWriter;

	/**
	 * Executes the latency benchmark with the given number of records.
	 *
	 * @param records
	 * 		records to pass through the network stack
	 * @param flushAfterLastEmit
	 * 		whether to flush the {@link RecordWriter} after the last record
	 */
	public void executeBenchmark(long records, boolean flushAfterLastEmit) throws Exception {
		final LongValue value = new LongValue();
		value.setValue(0);

		CompletableFuture<?> recordsReceived = receiver.setExpectedRecord(records);

		for (int i = 1; i < records; i++) {
			recordWriter.emit(value);
		}
		value.setValue(records);
		recordWriter.broadcastEmit(value);
		if (flushAfterLastEmit) {
			recordWriter.flushAll();
		}

		recordsReceived.get(RECEIVER_TIMEOUT, TimeUnit.MILLISECONDS);
	}

	public void setUp(long flushTimeout) throws Exception {
		setUp(flushTimeout, new Configuration());
	}

	/**
	 * Initializes the throughput benchmark with the given parameters.
	 *
	 * @param flushTimeout
	 * 		output flushing interval of the
	 * 		{@link org.apache.flink.runtime.io.network.api.writer.RecordWriter}'s output flusher thread
	 */
	public void setUp(long flushTimeout, Configuration config) throws Exception {
		environment = new StreamNetworkBenchmarkEnvironment<>();
		environment.setUp(1, 1, false, -1, -1, config);

		ResultPartitionWriter resultPartitionWriter = environment.createResultPartitionWriter(0);

		recordWriter = new RecordWriterBuilder().setTimeout(flushTimeout).build(resultPartitionWriter);
		receiver = environment.createReceiver();
	}

	/**
	 * Shuts down a benchmark previously set up via {@link #setUp}.
	 */
	public void tearDown() {
		environment.tearDown();
		receiver.shutdown();
	}
}
