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

import org.apache.flink.api.common.typeutils.base.LongValueSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.streaming.runtime.io.StreamRecordWriter;
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
	private ReceiverThread<LongValue> receiver;
	private StreamRecordWriter<LongValue> recordWriter;

	/**
	 * Executes the latency benchmark with the given number of records.
	 *
	 * @param records
	 * 		records to pass through the network stack
	 * @param flushAfterLastEmit
	 * 		whether to flush the {@link RecordWriter} after the last record
	 */
	public void executeBenchmark(long records, boolean flushAfterLastEmit) throws Exception {
		final LongValue value = new LongValue(0);

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

	/**
	 * Initializes the throughput benchmark with the given parameters.
	 *
	 * @param flushTimeout
	 * 		output flushing interval of the
	 * 		{@link org.apache.flink.streaming.runtime.io.StreamRecordWriter}'s output flusher thread
	 */
	public void setUp(long flushTimeout) throws Exception {
		environment = new StreamNetworkBenchmarkEnvironment<>();
		environment.setUp(
			1,
			1,
			false,
			false,
			-1,
			-1,
			new Configuration());

		ResultPartition resultPartition = environment.createInternalResultPartition(0, new LongValueSerializer());
		recordWriter = environment.createRecordWriter(resultPartition, flushTimeout);

		SingleInputGate[] inputGates = environment.createInputGate(ResultPartitionType.PIPELINED, 0, 1);
		receiver = new ReceiverThread<>(new LongValue());
		receiver.setupReader(inputGates);
		receiver.start();
	}

	/**
	 * Shuts down a benchmark previously set up via {@link #setUp}.
	 */
	public void tearDown() throws Exception {
		environment.tearDown();
		receiver.shutdown();
	}
}
