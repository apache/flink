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

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.api.writer.RoundRobinChannelSelector;
import org.apache.flink.runtime.io.network.benchmark.NetworkBenchmarkEnvironment;
import org.apache.flink.streaming.runtime.io.StreamRecordWriter;

/**
 * Context for stream network benchmarks executed by the external
 * <a href="https://github.com/dataArtisans/flink-benchmarks">flink-benchmarks</a> project.
 */
public class StreamNetworkBenchmarkEnvironment<T extends IOReadableWritable> extends NetworkBenchmarkEnvironment<T> {

	public RecordWriter<T> createStreamRecordWriter(int partitionIndex, long flushTimeout)
			throws Exception {
		ResultPartitionWriter sender =
			createResultPartition(jobId, partitionIds[partitionIndex], senderEnv, channels);
		return new StreamRecordWriter<>(sender, new RoundRobinChannelSelector<>(), flushTimeout);
	}
}
