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

package org.apache.flink.streaming.connectors.kafka.internals;


import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * The offset handler is responsible for locating the initial partition offsets 
 * where the source should start reading, as well as committing offsets from completed
 * checkpoints.
 */
public interface OffsetHandler {

	/**
	 * Commits the given offset for the partitions. May commit the offsets to the Kafka broker,
	 * or to ZooKeeper, based on its configured behavior.
	 *
	 * @param offsetsToCommit The offset to commit, per partition.
	 */
	void commit(Map<KafkaTopicPartition, Long> offsetsToCommit) throws Exception;

	/**
	 * Positions the given fetcher to the initial read offsets where the stream consumption
	 * will start from.
	 * 
	 * @param partitions The partitions for which to seeks the fetcher to the beginning.
	 * @param fetcher The fetcher that will pull data from Kafka and must be positioned.
	 */
	void seekFetcherToInitialOffsets(List<KafkaTopicPartitionLeader> partitions, Fetcher fetcher) throws Exception;

	/**
	 * Closes the offset handler, releasing all resources.
	 * 
	 * @throws IOException Thrown, if the closing fails.
	 */
	void close() throws IOException;
}
