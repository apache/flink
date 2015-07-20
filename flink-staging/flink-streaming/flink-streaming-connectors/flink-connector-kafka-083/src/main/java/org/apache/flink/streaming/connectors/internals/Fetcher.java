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
package org.apache.flink.streaming.connectors.internals;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.kafka.copied.common.TopicPartition;

import java.util.List;
import java.util.Map;

public interface Fetcher {

	/**
	 * Set which partitions we want to read from
	 * @param partitions
	 */
	void partitionsToRead(List<TopicPartition> partitions);

	/**
	 * Ask the run() method to stop reading
	 */
	void stop();

	/**
	 * Close the underlying connection
	 */
	void close();

	/**
	 * Start and fetch indefinitely from the underlying fetcher
	 * @param sourceContext
	 * @param valueDeserializer
	 * @param lastOffsets
	 * @param <T>
	 */
	<T> void run(SourceFunction.SourceContext<T> sourceContext, DeserializationSchema<T> valueDeserializer, long[] lastOffsets);

	/**
	 * Commit offset (if supported)
	 * @param offsetsToCommit
	 */
	void commit(Map<TopicPartition, Long> offsetsToCommit);

	/**
	 * Set offsets for the partitions.
	 * The offset is the next offset to read. So if set to 0, the Fetcher's first result will be the msg with offset=0.
	 */
	void seek(TopicPartition topicPartition, long offsetToRead);
}
