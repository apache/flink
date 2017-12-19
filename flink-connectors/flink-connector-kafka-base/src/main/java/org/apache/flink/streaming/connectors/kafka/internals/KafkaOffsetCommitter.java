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

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.connectors.kafka.config.OffsetCommitMode;

import javax.annotation.Nonnull;

import java.util.Map;

/**
 * The offset committer service that commits offsets back to Kafka's brokers / Zookeeper.
 * Committed offsets serves only as a means to expose the consumer progress to the outside world, and
 * is not used for Flink's exactly-once guarantees.
 */
@Internal
public interface KafkaOffsetCommitter {

	/**
	 * Commits the given partition offsets to the Kafka brokers (or to ZooKeeper for
	 * older Kafka versions). This method is only ever called when the offset commit mode of
	 * the consumer is {@link OffsetCommitMode#ON_CHECKPOINTS}.
	 *
	 * <p>The given offsets are the internal checkpointed offsets, representing
	 * the last processed record of each partition. Version-specific implementations of this method
	 * need to hold the contract that the given offsets must be incremented by 1 before
	 * committing them, so that committed offsets to Kafka represent "the next record to process".
	 *
	 * @param offsets The offsets to commit to Kafka (implementations must increment offsets by 1 before committing).
	 * @param commitCallback The callback that the user should trigger when a commit request completes or fails.
	 * @throws Exception This method forwards exceptions.
	 */
	void commitInternalOffsetsToKafka(
			Map<KafkaTopicPartition, Long> offsets,
			@Nonnull KafkaCommitCallback commitCallback) throws Exception;
}
