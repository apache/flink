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

package org.apache.flink.connector.kafka.source.enumerator.initializer;

import org.apache.flink.annotation.Internal;

import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * An implementation of {@link OffsetsInitializer} which does not initialize anything.
 *
 * <p>This class is used as the default stopping offsets initializer for unbounded Kafka sources.
 */
@Internal
public class NoStoppingOffsetsInitializer implements OffsetsInitializer {
	private static final long serialVersionUID = 4186323669290142732L;

	@Override
	public Map<TopicPartition, Long> getPartitionOffsets(
			Collection<TopicPartition> partitions,
			PartitionOffsetsRetriever partitionOffsetsRetriever) {
		return Collections.emptyMap();
	}

	@Override
	public OffsetResetStrategy getAutoOffsetResetStrategy() {
		throw new UnsupportedOperationException(
			"The NoStoppingOffsetsInitializer does not have an OffsetResetStrategy. It should only be used " +
				"to end offset.");
	}
}
