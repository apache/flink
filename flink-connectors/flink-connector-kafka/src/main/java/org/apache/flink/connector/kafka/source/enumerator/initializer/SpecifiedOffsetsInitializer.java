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

import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * An implementation of {@link OffsetsInitializer} which initializes the offsets
 * of the partition according to the user specified offsets.
 *
 * <P>Package private and should be instantiated via {@link OffsetsInitializer}.
 */
class SpecifiedOffsetsInitializer implements OffsetsInitializer {
	private static final long serialVersionUID = 1649702397250402877L;
	private final Map<TopicPartition, Long> initialOffsets;
	private final OffsetResetStrategy offsetResetStrategy;

	SpecifiedOffsetsInitializer(
			Map<TopicPartition, Long> initialOffsets,
			OffsetResetStrategy offsetResetStrategy) {
		this.initialOffsets = Collections.unmodifiableMap(initialOffsets);
		this.offsetResetStrategy = offsetResetStrategy;
	}

	@Override
	public Map<TopicPartition, Long> getPartitionOffsets(
			Collection<TopicPartition> partitions,
			PartitionOffsetsRetriever partitionOffsetsRetriever) {
		Map<TopicPartition, Long> offsets = new HashMap<>();
		List<TopicPartition> toLookup = new ArrayList<>();
		for (TopicPartition tp : partitions) {
			Long offset = initialOffsets.get(tp);
			if (offset == null) {
				toLookup.add(tp);
			} else {
				offsets.put(tp, offset);
			}
		}
		if (!toLookup.isEmpty()) {
			switch (offsetResetStrategy) {
				case EARLIEST:
					offsets.putAll(partitionOffsetsRetriever.beginningOffsets(toLookup));
					break;
				case LATEST:
					offsets.putAll(partitionOffsetsRetriever.endOffsets(toLookup));
					break;
				default:
					throw new IllegalStateException("Cannot find initial offsets for partitions: " + toLookup);
			}
		}
		return offsets;
	}

	@Override
	public OffsetResetStrategy getAutoOffsetResetStrategy() {
		return offsetResetStrategy;
	}
}
