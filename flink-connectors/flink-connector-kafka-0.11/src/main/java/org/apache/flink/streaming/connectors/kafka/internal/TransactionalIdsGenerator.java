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

package org.apache.flink.streaming.connectors.kafka.internal;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Class responsible for generating transactional ids to use when communicating with Kafka.
 */
public class TransactionalIdsGenerator {
	private final String prefix;
	private final int subtaskIndex;
	private final int poolSize;
	private final int safeScaleDownFactor;

	public TransactionalIdsGenerator(
			String prefix,
			int subtaskIndex,
			int poolSize,
			int safeScaleDownFactor) {
		this.prefix = checkNotNull(prefix);
		this.subtaskIndex = subtaskIndex;
		this.poolSize = poolSize;
		this.safeScaleDownFactor = safeScaleDownFactor;
	}

	/**
	 * Range of available transactional ids to use is:
	 * [nextFreeTransactionalId, nextFreeTransactionalId + parallelism * kafkaProducersPoolSize)
	 * loop below picks in a deterministic way a subrange of those available transactional ids based on index of
	 * this subtask.
	 */
	public Set<String> generateIdsToUse(long nextFreeTransactionalId) {
		Set<String> transactionalIds = new HashSet<>();
		for (int i = 0; i < poolSize; i++) {
			long transactionalId = nextFreeTransactionalId + subtaskIndex * poolSize + i;
			transactionalIds.add(generateTransactionalId(transactionalId));
		}
		return transactionalIds;
	}

	/**
	 *  If we have to abort previous transactional id in case of restart after a failure BEFORE first checkpoint
	 *  completed, we don't know what was the parallelism used in previous attempt. In that case we must guess the ids
	 *  range to abort based on current configured pool size, current parallelism and safeScaleDownFactor.
	 */
	public Set<String> generateIdsToAbort() {
		long abortTransactionalIdStart = subtaskIndex;
		long abortTransactionalIdEnd = abortTransactionalIdStart + 1;

		abortTransactionalIdStart *= poolSize * safeScaleDownFactor;
		abortTransactionalIdEnd *= poolSize * safeScaleDownFactor;
		return LongStream.range(abortTransactionalIdStart, abortTransactionalIdEnd)
			.mapToObj(this::generateTransactionalId)
			.collect(Collectors.toSet());
	}

	private String generateTransactionalId(long transactionalId) {
		return String.format(prefix + "-%d", transactionalId);
	}
}
