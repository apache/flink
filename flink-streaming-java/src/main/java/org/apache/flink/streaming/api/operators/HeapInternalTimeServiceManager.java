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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

/**
 * An entity keeping all the time-related services available to all operators extending the
 * {@link AbstractStreamOperator}. Right now, this is only a
 * {@link HeapInternalTimerService timer services}.
 *
 * <b>NOTE:</b> These services are only available to keyed operators.
 *
 * @param <K> The type of keys used for the timers and the registry.
 * @param <N> The type of namespace used for the timers.
 */
@Internal
public class HeapInternalTimeServiceManager<K, N> extends InternalTimeServiceManager<K, N> {

	public HeapInternalTimeServiceManager(
			Environment env,
			JobID jobID,
			String operatorIdentifier,
			int totalKeyGroups,
			KeyGroupRange localKeyGroupRange,
			TypeSerializer<K> keySerializer,
			KeyContext keyContext,
			ProcessingTimeService processingTimeService) {
		super(env, jobID, operatorIdentifier, totalKeyGroups,
			localKeyGroupRange, keySerializer, keyContext,
			processingTimeService);
	}

	@Override
	protected InternalTimerService<K, N> createInternalTimerService() {
		return new HeapInternalTimerService<>(totalKeyGroups, localKeyGroupRange, keyContext, processingTimeService);
	}
}
