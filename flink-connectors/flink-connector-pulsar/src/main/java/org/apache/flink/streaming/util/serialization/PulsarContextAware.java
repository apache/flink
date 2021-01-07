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

package org.apache.flink.streaming.util.serialization;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;

import org.apache.pulsar.client.api.Schema;

import java.util.Optional;

/**
 * An interface for {@link PulsarDeserializationSchema dynamicPulsarSerializationSchema} that need information
 * about the context where the Pulsar Producer is running along with information about the available
 * partitions.
 *
 * <p>You only need to override the methods for the information that you need. However, {@link
 * #getTargetTopic(Object)} is required because it is used to determine the available partitions.
 */
@PublicEvolving
public interface PulsarContextAware<T> extends ResultTypeQueryable<T> {

	/**
	 * Returns the topic that the presented element should be sent to.
	 */
	default Optional<String> getTargetTopic(T element) {
		return Optional.empty();
	}

	/**
	 * Sets the number of the parallel subtask that the Pulsar Sink is running on. The numbering
	 * starts from 0 and goes up to parallelism-1 (parallelism as returned by {@link
	 * #setNumParallelInstances(int)}).
	 */
	default void setParallelInstanceId(int parallelInstanceId) {
	}

	/**
	 * Sets the parallelism with which the parallel task of the Pulsar Sink runs.
	 */
	default void setNumParallelInstances(int numParallelInstances) {
	}

	Schema<T> getSchema();
}
