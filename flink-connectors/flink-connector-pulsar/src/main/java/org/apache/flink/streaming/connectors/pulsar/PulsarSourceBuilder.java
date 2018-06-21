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

package org.apache.flink.streaming.connectors.pulsar;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Preconditions;

/**
 * A class for building a pulsar source.
 */
@PublicEvolving
public class PulsarSourceBuilder<T> {

	final DeserializationSchema<T> deserializationSchema;
	String serviceUrl = Defaults.SERVICE_URL;
	String topic;
	String subscriptionName = "flink-sub";
	long acknowledgementBatchSize = Defaults.ACKNOWLEDGEMENT_BATCH_SIZE;

	private PulsarSourceBuilder(DeserializationSchema<T> deserializationSchema) {
		this.deserializationSchema = deserializationSchema;
	}

	/**
	 * Sets the pulsar service url to connect to. Defaults to pulsar://localhost:6650.
	 *
	 * @param serviceUrl service url to connect to
	 * @return this builder
	 */
	public PulsarSourceBuilder<T> serviceUrl(String serviceUrl) {
		Preconditions.checkNotNull(serviceUrl);
		this.serviceUrl = serviceUrl;
		return this;
	}

	/**
	 * Sets the topic to consumer from. This is required.
	 *
	 * <p>Topic names (https://pulsar.incubator.apache.org/docs/latest/getting-started/ConceptsAndArchitecture/#Topics)
	 * are in the following format:
	 * {persistent|non-persistent}://tenant/namespace/topic
	 *
	 * @param topic the topic to consumer from
	 * @return this builder
	 */
	public PulsarSourceBuilder<T> topic(String topic) {
		Preconditions.checkNotNull(topic);
		this.topic = topic;
		return this;
	}

	/**
	 * Sets the subscription name for the topic consumer. Defaults to flink-sub.
	 *
	 * @param subscriptionName the subscription name for the topic consumer
	 * @return this builder
	 */
	public PulsarSourceBuilder<T> subscriptionName(String subscriptionName) {
		Preconditions.checkNotNull(subscriptionName);
		this.subscriptionName = subscriptionName;
		return this;
	}

	/**
	 * Sets the number of messages to receive before acknowledging. This defaults to 100. This
	 * value is only used when checkpointing is disabled.
	 *
	 * @param size number of messages to receive before acknowledging
	 * @return this builder
	 */
	public PulsarSourceBuilder<T> acknowledgementBatchSize(long size) {
		if (size > 0 && size <= Defaults.MAX_ACKNOWLEDGEMENT_BATCH_SIZE) {
			acknowledgementBatchSize = size;
		}
		return this;
	}

	public SourceFunction<T> build() {
		Preconditions.checkNotNull(serviceUrl, "a service url is required");
		Preconditions.checkNotNull(topic, "a topic is required");
		Preconditions.checkNotNull(subscriptionName, "a subscription name is required");

		return new PulsarConsumerSource<>(this);
	}

	/**
	 * Creates a PulsarSourceBuilder.
	 *
	 * @param deserializationSchema the deserializer used to convert between Pulsar's byte messages and Flink's objects.
	 * @return a builder
	 */
	public static <T> PulsarSourceBuilder<T> builder(DeserializationSchema<T> deserializationSchema) {
		Preconditions.checkNotNull(deserializationSchema);
		return new PulsarSourceBuilder<>(deserializationSchema);
	}
}
