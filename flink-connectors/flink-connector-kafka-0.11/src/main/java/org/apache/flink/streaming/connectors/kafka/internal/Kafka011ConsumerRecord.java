/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.kafka.internal;

import org.apache.flink.shaded.guava18.com.google.common.base.Function;
import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.AbstractMap;
import java.util.Map;

/**
 * Extends base Kafka09ConsumerRecord to provide access to Kafka headers.
 */
class Kafka011ConsumerRecord extends Kafka09ConsumerRecord {
	/**
	 * Wraps {@link Header} as Map.Entry.
	 */
	private static final Function<Header, Map.Entry<String, byte[]>> HEADER_TO_MAP_ENTRY_FUNCTION =
		new Function<Header, Map.Entry<String, byte[]>>() {
			@Nonnull
			@Override
			public Map.Entry<String, byte[]> apply(@Nullable Header header) {
				return new AbstractMap.SimpleImmutableEntry<>(header.key(), header.value());
			}
		};

	Kafka011ConsumerRecord(ConsumerRecord<byte[], byte[]> consumerRecord) {
		super(consumerRecord);
	}

	@Override
	public Iterable<Map.Entry<String, byte[]>> headers() {
		return Iterables.transform(consumerRecord.headers(), HEADER_TO_MAP_ENTRY_FUNCTION);
	}
}
