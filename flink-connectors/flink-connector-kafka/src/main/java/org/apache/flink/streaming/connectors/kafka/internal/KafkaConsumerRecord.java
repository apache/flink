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

import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;

import org.apache.flink.shaded.guava18.com.google.common.base.Function;
import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

import javax.annotation.Nonnull;

import java.util.AbstractMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This class wraps Kafka modern {@link ConsumerRecord}
 *  to implement KeyedDeserializationSchema.Record interface.
 */
public class KafkaConsumerRecord implements KeyedDeserializationSchema.Record {
	protected final ConsumerRecord<byte[], byte[]> consumerRecord;
	/**
	 * Wraps {@link Header} as Map.Entry.
	 */
	private static final Function<Header, Map.Entry<String, byte[]>> HEADER_TO_MAP_ENTRY_FUNCTION =
		new Function<Header, Map.Entry<String, byte[]>>() {
			@Nonnull
			@Override
			public Map.Entry<String, byte[]> apply(Header header) {
				return new AbstractMap.SimpleImmutableEntry<>(header.key(), header.value());
			}
		};

	KafkaConsumerRecord(ConsumerRecord<byte[], byte[]> consumerRecord) {
		this.consumerRecord = checkNotNull(consumerRecord, "consumerRecord");
	}

	@Override
	public byte[] key() {
		return consumerRecord.key();
	}

	@Override
	public byte[] value() {
		return consumerRecord.value();
	}

	@Override
	public String topic() {
		return consumerRecord.topic();
	}

	@Override
	public int partition() {
		return consumerRecord.partition();
	}

	@Override
	public long offset() {
		return consumerRecord.offset();
	}

	@Override
	public Iterable<Map.Entry<String, byte[]>> headers() {
		return Iterables.transform(consumerRecord.headers(), HEADER_TO_MAP_ENTRY_FUNCTION);
	}
}
