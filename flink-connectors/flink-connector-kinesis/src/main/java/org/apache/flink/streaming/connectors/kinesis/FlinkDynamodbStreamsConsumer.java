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

package org.apache.flink.streaming.connectors.kinesis;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kinesis.internals.DynamodbStreamsDataFetcher;
import org.apache.flink.streaming.connectors.kinesis.serialization.DynamodbStreamsSchema;
import org.apache.flink.streaming.connectors.kinesis.internals.KinesisDataFetcher;
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisDeserializationSchema;

import com.amazonaws.services.dynamodbv2.model.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Consume events from the dyanmodbdb streams.
 *
 * @param <T> the type of data emitted
 */
public class FlinkDynamodbStreamsConsumer<T> extends FlinkKinesisConsumer<T> {
	private static final Logger LOG = LoggerFactory.getLogger(FlinkDynamodbStreamsConsumer.class);

	/**
	 * Constructor of FlinkDynamodbStreamsConsumer.
	 *
	 * @param stream stream to consume
	 * @param deserializer deserialization schema
	 * @param config config properties
	 */
	public FlinkDynamodbStreamsConsumer(
			String stream,
			DeserializationSchema<T> deserializer,
			Properties config) {
		super(stream, deserializer, config);
	}

	/**
	 * Constructor of FlinkDynamodbStreamConsumer.
	 *
	 * @param streams list of streams to consume
	 * @param deserializer  deserialization schema
	 * @param config config properties
	 */
	public FlinkDynamodbStreamsConsumer(
			List<String> streams,
			KinesisDeserializationSchema deserializer,
			Properties config) {
		super(streams, deserializer, config);
	}

	public static <T> FlinkDynamodbStreamsConsumer<T> create(String stream,
			DeserializationSchema<T> deserializer,
			Properties config) {
		return new FlinkDynamodbStreamsConsumer<>(stream, deserializer, config);
	}

	public static <T> FlinkDynamodbStreamsConsumer<T> create(List<String> streams,
			KinesisDeserializationSchema<T> deserializer,
			Properties config) {
		return new FlinkDynamodbStreamsConsumer<>(streams, deserializer, config);
	}

	public static FlinkDynamodbStreamsConsumer<Record> create(String stream,
			Properties config) {
		return create(Collections.singletonList(stream), new DynamodbStreamsSchema(), config);
	}

	public static FlinkDynamodbStreamsConsumer<Record> create(List<String> streams,
			Properties config) {
		return create(streams, new DynamodbStreamsSchema(), config);
	}

	@Override
	protected KinesisDataFetcher<T> createFetcher(
			List<String> streams,
			SourceFunction.SourceContext<T> sourceContext,
			RuntimeContext runtimeContext,
			Properties configProps,
			KinesisDeserializationSchema<T> deserializationSchema) {
		return new DynamodbStreamsDataFetcher<T>(
				streams,
				sourceContext,
				runtimeContext,
				configProps,
				deserializationSchema,
				getShardAssigner());
	}
}
