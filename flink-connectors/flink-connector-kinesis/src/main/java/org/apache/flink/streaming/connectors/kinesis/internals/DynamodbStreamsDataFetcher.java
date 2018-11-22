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

package org.apache.flink.streaming.connectors.kinesis.internals;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kinesis.KinesisShardAssigner;
import org.apache.flink.streaming.connectors.kinesis.metrics.ShardMetricsReporter;
import org.apache.flink.streaming.connectors.kinesis.model.DynamodbStreamsShardHandle;
import org.apache.flink.streaming.connectors.kinesis.model.SequenceNumber;
import org.apache.flink.streaming.connectors.kinesis.model.StreamShardHandle;
import org.apache.flink.streaming.connectors.kinesis.proxy.DynamodbStreamsProxy;
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisDeserializationSchema;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.DEFAULT_DYNAMODB_STREAM_SHARDID_FORMAT_CHECK;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.DYNAMODB_STREAM_SHARDID_FORMAT_CHECK;

/**
 * Dynamodb streams data fetcher.
 * @param <T> type of fetched data.
 */
public class DynamodbStreamsDataFetcher <T> extends KinesisDataFetcher<T> {
	private boolean shardIdFormatCheck = false;

	/**
	 * Constructor.
	 *
	 * @param streams list of streams to fetch data
	 * @param sourceContext source context
	 * @param runtimeContext runtime context
	 * @param configProps config properties
	 * @param deserializationSchema deserialization schema
	 * @param shardAssigner shard assigner
	 */
	public DynamodbStreamsDataFetcher(List<String> streams,
		SourceFunction.SourceContext<T> sourceContext,
		RuntimeContext runtimeContext,
		Properties configProps,
		KinesisDeserializationSchema<T> deserializationSchema,
		KinesisShardAssigner shardAssigner) {

		super(streams,
			sourceContext,
			sourceContext.getCheckpointLock(),
			runtimeContext,
			configProps,
			deserializationSchema,
			shardAssigner,
			new AtomicReference<>(),
			new ArrayList<>(),
			createInitialSubscribedStreamsToLastDiscoveredShardsState(streams),
			// use DynamodbStreamsProxy
			DynamodbStreamsProxy::create);

		shardIdFormatCheck = Boolean.valueOf(configProps.getProperty(
				DYNAMODB_STREAM_SHARDID_FORMAT_CHECK,
				DEFAULT_DYNAMODB_STREAM_SHARDID_FORMAT_CHECK));
	}

	/**
	 * Updates the last discovered shard of a subscribed stream; only updates if the update is valid.
	 */
	@Override
	public void advanceLastDiscoveredShardOfStream(String stream, String shardId) {
		String lastSeenShardIdOfStream = subscribedStreamsToLastDiscoveredShardIds.get(stream);

		if (lastSeenShardIdOfStream == null) {
			// if not previously set, simply put as the last seen shard id
			subscribedStreamsToLastDiscoveredShardIds.put(stream, shardId);
		} else {
			if (shardIdFormatCheck &&
				DynamodbStreamsShardHandle.compareShardIds(shardId, lastSeenShardIdOfStream) <= 0) {
				// Update is valid only if the given shard id is greater
				// than the previous last seen shard id of the stream.
				return;
			}
			subscribedStreamsToLastDiscoveredShardIds.put(stream, shardId);
		}
	}

	/**
	 * Create a new DynamoDB streams shard consumer.
	 *
	 * @param subscribedShardStateIndex the state index of the shard this consumer is subscribed to
	 * @param handle stream handle
	 * @param lastSeqNum last sequence number
	 * @param shardMetricsReporter the reporter to report metrics to
	 * @return
	 */
	@Override
	protected ShardConsumer createShardConsumer(
		Integer subscribedShardStateIndex,
		StreamShardHandle handle,
		SequenceNumber lastSeqNum,
		ShardMetricsReporter shardMetricsReporter) {

		return new ShardConsumer(
			this,
			subscribedShardStateIndex,
			handle,
			lastSeqNum,
			DynamodbStreamsProxy.create(getConsumerConfiguration()),
			shardMetricsReporter);
	}
}
