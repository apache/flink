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

package org.apache.flink.streaming.connectors.kinesis.proxy;

import org.apache.flink.annotation.Internal;

import software.amazon.awssdk.services.kinesis.model.DeregisterStreamConsumerResponse;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamConsumerResponse;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryResponse;
import software.amazon.awssdk.services.kinesis.model.RegisterStreamConsumerResponse;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardRequest;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponseHandler;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Interface for a Kinesis proxy using AWS SDK v2.x operating on multiple Kinesis streams within the same AWS service region.
 */
@Internal
public interface KinesisProxyV2Interface {

	DescribeStreamSummaryResponse describeStreamSummary(String stream) throws InterruptedException, ExecutionException;

	DescribeStreamConsumerResponse describeStreamConsumer(final String streamConsumerArn) throws InterruptedException, ExecutionException;

	DescribeStreamConsumerResponse describeStreamConsumer(final String streamArn, final String consumerName) throws InterruptedException, ExecutionException;

	RegisterStreamConsumerResponse registerStreamConsumer(final String streamArn, final String consumerName) throws InterruptedException, ExecutionException;

	DeregisterStreamConsumerResponse deregisterStreamConsumer(final String consumerArn) throws InterruptedException, ExecutionException;

	CompletableFuture<Void> subscribeToShard(SubscribeToShardRequest request, SubscribeToShardResponseHandler responseHandler);

	/**
	 * Destroy any open resources used by the factory.
	 */
	default void close() {
		// Do nothing by default
	}

}
