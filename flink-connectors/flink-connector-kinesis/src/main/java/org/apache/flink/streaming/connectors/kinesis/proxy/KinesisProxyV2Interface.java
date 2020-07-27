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
import org.apache.flink.streaming.connectors.kinesis.internals.publisher.fanout.FanOutRecordPublisherConfiguration;
import org.apache.flink.streaming.connectors.kinesis.internals.publisher.fanout.FanOutStreamConsumerInfo;

import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse;

import java.util.concurrent.ExecutionException;

/**
 * Interface for a Kinesis proxy using AWS SDK v2.x operating on multiple Kinesis streams within the same AWS service region.
 */
@Internal
public interface KinesisProxyV2Interface {
	/**
	 * Send a describeStream request via AWS SDK v2.x to get the stream arn for each stream.
	 * @param stream the stream name to be described.
	 * @return a describe stream response.
	 * @throws InterruptedException this method will retry with backoff if AWS Kinesis complains that the
	 * 	                                 operation has exceeded the rate limit; this exception will be thrown
	 * 	                                 if the backoff is interrupted.
	 */
	DescribeStreamResponse describeStream(String stream) throws InterruptedException, ExecutionException;

	/**
	 * Send a registerStream request via AWS SDK v2.x to get the consumer arn for each stream consumer, consumer name is set via {@link FanOutRecordPublisherConfiguration}.
	 * @param stream the stream to be registered.
	 * @param streamArn the stream's arn to be registered.
	 * @return a fan out stream info. {@link FanOutStreamConsumerInfo}
	 * @throws InterruptedException this method will retry with backoff if AWS Kinesis complains that the
	 * 	  	                             operation has exceeded the rate limit; this exception will be thrown
	 * 	  	                             if the backoff is interrupted.
	 */
	FanOutStreamConsumerInfo registerStreamConsumer(String stream, String streamArn) throws InterruptedException, ExecutionException;

	/**
	 * Send a deregisterStream request via AWS SDK v2.x to derigster each consumer.
	 * @param fanOutStreamConsumerInfo an instance of fan out stream info. {@link FanOutStreamConsumerInfo}
	 * @throws InterruptedException this method will retry with backoff if AWS Kinesis complains that the
	 * 	  	  	                         operation has exceeded the rate limit; this exception will be thrown
	 * 	  	  	                         if the backoff is interrupted.
	 */
	void deregisterStreamConsumer(FanOutStreamConsumerInfo fanOutStreamConsumerInfo) throws InterruptedException, ExecutionException;
}
