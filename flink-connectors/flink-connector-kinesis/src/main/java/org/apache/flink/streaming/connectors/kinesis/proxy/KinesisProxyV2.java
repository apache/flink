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
import org.apache.flink.streaming.connectors.kinesis.util.AwsV2Util;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.DeregisterStreamConsumerRequest;
import software.amazon.awssdk.services.kinesis.model.DeregisterStreamConsumerResponse;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamConsumerRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamConsumerResponse;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse;
import software.amazon.awssdk.services.kinesis.model.RegisterStreamConsumerRequest;
import software.amazon.awssdk.services.kinesis.model.RegisterStreamConsumerResponse;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardRequest;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponseHandler;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Kinesis proxy implementation using AWS SDK v2.x - a utility class that is used as a proxy to make
 * calls to AWS Kinesis for several EFO (Enhanced Fan Out) functions, such as de-/registering stream consumers,
 * subscribing to a shard and receiving records from a shard.
 */
@Internal
public class KinesisProxyV2 implements KinesisProxyV2Interface {

	private static final Logger LOG = LoggerFactory.getLogger(KinesisProxyV2.class);

	/** An Asynchronous client used to communicate with AWS services. */
	private final KinesisAsyncClient kinesisAsyncClient;

	private final SdkAsyncHttpClient httpClient;

	private final FanOutRecordPublisherConfiguration fanOutRecordPublisherConfiguration;

	private final FullJitterBackoff backoff;

	/**
	 * Create a new KinesisProxyV2.
	 *
	 * @param kinesisAsyncClient AWS SDK v2 Kinesis client used to communicate with AWS services
	 * @param httpClient the underlying HTTP client, reference required for close only
	 * @param fanOutRecordPublisherConfiguration the configuration for Fan Out features
	 * @param backoff the backoff utility used to introduce Full Jitter delays
	 */
	public KinesisProxyV2(
			final KinesisAsyncClient kinesisAsyncClient,
			final SdkAsyncHttpClient httpClient,
			final FanOutRecordPublisherConfiguration fanOutRecordPublisherConfiguration,
			final FullJitterBackoff backoff) {
		this.kinesisAsyncClient = Preconditions.checkNotNull(kinesisAsyncClient);
		this.httpClient = httpClient;
		this.fanOutRecordPublisherConfiguration = fanOutRecordPublisherConfiguration;
		this.backoff = backoff;
	}

	@Override
	public CompletableFuture<Void> subscribeToShard(
		final SubscribeToShardRequest request,
		final SubscribeToShardResponseHandler responseHandler) {
		return kinesisAsyncClient.subscribeToShard(request, responseHandler);
	}

	@Override
	public void close() {
		kinesisAsyncClient.close();
		httpClient.close();
	}

	@Override
	public DescribeStreamResponse describeStream(String stream) throws InterruptedException, ExecutionException {
		DescribeStreamRequest describeStreamRequest = DescribeStreamRequest
			.builder()
			.streamName(stream)
			.build();

		return invokeWithRetryAndBackoff(
			() -> kinesisAsyncClient.describeStream(describeStreamRequest).get(),
			fanOutRecordPublisherConfiguration.getDescribeStreamBaseBackoffMillis(),
			fanOutRecordPublisherConfiguration.getDescribeStreamMaxBackoffMillis(),
			fanOutRecordPublisherConfiguration.getDescribeStreamExpConstant(),
			fanOutRecordPublisherConfiguration.getDescribeStreamMaxRetries());
	}

	@Override
	public DescribeStreamConsumerResponse describeStreamConsumer(final String streamArn, final String consumerName) throws InterruptedException, ExecutionException  {
		DescribeStreamConsumerRequest describeStreamConsumerRequest = DescribeStreamConsumerRequest
			.builder()
			.streamARN(streamArn)
			.consumerName(consumerName)
			.build();

		return describeStreamConsumer(describeStreamConsumerRequest);
	}

	@Override
	public DescribeStreamConsumerResponse describeStreamConsumer(final String streamConsumerArn) throws InterruptedException, ExecutionException  {
		DescribeStreamConsumerRequest describeStreamConsumerRequest = DescribeStreamConsumerRequest
			.builder()
			.consumerARN(streamConsumerArn)
			.build();

		return describeStreamConsumer(describeStreamConsumerRequest);
	}

	private DescribeStreamConsumerResponse describeStreamConsumer(final DescribeStreamConsumerRequest request) throws InterruptedException, ExecutionException  {
		return invokeWithRetryAndBackoff(
			() -> kinesisAsyncClient.describeStreamConsumer(request).get(),
			fanOutRecordPublisherConfiguration.getDescribeStreamConsumerBaseBackoffMillis(),
			fanOutRecordPublisherConfiguration.getDescribeStreamConsumerMaxBackoffMillis(),
			fanOutRecordPublisherConfiguration.getDescribeStreamConsumerExpConstant(),
			fanOutRecordPublisherConfiguration.getDescribeStreamConsumerMaxRetries());
	}

	@Override
	public RegisterStreamConsumerResponse registerStreamConsumer(final String streamArn, final String consumerName) throws InterruptedException, ExecutionException {
		RegisterStreamConsumerRequest registerStreamConsumerRequest = RegisterStreamConsumerRequest
			.builder()
			.streamARN(streamArn)
			.consumerName(consumerName)
			.build();

		return invokeWithRetryAndBackoff(
			() -> kinesisAsyncClient.registerStreamConsumer(registerStreamConsumerRequest).get(),
			fanOutRecordPublisherConfiguration.getRegisterStreamBaseBackoffMillis(),
			fanOutRecordPublisherConfiguration.getRegisterStreamMaxBackoffMillis(),
			fanOutRecordPublisherConfiguration.getRegisterStreamExpConstant(),
			fanOutRecordPublisherConfiguration.getRegisterStreamMaxRetries());
	}

	@Override
	public DeregisterStreamConsumerResponse deregisterStreamConsumer(final String consumerArn) throws InterruptedException, ExecutionException {
		DeregisterStreamConsumerRequest deregisterStreamConsumerRequest = DeregisterStreamConsumerRequest
			.builder()
			.consumerARN(consumerArn)
			.build();

		return invokeWithRetryAndBackoff(
			() -> kinesisAsyncClient.deregisterStreamConsumer(deregisterStreamConsumerRequest).get(),
			fanOutRecordPublisherConfiguration.getDeregisterStreamBaseBackoffMillis(),
			fanOutRecordPublisherConfiguration.getDeregisterStreamMaxBackoffMillis(),
			fanOutRecordPublisherConfiguration.getDeregisterStreamExpConstant(),
			fanOutRecordPublisherConfiguration.getDeregisterStreamMaxRetries());
	}

	private <T> T invokeWithRetryAndBackoff(
			final ResponseSupplier<T> responseSupplier,
			final long jitterBase,
			final long jitterMax,
			final double jitterExponent,
			final int maximumNumberOfRetries) throws InterruptedException, ExecutionException {
		T response = null;
		int attempt = 0;

		while (attempt < maximumNumberOfRetries && response == null) {
			try {
				response = responseSupplier.get();
			} catch (Exception ex) {
				if (AwsV2Util.isRecoverableException(ex)) {
					long backoffMillis = backoff.calculateFullJitterBackoff(jitterBase, jitterMax, jitterExponent, ++attempt);
					LOG.warn("Encountered recoverable error: {}. Backing off for {} millis.",
						ex.getClass().getSimpleName(), backoffMillis, ex);

					backoff.sleep(backoffMillis);
				} else {
					throw ex;
				}
			}
		}

		if (response == null) {
			throw new RuntimeException("Retries exceeded - all " + maximumNumberOfRetries + " retry attempts failed.");
		}

		return response;
	}

	private interface ResponseSupplier<T> {
		T get() throws ExecutionException, InterruptedException;
	}

}
