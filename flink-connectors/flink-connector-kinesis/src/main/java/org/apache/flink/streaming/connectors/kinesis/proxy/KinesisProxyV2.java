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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.internals.publisher.fanout.FanOutRecordPublisherConfiguration;
import org.apache.flink.streaming.connectors.kinesis.internals.publisher.fanout.FanOutStreamConsumerInfo;
import org.apache.flink.streaming.connectors.kinesis.util.AwsV2Util;
import org.apache.flink.util.Preconditions;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.ClientConfigurationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.DeregisterStreamConsumerRequest;
import software.amazon.awssdk.services.kinesis.model.DeregisterStreamConsumerResponse;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamConsumerRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamConsumerResponse;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse;
import software.amazon.awssdk.services.kinesis.model.RegisterStreamConsumerRequest;
import software.amazon.awssdk.services.kinesis.model.RegisterStreamConsumerResponse;

import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

/**
 * Kinesis proxy implementation using AWS SDK v2.x - a utility class that is used as a proxy to make
 * calls to AWS Kinesis for several EFO (Enhanced Fan Out) functions, such as de-/registering stream consumers,
 * subscribing to a shard and receiving records from a shard.
 */
@Internal
public class KinesisProxyV2 implements KinesisProxyV2Interface {
	private static final Logger LOG = LoggerFactory.getLogger(KinesisProxyV2.class);

	/** Random seed used to calculate backoff jitter for Kinesis operations. */
	private static final Random seed = new Random();

	private final KinesisAsyncClient kinesisAsyncClient;

	private final FanOutRecordPublisherConfiguration fanOutRecordPublisherConfiguration;

	/**
	 * Create a new KinesisProxyV2 based on the supplied configuration properties.
	 *
	 * @param configProps configuration properties containing AWS credential and AWS region info
	 */
	public KinesisProxyV2(final Properties configProps, List<String> streams) {
		this.kinesisAsyncClient = createKinesisAsyncClient(configProps);
		this.fanOutRecordPublisherConfiguration = new FanOutRecordPublisherConfiguration(configProps, streams);
	}

	/**
	 * Creates a Kinesis proxy V2.
	 *
	 * @param configProps configuration properties
	 * @param streams list of kinesis stream names
	 * @return the created kinesis proxy v2
	 */
	public static KinesisProxyV2Interface create(Properties configProps, List<String> streams) {
		return new KinesisProxyV2(configProps, streams);
	}

	/**
	 * Create the Kinesis client, using the provided configuration properties.
	 * Derived classes can override this method to customize the client configuration.
	 *
	 * @param configProps the properties map used to create the Kinesis Client
	 * @return a Kinesis Client
	 */
	protected KinesisAsyncClient createKinesisAsyncClient(final Properties configProps) {
		final ClientConfiguration config = new ClientConfigurationFactory().getConfig();
		return AwsV2Util.createKinesisAsyncClient(configProps, config);
	}

	@Override
	public DescribeStreamResponse describeStream(String stream) throws InterruptedException, ExecutionException {
		DescribeStreamRequest describeStreamRequest = DescribeStreamRequest
			.builder()
			.streamName(stream)
			.build();
		DescribeStreamResponse describeStreamResponse = null;

		int retryCount = 0;
		while (retryCount <= fanOutRecordPublisherConfiguration.getDescribeStreamMaxRetries() && describeStreamResponse == null) {
			try {
				describeStreamResponse = kinesisAsyncClient.describeStream(describeStreamRequest).get();
			} catch (ExecutionException ex) {
				if (AwsV2Util.isRecoverableException(ex)) {
					long backoffMillis = fullJitterBackoff(
						fanOutRecordPublisherConfiguration.getDescribeStreamBaseBackoffMillis(), fanOutRecordPublisherConfiguration.getDescribeStreamMaxBackoffMillis(), fanOutRecordPublisherConfiguration.getDescribeStreamExpConstant(), retryCount++);
					LOG.warn("Got recoverable AmazonServiceException when trying to describe stream " + stream + ". Backing off for "
						+ backoffMillis + " millis (" + ex.getClass().getName() + ": " + ex.getMessage() + ")");
					Thread.sleep(backoffMillis);
				} else {
					throw ex;
				}
			}
		}
		if (describeStreamResponse == null) {
			throw new RuntimeException("Retries exceeded for describeStream operation - all " + fanOutRecordPublisherConfiguration.getDescribeStreamMaxRetries() +
				" retry attempts failed.");
		}
		return describeStreamResponse;
	}

	@VisibleForTesting
	private FanOutStreamConsumerInfo describeStreamConsumer(String stream, String streamArn, String consumerName) throws InterruptedException, ExecutionException  {
		DescribeStreamConsumerRequest describeStreamConsumerRequest = DescribeStreamConsumerRequest
			.builder()
			.streamARN(streamArn)
			.consumerName(consumerName)
			.build();
		FanOutStreamConsumerInfo fanOutStreamConsumerInfo = null;
		int retryCount = 0;
		while (retryCount <= fanOutRecordPublisherConfiguration.getDescribeStreamConsumerMaxRetries() && fanOutStreamConsumerInfo == null) {
			try {
				DescribeStreamConsumerResponse describeStreamConsumerResponse = kinesisAsyncClient.describeStreamConsumer(describeStreamConsumerRequest).get();
				fanOutStreamConsumerInfo = new FanOutStreamConsumerInfo(stream, streamArn, consumerName, describeStreamConsumerResponse.consumerDescription().consumerARN());
			} catch (ExecutionException ex) {
				if (AwsV2Util.isRecoverableException(ex)) {
					long backoffMillis = fullJitterBackoff(
						fanOutRecordPublisherConfiguration.getDescribeStreamConsumerBaseBackoffMillis(), fanOutRecordPublisherConfiguration.getDescribeStreamConsumerMaxBackoffMillis(), fanOutRecordPublisherConfiguration.getDescribeStreamConsumerExpConstant(), retryCount++);
					LOG.warn("Got recoverable AmazonServiceException when trying to describe stream consumer " + stream + ". Backing off for "
						+ backoffMillis + " millis (" + ex.getClass().getName() + ": " + ex.getMessage() + ")");
					Thread.sleep(backoffMillis);
				} else {
					throw ex;
				}
			}
		}

		if (fanOutStreamConsumerInfo == null) {
			throw new RuntimeException("Retries exceeded for describeStreamConsumer operation - all " + fanOutRecordPublisherConfiguration.getDescribeStreamConsumerMaxRetries() +
				" retry attempts failed.");
		}
		return fanOutStreamConsumerInfo;
	}

	@VisibleForTesting
	private FanOutStreamConsumerInfo registerStreamConsumerDirectly(String stream, String streamArn, String consumerName) throws InterruptedException, ExecutionException {
		RegisterStreamConsumerRequest registerStreamConsumerRequest = RegisterStreamConsumerRequest
			.builder()
			.streamARN(streamArn)
			.consumerName(consumerName)
			.build();
		FanOutStreamConsumerInfo fanOutStreamConsumerInfo = null;
		int retryCount = 0;
		while (retryCount <= fanOutRecordPublisherConfiguration.getRegisterStreamMaxRetries() && fanOutStreamConsumerInfo == null) {
			try {
				RegisterStreamConsumerResponse registerStreamConsumerResponse = kinesisAsyncClient.registerStreamConsumer(registerStreamConsumerRequest).get();
				fanOutStreamConsumerInfo = new FanOutStreamConsumerInfo(stream, streamArn, consumerName, registerStreamConsumerResponse.consumer().consumerARN());
			} catch (ExecutionException ex) {
				if (AwsV2Util.isRecoverableException(ex)) {
					long backoffMillis = fullJitterBackoff(
						fanOutRecordPublisherConfiguration.getRegisterStreamBaseBackoffMillis(), fanOutRecordPublisherConfiguration.getRegisterStreamMaxBackoffMillis(), fanOutRecordPublisherConfiguration.getRegisterStreamExpConstant(), retryCount++);
					LOG.warn("Got recoverable AmazonServiceException when trying to register " + stream + ". Backing off for "
						+ backoffMillis + " millis (" + ex.getClass().getName() + ": " + ex.getMessage() + ")");
					Thread.sleep(backoffMillis);
				} else {
					throw ex;
				}
			}
		}
		if (fanOutStreamConsumerInfo == null) {
			throw new RuntimeException("Retries exceeded for registerStream operation - all " + fanOutRecordPublisherConfiguration.getRegisterStreamMaxRetries() +
				" retry attempts failed.");
		}
		return fanOutStreamConsumerInfo;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public FanOutStreamConsumerInfo registerStreamConsumer(String stream, String streamArn) throws InterruptedException, ExecutionException {
		if (this.fanOutRecordPublisherConfiguration.getEfoRegistrationType() == ConsumerConfigConstants.EFORegistrationType.NONE){
			throw new IllegalArgumentException("EFO registration strategy of NONE can not call register stream operation.");
		}
		Preconditions.checkArgument(fanOutRecordPublisherConfiguration.getConsumerName().isPresent(), "Consumer name is not set.");
		if (this.fanOutRecordPublisherConfiguration.getEfoRegistrationType() == ConsumerConfigConstants.EFORegistrationType.LAZY){
			long backoffMillis = fullJitterBackoff(
				fanOutRecordPublisherConfiguration.getRegisterStreamBaseBackoffMillis(), fanOutRecordPublisherConfiguration.getRegisterStreamMaxBackoffMillis(), fanOutRecordPublisherConfiguration.getRegisterStreamExpConstant(), 1);
			Thread.sleep(backoffMillis);
		}
		String consumerName = fanOutRecordPublisherConfiguration.getConsumerName().get();
		FanOutStreamConsumerInfo result;
		try {
			result = describeStreamConsumer(stream, streamArn, consumerName);
		} catch (ExecutionException ex) {
			if (AwsV2Util.isResourceNotFound(ex)) {
				try {
					long backoffMillis = fullJitterBackoff(
						fanOutRecordPublisherConfiguration.getRegisterStreamBaseBackoffMillis(), fanOutRecordPublisherConfiguration.getRegisterStreamMaxBackoffMillis(), fanOutRecordPublisherConfiguration.getRegisterStreamExpConstant(), 1);
					Thread.sleep(backoffMillis);
					result = registerStreamConsumerDirectly(stream, streamArn, consumerName);
				} catch (ExecutionException ex1) {
					if (AwsV2Util.isResourceInUse(ex1)) {
						result = describeStreamConsumer(stream, streamArn, consumerName);
					} else {
						throw ex1;
					}
				}
			} else {
				throw ex;
			}
		}
		return result;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void deregisterStreamConsumer(FanOutStreamConsumerInfo fanOutStreamConsumerInfo) throws InterruptedException, ExecutionException {
		if (this.fanOutRecordPublisherConfiguration.getEfoRegistrationType() == ConsumerConfigConstants.EFORegistrationType.NONE){
			throw new IllegalArgumentException("EFO registration strategy of NONE can not call de-register stream operation.");
		}
		//Force deregister operation to first sleep for a while.
		long backoffMillis = fullJitterBackoff(
			fanOutRecordPublisherConfiguration.getDeregisterStreamBaseBackoffMillis(), fanOutRecordPublisherConfiguration.getDeregisterStreamMaxBackoffMillis(), fanOutRecordPublisherConfiguration.getDeregisterStreamExpConstant(), 1);
		Thread.sleep(backoffMillis);

		DeregisterStreamConsumerRequest deregisterStreamConsumerRequest = DeregisterStreamConsumerRequest
			.builder()
			.consumerARN(fanOutStreamConsumerInfo.getConsumerArn())
			.consumerName(fanOutStreamConsumerInfo.getConsumerName())
			.build();

		int retryCount = 0;
		DeregisterStreamConsumerResponse deregisterStreamConsumerResponse = null;
		while (retryCount <= fanOutRecordPublisherConfiguration.getDeregisterStreamMaxRetries() && deregisterStreamConsumerResponse == null) {
			try {
				deregisterStreamConsumerResponse = kinesisAsyncClient.deregisterStreamConsumer(deregisterStreamConsumerRequest).get();
			} catch (ExecutionException ex) {
				if (AwsV2Util.isResourceNotFound(ex)) {
					//it indicates that someone else has de-registered the consumer.
					return;
				}
				if (AwsV2Util.isRecoverableException(ex)) {
					backoffMillis = fullJitterBackoff(
						fanOutRecordPublisherConfiguration.getDeregisterStreamBaseBackoffMillis(), fanOutRecordPublisherConfiguration.getDeregisterStreamMaxBackoffMillis(), fanOutRecordPublisherConfiguration.getDeregisterStreamExpConstant(), retryCount++);
					LOG.warn("Got recoverable AmazonServiceException when trying to deregister " + fanOutStreamConsumerInfo.getStream() + ". Backing off for "
						+ backoffMillis + " millis (" + ex.getClass().getName() + ": " + ex.getMessage() + ")");
					Thread.sleep(backoffMillis);
				} else {
					throw ex;
				}
			}
		}
		if (deregisterStreamConsumerResponse == null) {
			throw new RuntimeException("Retries exceeded for deregisterStream operation - all " + fanOutRecordPublisherConfiguration.getDeregisterStreamMaxRetries() +
				" retry attempts failed.");
		}
	}

	/**
	 * Return a random jitter between 0 and the exponential backoff.
	 */
	protected static long fullJitterBackoff(long base, long max, double power, int attempt) {
		long exponentialBackoff = (long) Math.min(max, base * Math.pow(power, attempt));
		return (long) (seed.nextDouble() * exponentialBackoff);
	}
}
