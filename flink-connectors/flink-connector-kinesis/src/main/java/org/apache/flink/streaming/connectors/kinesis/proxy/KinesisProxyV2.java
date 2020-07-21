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
import org.apache.flink.streaming.connectors.kinesis.internals.fanout.FanOutProperties;
import org.apache.flink.streaming.connectors.kinesis.internals.fanout.FanOutStreamInfo;
import org.apache.flink.streaming.connectors.kinesis.util.AwsV2Util;
import org.apache.flink.util.Preconditions;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.ClientConfigurationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.DeregisterStreamConsumerRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamConsumerRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamConsumerResponse;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse;
import software.amazon.awssdk.services.kinesis.model.RegisterStreamConsumerRequest;
import software.amazon.awssdk.services.kinesis.model.RegisterStreamConsumerResponse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

	private final FanOutProperties fanOutProperties;

	/**
	 * Create a new KinesisProxyV2 based on the supplied configuration properties.
	 *
	 * @param configProps configuration properties containing AWS credential and AWS region info
	 */
	public KinesisProxyV2(final Properties configProps, List<String> streams) {
		this.kinesisAsyncClient = createKinesisAsyncClient(configProps);
		this.fanOutProperties = new FanOutProperties(configProps, streams);
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

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Map<String, String> describeStream(List<String> streams) throws InterruptedException, ExecutionException {
		Map<String, String> result = new HashMap<>();
		for (String stream : streams) {
			DescribeStreamRequest describeStreamRequest = DescribeStreamRequest
				.builder()
				.streamName(stream)
				.build();
			DescribeStreamResponse describeStreamResponse = null;

			int retryCount = 0;
			while (retryCount <= fanOutProperties.getDescribeStreamMaxRetries() && describeStreamResponse == null) {
				try {
					describeStreamResponse = kinesisAsyncClient.describeStream(describeStreamRequest).get();
				} catch (ExecutionException ex) {
					if (AwsV2Util.isRecoverableException(ex)) {
						long backoffMillis = fullJitterBackoff(
							fanOutProperties.getDescribeStreamBaseBackoffMillis(), fanOutProperties.getDescribeStreamMaxBackoffMillis(), fanOutProperties.getDescribeStreamExpConstant(), retryCount++);
						LOG.warn("Got recoverable AmazonServiceException when trying to describe stream " + stream + ". Backing off for "
							+ backoffMillis + " millis (" + ex.getClass().getName() + ": " + ex.getMessage() + ")");
						Thread.sleep(backoffMillis);
					} else {
						throw ex;
					}
				}
			}
			if (describeStreamResponse == null) {
				throw new RuntimeException("Retries exceeded for describeStream operation - all " + fanOutProperties.getDescribeStreamMaxRetries() +
					" retry attempts failed.");
			}
			result.put(stream, describeStreamResponse.streamDescription().streamARN());
		}
		return result;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public List<FanOutStreamInfo> registerStreamConsumer(Map<String, String> streamArns) throws InterruptedException, ExecutionException {
		Preconditions.checkArgument(fanOutProperties.getConsumerName().isPresent());
		String consumerName = fanOutProperties.getConsumerName().get();
		List<FanOutStreamInfo> result = new ArrayList<>();
		for (Map.Entry<String, String> entry : streamArns.entrySet()) {
			String stream = entry.getKey();
			String streamArn = entry.getValue();
			RegisterStreamConsumerRequest registerStreamConsumerRequest = RegisterStreamConsumerRequest
				.builder()
				.consumerName(consumerName)
				.streamARN(streamArn)
				.build();
			FanOutStreamInfo fanOutStreamInfo = null;
			int retryCount = 0;
			while (retryCount <= fanOutProperties.getRegisterStreamMaxRetries() && fanOutStreamInfo == null) {
				try {
					RegisterStreamConsumerResponse registerStreamConsumerResponse = kinesisAsyncClient.registerStreamConsumer(registerStreamConsumerRequest).get();
					fanOutStreamInfo = new FanOutStreamInfo(stream, streamArn, consumerName, registerStreamConsumerResponse.consumer().consumerARN());
				} catch (ExecutionException ex) {
					if (AwsV2Util.isResourceInUse(ex)) {
						fanOutStreamInfo = describeStreamConsumer(stream, streamArn, consumerName);
					} else if (AwsV2Util.isRecoverableException(ex)) {
						long backoffMillis = fullJitterBackoff(
							fanOutProperties.getRegisterStreamBaseBackoffMillis(), fanOutProperties.getRegisterStreamMaxBackoffMillis(), fanOutProperties.getRegisterStreamExpConstant(), retryCount++);
						LOG.warn("Got recoverable AmazonServiceException when trying to register " + stream + ". Backing off for "
							+ backoffMillis + " millis (" + ex.getClass().getName() + ": " + ex.getMessage() + ")");
						Thread.sleep(backoffMillis);
					} else {
						throw ex;
					}
				}
			}

			if (fanOutStreamInfo == null) {
				throw new RuntimeException("Retries exceeded for registerStream operation - all " + fanOutProperties.getRegisterStreamMaxRetries() +
					" retry attempts failed.");
			}
			result.add(fanOutStreamInfo);
		}
		return result;
	}

	public FanOutStreamInfo describeStreamConsumer(String stream, String streamArn, String consumerName) throws InterruptedException, ExecutionException  {
		DescribeStreamConsumerRequest describeStreamConsumerRequest = DescribeStreamConsumerRequest
			.builder()
			.streamARN(streamArn)
			.consumerName(consumerName)
			.build();
		FanOutStreamInfo fanOutStreamInfo = null;
		int retryCount = 0;
		while (retryCount <= fanOutProperties.getDescribeStreamConsumerMaxRetries() && fanOutStreamInfo == null) {
			try {
				DescribeStreamConsumerResponse describeStreamConsumerResponse = kinesisAsyncClient.describeStreamConsumer(describeStreamConsumerRequest).get();
				fanOutStreamInfo = new FanOutStreamInfo(stream, streamArn, consumerName, describeStreamConsumerResponse.consumerDescription().consumerARN());
			} catch (ExecutionException ex) {
				if (AwsV2Util.isRecoverableException(ex)) {
					long backoffMillis = fullJitterBackoff(
						fanOutProperties.getDescribeStreamConsumerBaseBackoffMillis(), fanOutProperties.getDescribeStreamConsumerMaxBackoffMillis(), fanOutProperties.getDescribeStreamConsumerExpConstant(), retryCount++);
					LOG.warn("Got recoverable AmazonServiceException when trying to describe stream consumer " + stream + ". Backing off for "
						+ backoffMillis + " millis (" + ex.getClass().getName() + ": " + ex.getMessage() + ")");
					Thread.sleep(backoffMillis);
				} else {
					throw ex;
				}
			}
		}

		if (fanOutStreamInfo == null) {
			throw new RuntimeException("Retries exceeded for registerStream operation - all " + fanOutProperties.getRegisterStreamMaxRetries() +
				" retry attempts failed.");
		}
		return fanOutStreamInfo;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void deregisterStreamConsumer(List<FanOutStreamInfo> fanOutStreamInfos) throws InterruptedException, ExecutionException {
		for (FanOutStreamInfo fanOutStreamInfo : fanOutStreamInfos) {
			DeregisterStreamConsumerRequest deregisterStreamConsumerRequest = DeregisterStreamConsumerRequest
				.builder()
				.consumerARN(fanOutStreamInfo.getConsumerArn())
				.consumerName(fanOutStreamInfo.getConsumerName())
				.build();

			int retryCount = 0;
			boolean deregisterSuccessFlag = false;
			while (retryCount <= fanOutProperties.getDeregisterStreamMaxRetries() && !deregisterSuccessFlag) {
				try {
					kinesisAsyncClient.deregisterStreamConsumer(deregisterStreamConsumerRequest).get();
					deregisterSuccessFlag = true;
				} catch (ExecutionException ex) {
					if (AwsV2Util.isResourceNotFound(ex)) {
						deregisterSuccessFlag = true;
						break;
					}
					if (AwsV2Util.isRecoverableException(ex)) {
						long backoffMillis = fullJitterBackoff(
							fanOutProperties.getDeregisterStreamBaseBackoffMillis(), fanOutProperties.getDeregisterStreamMaxBackoffMillis(), fanOutProperties.getDeregisterStreamExpConstant(), retryCount++);
						LOG.warn("Got recoverable AmazonServiceException when trying to deregister " + fanOutStreamInfo.getStream() + ". Backing off for "
							+ backoffMillis + " millis (" + ex.getClass().getName() + ": " + ex.getMessage() + ")");
						Thread.sleep(backoffMillis);
					} else {
						throw ex;
					}
				}
			}
			if (!deregisterSuccessFlag) {
				throw new RuntimeException("Retries exceeded for deregisterStream operation - all " + fanOutProperties.getDeregisterStreamMaxRetries() +
					" retry attempts failed.");
			}
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
