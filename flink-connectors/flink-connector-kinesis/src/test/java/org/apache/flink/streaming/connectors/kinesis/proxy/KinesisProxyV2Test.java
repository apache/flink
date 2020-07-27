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

import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.internals.publisher.fanout.FanOutStreamConsumerInfo;
import org.apache.flink.streaming.connectors.kinesis.util.AwsV2Util;

import org.apache.flink.shaded.curator4.org.apache.curator.shaded.com.google.common.collect.Lists;

import org.apache.commons.lang3.mutable.MutableInt;
import org.hamcrest.CoreMatchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.Consumer;
import software.amazon.awssdk.services.kinesis.model.ConsumerDescription;
import software.amazon.awssdk.services.kinesis.model.DeregisterStreamConsumerRequest;
import software.amazon.awssdk.services.kinesis.model.DeregisterStreamConsumerResponse;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamConsumerRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamConsumerResponse;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse;
import software.amazon.awssdk.services.kinesis.model.LimitExceededException;
import software.amazon.awssdk.services.kinesis.model.ProvisionedThroughputExceededException;
import software.amazon.awssdk.services.kinesis.model.RegisterStreamConsumerRequest;
import software.amazon.awssdk.services.kinesis.model.RegisterStreamConsumerResponse;
import software.amazon.awssdk.services.kinesis.model.ResourceInUseException;
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;
import software.amazon.awssdk.services.kinesis.model.StreamDescription;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;

/**
 * Test for methods in the {@link KinesisProxyV2} class.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(KinesisProxyV2.class)
public class KinesisProxyV2Test {
	@Rule
	private ExpectedException exception = ExpectedException.none();

	@Test
	public void testIsRecoverableException() {
		Exception notAwsEx = new IllegalArgumentException("abc");
		Exception awsNotRecoverableEx = ResourceNotFoundException.builder().cause(notAwsEx).build();
		Exception awsRecoverableEx = LimitExceededException.builder().cause(notAwsEx).build();
		assertFalse(AwsV2Util.isRecoverableException(new ExecutionException(notAwsEx)));
		assertFalse(AwsV2Util.isRecoverableException(new ExecutionException(awsNotRecoverableEx)));
		assertTrue(AwsV2Util.isRecoverableException(new ExecutionException(awsRecoverableEx)));
	}

	@Test
	public void testDescribeStreamNormal() throws Exception {
		final String fakedStream1 = "fakedstream1";
		final String fakedStreamArn = "fakedstreamarn";

		Properties kinesisConsumerConfig = getProperties();

		KinesisAsyncClient mockClient = mock(KinesisAsyncClient.class);
		DescribeStreamResponse expectedResponse = DescribeStreamResponse
			.builder()
			.streamDescription(
				StreamDescription
					.builder()
					.streamARN(fakedStreamArn)
					.build()
			)
			.build();
		Mockito
			.when(mockClient.describeStream((DescribeStreamRequest) any()))
			.thenAnswer((Answer<CompletableFuture<DescribeStreamResponse>>) invocationOnMock -> CompletableFuture.completedFuture(expectedResponse));

		List<String> streams = Lists.newArrayList(fakedStream1);
		KinesisProxyV2 kinesisProxy = new KinesisProxyV2(kinesisConsumerConfig, streams);
		Whitebox.getField(KinesisProxyV2.class, "kinesisAsyncClient").set(kinesisProxy, mockClient);

		DescribeStreamResponse response = kinesisProxy.describeStream(fakedStream1);
		String resultStreamArn = response.streamDescription().streamARN();
		assertEquals(resultStreamArn, fakedStreamArn);
	}

	@Test
	public void testDescribeStreamWithUnrecoverableException() throws Exception {
		exception.expect(ExecutionException.class);
		exception.expectCause(CoreMatchers.isA(ResourceNotFoundException.class));

		final String fakedStream1 = "fakedstream1";
		final String fakedStream2 = "fakedstream2";

		Properties kinesisConsumerConfig = getProperties();
		KinesisAsyncClient mockClient = mock(KinesisAsyncClient.class);
		Mockito
			.when(mockClient.describeStream((DescribeStreamRequest) any()))
			.thenAnswer((Answer<CompletableFuture<DescribeStreamResponse>>) invocationOnMock ->
				CompletableFuture.failedFuture(ResourceNotFoundException.builder().build())
			);

		List<String> streams = Lists.newArrayList(fakedStream1, fakedStream2);
		KinesisProxyV2 kinesisProxy = new KinesisProxyV2(kinesisConsumerConfig, streams);
		Whitebox.getField(KinesisProxyV2.class, "kinesisAsyncClient").set(kinesisProxy, mockClient);

		kinesisProxy.describeStream(fakedStream1);
	}

	@Test
	public void testDescribeStreamWithRecoverableException() throws Exception {
		final String fakedStream1 = "fakedstream1";
		final String fakedStream2 = "fakedstream2";
		final String fakedStreamArn = "fakedstreamarn";

		Properties kinesisConsumerConfig = getProperties();

		KinesisAsyncClient mockClient = mock(KinesisAsyncClient.class);
		DescribeStreamResponse expectedResponse = DescribeStreamResponse
			.builder()
			.streamDescription(
				StreamDescription
					.builder()
					.streamARN(fakedStreamArn)
					.build()
			)
			.build();
		MutableInt retries = new MutableInt();
		final Throwable[] retriableExceptions = new Throwable[]{
			LimitExceededException.builder().build(),
			ProvisionedThroughputExceededException.builder().build()
		};
		Mockito
			.when(mockClient.describeStream((DescribeStreamRequest) any()))
			.thenAnswer((Answer<CompletableFuture<DescribeStreamResponse>>) invocationOnMock -> {
					if (retries.intValue() < retriableExceptions.length) {
						retries.increment();
						return CompletableFuture.failedFuture(retriableExceptions[retries.intValue() - 1]);
					}
					return CompletableFuture.completedFuture(expectedResponse);
				}
			);

		List<String> streams = Lists.newArrayList(fakedStream1, fakedStream2);
		KinesisProxyV2 kinesisProxy = new KinesisProxyV2(kinesisConsumerConfig, streams);
		Whitebox.getField(KinesisProxyV2.class, "kinesisAsyncClient").set(kinesisProxy, mockClient);

		DescribeStreamResponse response = kinesisProxy.describeStream(fakedStream1);
		String resultStreamArn = response.streamDescription().streamARN();
		assertEquals(resultStreamArn, fakedStreamArn);
	}

	@Test
	public void testDescribeStreamWithRecoverableExceptionExceedsMaxRetry() throws Exception {
		exception.expect(RuntimeException.class);
		exception.expectMessage(CoreMatchers.containsString("Retries exceeded for describeStream operation - all 2 retry attempts failed."));

		final String fakedStream1 = "fakedstream1";
		final String fakedStream2 = "fakedstream2";
		final String fakedStreamArn = "fakedstreamarn";

		Properties kinesisConsumerConfig = getProperties();
		kinesisConsumerConfig.setProperty(ConsumerConfigConstants.STREAM_DESCRIBE_RETRIES, "2");

		KinesisAsyncClient mockClient = mock(KinesisAsyncClient.class);
		DescribeStreamResponse expectedResponse = DescribeStreamResponse
			.builder()
			.streamDescription(
				StreamDescription
					.builder()
					.streamARN(fakedStreamArn)
					.build()
			)
			.build();
		MutableInt retries = new MutableInt();
		final Throwable[] retriableExceptions = new Throwable[]{
			LimitExceededException.builder().build(),
			ProvisionedThroughputExceededException.builder().build(),
			ProvisionedThroughputExceededException.builder().build(),
			ProvisionedThroughputExceededException.builder().build()
		};
		Mockito
			.when(mockClient.describeStream((DescribeStreamRequest) any()))
			.thenAnswer((Answer<CompletableFuture<DescribeStreamResponse>>) invocationOnMock -> {
					if (retries.intValue() < retriableExceptions.length) {
						retries.increment();
						return CompletableFuture.failedFuture(retriableExceptions[retries.intValue() - 1]);
					}
					return CompletableFuture.completedFuture(expectedResponse);
				}
			);

		List<String> streams = Lists.newArrayList(fakedStream1, fakedStream2);
		KinesisProxyV2 kinesisProxy = new KinesisProxyV2(kinesisConsumerConfig, streams);
		Whitebox.getField(KinesisProxyV2.class, "kinesisAsyncClient").set(kinesisProxy, mockClient);

		kinesisProxy.describeStream(fakedStream1);

	}

	@Test
	public void testRegisterStreamConsumerWhenConsumerIsNotRegistered() throws Exception {
		final String fakedStream1 = "fakedstream1";
		final String fakedStream2 = "fakedstream2";
		final String fakedStreamArn = "fakedstreamarn";
		final String fakedConsumerArn = "fakedconsumerarn";

		Properties kinesisConsumerConfig = getProperties();
		final String consumerName = kinesisConsumerConfig.getProperty(ConsumerConfigConstants.EFO_CONSUMER_NAME);
		KinesisAsyncClient mockClient = mock(KinesisAsyncClient.class);

		RegisterStreamConsumerResponse expectedResponse = RegisterStreamConsumerResponse
			.builder()
			.consumer(
				Consumer
					.builder()
					.consumerARN(fakedConsumerArn)
					.consumerName(consumerName)
					.build()
			)
			.build();
		Mockito
			.when(mockClient.describeStreamConsumer((DescribeStreamConsumerRequest) any()))
			.thenAnswer((Answer<CompletableFuture<DescribeStreamConsumerResponse>>) invocationOnMock -> CompletableFuture.failedFuture(ResourceNotFoundException.builder().build()));
		Mockito
			.when(mockClient.registerStreamConsumer((RegisterStreamConsumerRequest) any()))
			.thenAnswer((Answer<CompletableFuture<RegisterStreamConsumerResponse>>) invocationOnMock -> CompletableFuture.completedFuture(expectedResponse));

		List<String> streams = Lists.newArrayList(fakedStream1, fakedStream2);
		KinesisProxyV2 kinesisProxy = new KinesisProxyV2(kinesisConsumerConfig, streams);
		Whitebox.getField(KinesisProxyV2.class, "kinesisAsyncClient").set(kinesisProxy, mockClient);
		FanOutStreamConsumerInfo result = kinesisProxy.registerStreamConsumer(fakedStream1, fakedStreamArn);
		FanOutStreamConsumerInfo expectedResult = new FanOutStreamConsumerInfo(fakedStream1, fakedStreamArn, consumerName, fakedConsumerArn);
		assert isFanOutStreamConsumerInfoEqual(result, expectedResult);
	}

	@Test
	public void testRegisterStreamConsumerWhenConsumerIsAlreadyRegistered() throws Exception {
		final String fakedStream1 = "fakedstream1";
		final String fakedStreamArn = "fakedstreamarn";
		final String fakedConsumerArn = "fakedconsumerarn";

		Properties kinesisConsumerConfig = getProperties();
		final String consumerName = kinesisConsumerConfig.getProperty(ConsumerConfigConstants.EFO_CONSUMER_NAME);
		KinesisAsyncClient mockClient = mock(KinesisAsyncClient.class);
		DescribeStreamConsumerResponse expectedResponse = DescribeStreamConsumerResponse
			.builder()
			.consumerDescription(
				ConsumerDescription
					.builder()
					.consumerName(consumerName)
					.consumerARN(fakedConsumerArn)
					.streamARN(fakedStreamArn)
					.build()
			)
			.build();
		Mockito
			.when(mockClient.describeStreamConsumer((DescribeStreamConsumerRequest) any()))
			.thenAnswer((Answer<CompletableFuture<DescribeStreamConsumerResponse>>) invocationOnMock -> CompletableFuture.completedFuture(expectedResponse));
		Mockito
			.when(mockClient.registerStreamConsumer((RegisterStreamConsumerRequest) any()))
			.thenAnswer((Answer<CompletableFuture<RegisterStreamConsumerResponse>>) invocationOnMock -> CompletableFuture.failedFuture(ResourceInUseException.builder().build()));

		List<String> streams = Lists.newArrayList(fakedStream1);
		KinesisProxyV2 kinesisProxy = new KinesisProxyV2(kinesisConsumerConfig, streams);
		Whitebox.getField(KinesisProxyV2.class, "kinesisAsyncClient").set(kinesisProxy, mockClient);
		FanOutStreamConsumerInfo result = kinesisProxy.registerStreamConsumer(fakedStream1, fakedStreamArn);
		FanOutStreamConsumerInfo expectedResult = new FanOutStreamConsumerInfo(fakedStream1, fakedStreamArn, consumerName, fakedConsumerArn);
		assert isFanOutStreamConsumerInfoEqual(result, expectedResult);
	}

	@Test
	public void testRegisterStreamConsumerWhenRegisterStreamConsumerMeetsUnrecoverableException() throws Exception {
		exception.expect(ExecutionException.class);
		exception.expectCause(CoreMatchers.isA(IllegalArgumentException.class));

		final String fakedStream1 = "fakedstream1";
		final String fakedStreamArn = "fakedstreamarn";

		Properties kinesisConsumerConfig = getProperties();
		KinesisAsyncClient mockClient = mock(KinesisAsyncClient.class);
		Mockito
			.when(mockClient.describeStreamConsumer((DescribeStreamConsumerRequest) any()))
			.thenAnswer((Answer<CompletableFuture<DescribeStreamConsumerResponse>>) invocationOnMock -> CompletableFuture.failedFuture(ResourceNotFoundException.builder().build()));
		Mockito
			.when(mockClient.registerStreamConsumer((RegisterStreamConsumerRequest) any()))
			.thenAnswer((Answer<CompletableFuture<RegisterStreamConsumerResponse>>) invocationOnMock -> CompletableFuture.failedFuture(new IllegalArgumentException()));

		List<String> streams = Lists.newArrayList(fakedStream1);
		KinesisProxyV2 kinesisProxy = new KinesisProxyV2(kinesisConsumerConfig, streams);
		Whitebox.getField(KinesisProxyV2.class, "kinesisAsyncClient").set(kinesisProxy, mockClient);
		kinesisProxy.registerStreamConsumer(fakedStream1, fakedStreamArn);
	}

	@Test
	public void testRegisterStreamConsumerWhenDescribeStreamConsumerMeetsUnrecoverableException() throws Exception {
		exception.expect(ExecutionException.class);
		exception.expectCause(CoreMatchers.isA(IllegalArgumentException.class));

		final String fakedStream1 = "fakedstream1";
		final String fakedStreamArn = "fakedstreamarn";

		Properties kinesisConsumerConfig = getProperties();
		KinesisAsyncClient mockClient = mock(KinesisAsyncClient.class);
		Mockito
			.when(mockClient.describeStreamConsumer((DescribeStreamConsumerRequest) any()))
			.thenAnswer((Answer<CompletableFuture<DescribeStreamConsumerResponse>>) invocationOnMock -> CompletableFuture.failedFuture(new IllegalArgumentException()));
		Mockito
			.when(mockClient.registerStreamConsumer((RegisterStreamConsumerRequest) any()))
			.thenAnswer((Answer<CompletableFuture<RegisterStreamConsumerResponse>>) invocationOnMock -> CompletableFuture.failedFuture(ResourceNotFoundException.builder().build()));

		List<String> streams = Lists.newArrayList(fakedStream1);
		KinesisProxyV2 kinesisProxy = new KinesisProxyV2(kinesisConsumerConfig, streams);
		Whitebox.getField(KinesisProxyV2.class, "kinesisAsyncClient").set(kinesisProxy, mockClient);
		kinesisProxy.registerStreamConsumer(fakedStream1, fakedStreamArn);
	}

	@Test
	public void testRegisterStreamConsumerWhenDescribeStreamConsumerMeetsRecoverableException() throws Exception {
		final String fakedStream1 = "fakedstream1";
		final String fakedStreamArn = "fakedstreamarn";
		final String fakedConsumerArn = "fakedconsumerarn";

		Properties kinesisConsumerConfig = getProperties();
		final String consumerName = kinesisConsumerConfig.getProperty(ConsumerConfigConstants.EFO_CONSUMER_NAME);
		KinesisAsyncClient mockClient = mock(KinesisAsyncClient.class);

		MutableInt retries = new MutableInt();
		final Throwable[] retriableExceptions = new Throwable[]{
			LimitExceededException.builder().build(),
			LimitExceededException.builder().build()
		};

		DescribeStreamConsumerResponse expectedResponse = DescribeStreamConsumerResponse
			.builder()
			.consumerDescription(
				ConsumerDescription
					.builder()
					.consumerARN(fakedConsumerArn)
					.consumerName(consumerName)
					.build()
			)
			.build();
		Mockito
			.when(mockClient.registerStreamConsumer((RegisterStreamConsumerRequest) any()))
			.thenAnswer((Answer<CompletableFuture<RegisterStreamConsumerResponse>>) invocationOnMock -> CompletableFuture.failedFuture(ResourceInUseException.builder().build())
			);
		Mockito
			.when(mockClient.describeStreamConsumer((DescribeStreamConsumerRequest) any()))
			.thenAnswer((Answer<CompletableFuture<DescribeStreamConsumerResponse>>) invocationOnMock -> {
					if (retries.intValue() < retriableExceptions.length) {
						retries.increment();
						return CompletableFuture.failedFuture(retriableExceptions[retries.intValue() - 1]);
					}
					return CompletableFuture.completedFuture(expectedResponse);
				}
			);

		List<String> streams = Lists.newArrayList(fakedStream1);
		KinesisProxyV2 kinesisProxy = new KinesisProxyV2(kinesisConsumerConfig, streams);
		Whitebox.getField(KinesisProxyV2.class, "kinesisAsyncClient").set(kinesisProxy, mockClient);
		FanOutStreamConsumerInfo result = kinesisProxy.registerStreamConsumer(fakedStream1, fakedStreamArn);
		FanOutStreamConsumerInfo expectedResult = new FanOutStreamConsumerInfo(fakedStream1, fakedStreamArn, consumerName, fakedConsumerArn);
		assert isFanOutStreamConsumerInfoEqual(result, expectedResult);
	}

	@Test
	public void testRegisterStreamConsumerWhenDescribeStreamConsumerMeetsRecoverableExceptionExceedsMaxRetries() throws Exception {
		exception.expect(RuntimeException.class);
		exception.expectMessage(CoreMatchers.containsString("Retries exceeded for describeStreamConsumer operation - all 2 retry attempts failed."));

		final String fakedStream1 = "fakedstream1";
		final String fakedStreamArn = "fakedstreamarn";
		final String fakedConsumerArn = "fakedconsumerarn";

		Properties kinesisConsumerConfig = getProperties();
		kinesisConsumerConfig.setProperty(ConsumerConfigConstants.DESCRIBE_STREAM_CONSUMER_RETRIES, "2");
		final String consumerName = kinesisConsumerConfig.getProperty(ConsumerConfigConstants.EFO_CONSUMER_NAME);
		KinesisAsyncClient mockClient = mock(KinesisAsyncClient.class);

		MutableInt retries = new MutableInt();
		final Throwable[] retriableExceptions = new Throwable[]{
			LimitExceededException.builder().build(),
			LimitExceededException.builder().build(),
			LimitExceededException.builder().build()
		};

		DescribeStreamConsumerResponse expectedResponse = DescribeStreamConsumerResponse
			.builder()
			.consumerDescription(
				ConsumerDescription
					.builder()
					.consumerARN(fakedConsumerArn)
					.consumerName(consumerName)
					.build()
			)
			.build();
		Mockito
			.when(mockClient.registerStreamConsumer((RegisterStreamConsumerRequest) any()))
			.thenAnswer((Answer<CompletableFuture<RegisterStreamConsumerResponse>>) invocationOnMock -> CompletableFuture.failedFuture(ResourceInUseException.builder().build())
			);
		Mockito
			.when(mockClient.describeStreamConsumer((DescribeStreamConsumerRequest) any()))
			.thenAnswer((Answer<CompletableFuture<DescribeStreamConsumerResponse>>) invocationOnMock -> {
					if (retries.intValue() < retriableExceptions.length) {
						retries.increment();
						return CompletableFuture.failedFuture(retriableExceptions[retries.intValue() - 1]);
					}
					return CompletableFuture.completedFuture(expectedResponse);
				}
			);

		List<String> streams = Lists.newArrayList(fakedStream1);
		KinesisProxyV2 kinesisProxy = new KinesisProxyV2(kinesisConsumerConfig, streams);
		Whitebox.getField(KinesisProxyV2.class, "kinesisAsyncClient").set(kinesisProxy, mockClient);
		kinesisProxy.registerStreamConsumer(fakedStream1, fakedStreamArn);
	}

	@Test
	public void testRegisterStreamConsumerWhenRegisterStreamConsumerMeetsRecoverableExceptionExceedsMaxRetries() throws Exception {
		exception.expect(RuntimeException.class);
		exception.expectMessage(CoreMatchers.containsString("Retries exceeded for registerStream operation - all 2 retry attempts failed."));

		final String fakedStream1 = "fakedstream1";
		final String fakedStreamArn = "fakedstreamarn";
		final String fakedConsumerArn = "fakedconsumerarn";

		Properties kinesisConsumerConfig = getProperties();
		kinesisConsumerConfig.setProperty(ConsumerConfigConstants.REGISTER_STREAM_RETRIES, "2");
		final String consumerName = kinesisConsumerConfig.getProperty(ConsumerConfigConstants.EFO_CONSUMER_NAME);
		KinesisAsyncClient mockClient = mock(KinesisAsyncClient.class);

		MutableInt retries = new MutableInt();
		final Throwable[] retriableExceptions = new Throwable[]{
			LimitExceededException.builder().build(),
			LimitExceededException.builder().build(),
			LimitExceededException.builder().build()
		};

		RegisterStreamConsumerResponse expectedResponse = RegisterStreamConsumerResponse
			.builder()
			.consumer(
				Consumer
					.builder()
					.consumerARN(fakedConsumerArn)
					.consumerName(consumerName)
					.build()
			)
			.build();
		Mockito
			.when(mockClient.describeStreamConsumer((DescribeStreamConsumerRequest) any()))
			.thenAnswer((Answer<CompletableFuture<DescribeStreamConsumerResponse>>) invocationOnMock -> CompletableFuture.failedFuture(ResourceNotFoundException.builder().build()));
		Mockito
			.when(mockClient.registerStreamConsumer((RegisterStreamConsumerRequest) any()))
			.thenAnswer((Answer<CompletableFuture<RegisterStreamConsumerResponse>>) invocationOnMock -> {
					if (retries.intValue() < retriableExceptions.length) {
						retries.increment();
						return CompletableFuture.failedFuture(retriableExceptions[retries.intValue() - 1]);
					}
					return CompletableFuture.completedFuture(expectedResponse);
				}
			);

		List<String> streams = Lists.newArrayList(fakedStream1);
		KinesisProxyV2 kinesisProxy = new KinesisProxyV2(kinesisConsumerConfig, streams);
		Whitebox.getField(KinesisProxyV2.class, "kinesisAsyncClient").set(kinesisProxy, mockClient);
		kinesisProxy.registerStreamConsumer(fakedStream1, fakedStreamArn);
	}

	/**
	 * Describe stream consumer - fails with one recoverable exception then fails with ResourceNotFoundException.
	 * Register stream consumer - fails with two recoverable exception then fails with ResourceInUseException.
	 * Describe stream consumer - first fail two recoverable exceptions then succeed.
	 * Expects the register stream operation succeed.
	 */
	@Test
	public void testRegisterStreamConsumerWithComplicatedSituation() throws Exception {
		final String fakedStream1 = "fakedstream1";
		final String fakedStreamArn = "fakedstreamarn";
		final String fakedConsumerArn = "fakedconsumerarn";

		Properties kinesisConsumerConfig = getProperties();
		final String consumerName = kinesisConsumerConfig.getProperty(ConsumerConfigConstants.EFO_CONSUMER_NAME);
		KinesisAsyncClient mockClient = mock(KinesisAsyncClient.class);

		MutableInt describeStreamConsumerRetries = new MutableInt();
		final Throwable[] describeStreamConsumerRetriableExceptions = new Throwable[]{
			LimitExceededException.builder().build(),
			LimitExceededException.builder().build(),
			ResourceNotFoundException.builder().build(),
			LimitExceededException.builder().build(),
			LimitExceededException.builder().build()
		};

		MutableInt registerStreamConsumerRetries = new MutableInt();
		final Throwable[] registerStreamConsumerRetriableExceptions = new Throwable[]{
			LimitExceededException.builder().build(),
			LimitExceededException.builder().build(),
			ResourceInUseException.builder().build()
		};

		DescribeStreamConsumerResponse expectedResponse = DescribeStreamConsumerResponse
			.builder()
			.consumerDescription(
				ConsumerDescription
					.builder()
					.consumerARN(fakedConsumerArn)
					.consumerName(consumerName)
					.build()
			)
			.build();
		Mockito
			.when(mockClient.registerStreamConsumer((RegisterStreamConsumerRequest) any()))
			.thenAnswer((Answer<CompletableFuture<RegisterStreamConsumerResponse>>) invocationOnMock -> {
				registerStreamConsumerRetries.increment();
				return CompletableFuture.failedFuture(registerStreamConsumerRetriableExceptions[registerStreamConsumerRetries.intValue() - 1]);
				}
			);
		Mockito
			.when(mockClient.describeStreamConsumer((DescribeStreamConsumerRequest) any()))
			.thenAnswer((Answer<CompletableFuture<DescribeStreamConsumerResponse>>) invocationOnMock -> {
					if (describeStreamConsumerRetries.intValue() < describeStreamConsumerRetriableExceptions.length) {
						describeStreamConsumerRetries.increment();
						return CompletableFuture.failedFuture(describeStreamConsumerRetriableExceptions[describeStreamConsumerRetries.intValue() - 1]);
					}
					return CompletableFuture.completedFuture(expectedResponse);
				}
			);

		List<String> streams = Lists.newArrayList(fakedStream1);
		KinesisProxyV2 kinesisProxy = new KinesisProxyV2(kinesisConsumerConfig, streams);
		Whitebox.getField(KinesisProxyV2.class, "kinesisAsyncClient").set(kinesisProxy, mockClient);
		FanOutStreamConsumerInfo result = kinesisProxy.registerStreamConsumer(fakedStream1, fakedStreamArn);
		FanOutStreamConsumerInfo expectedResult = new FanOutStreamConsumerInfo(fakedStream1, fakedStreamArn, consumerName, fakedConsumerArn);
		assert isFanOutStreamConsumerInfoEqual(result, expectedResult);
	}

	@Test
	public void testDeregisterStreamConsumerNormal() throws Exception {
		final String fakedStream1 = "fakedstream1";
		final String fakedStreamArn = "fakedstreamarn";
		final String fakedConsumerArn = "fakedconsumerarn";
		Properties kinesisConsumerConfig = getProperties();
		final String consumerName = kinesisConsumerConfig.getProperty(ConsumerConfigConstants.EFO_CONSUMER_NAME);
		KinesisAsyncClient mockClient = mock(KinesisAsyncClient.class);
		DeregisterStreamConsumerResponse expectedResponse = DeregisterStreamConsumerResponse.builder().build();
		Mockito
			.when(mockClient.deregisterStreamConsumer((DeregisterStreamConsumerRequest) any()))
			.thenAnswer((Answer<CompletableFuture<DeregisterStreamConsumerResponse>>) invocationOnMock -> CompletableFuture.completedFuture(expectedResponse));
		List<String> streams = Lists.newArrayList(fakedStream1);
		KinesisProxyV2 kinesisProxy = new KinesisProxyV2(kinesisConsumerConfig, streams);

		Whitebox.getField(KinesisProxyV2.class, "kinesisAsyncClient").set(kinesisProxy, mockClient);
		FanOutStreamConsumerInfo deregisterStream =
			new FanOutStreamConsumerInfo(fakedStream1, fakedStreamArn, consumerName, fakedConsumerArn);
		kinesisProxy.deregisterStreamConsumer(deregisterStream);
	}

	@Test
	public void testDeregisterStreamConsumerWithUnrecoverableException() throws Exception {
		exception.expect(ExecutionException.class);
		exception.expectCause(CoreMatchers.isA(IllegalArgumentException.class));

		final String fakedStream1 = "fakedstream1";
		final String fakedStream2 = "fakedstream2";
		final String fakedStreamArn = "fakedstreamarn";
		final String fakedConsumerArn = "fakedconsumerarn";
		Properties kinesisConsumerConfig = getProperties();
		final String consumerName = kinesisConsumerConfig.getProperty(ConsumerConfigConstants.EFO_CONSUMER_NAME);
		KinesisAsyncClient mockClient = mock(KinesisAsyncClient.class);

		Mockito
			.when(mockClient.deregisterStreamConsumer((DeregisterStreamConsumerRequest) any()))
			.thenAnswer((Answer<CompletableFuture<DeregisterStreamConsumerResponse>>) invocationOnMock -> CompletableFuture.failedFuture(new IllegalArgumentException()));
		List<String> streams = Lists.newArrayList(fakedStream1, fakedStream2);
		KinesisProxyV2 kinesisProxy = new KinesisProxyV2(kinesisConsumerConfig, streams);

		Whitebox.getField(KinesisProxyV2.class, "kinesisAsyncClient").set(kinesisProxy, mockClient);
		FanOutStreamConsumerInfo deregisterStream =
			new FanOutStreamConsumerInfo(fakedStream1, fakedStreamArn, consumerName, fakedConsumerArn);
		kinesisProxy.deregisterStreamConsumer(deregisterStream);
	}

	@Test
	public void testDeregisterStreamConsumerWithResourceNotFoundException() throws Exception {
		final String fakedStream1 = "fakedstream1";
		final String fakedStreamArn = "fakedstreamarn";
		final String fakedConsumerArn = "fakedconsumerarn";
		Properties kinesisConsumerConfig = getProperties();
		final String consumerName = kinesisConsumerConfig.getProperty(ConsumerConfigConstants.EFO_CONSUMER_NAME);
		KinesisAsyncClient mockClient = mock(KinesisAsyncClient.class);

		Mockito
			.when(mockClient.deregisterStreamConsumer((DeregisterStreamConsumerRequest) any()))
			.thenAnswer((Answer<CompletableFuture<DeregisterStreamConsumerResponse>>) invocationOnMock -> CompletableFuture.failedFuture(ResourceNotFoundException.builder().build()));
		List<String> streams = Lists.newArrayList(fakedStream1);
		KinesisProxyV2 kinesisProxy = new KinesisProxyV2(kinesisConsumerConfig, streams);

		Whitebox.getField(KinesisProxyV2.class, "kinesisAsyncClient").set(kinesisProxy, mockClient);
		FanOutStreamConsumerInfo deregisterStream =
			new FanOutStreamConsumerInfo(fakedStream1, fakedStreamArn, consumerName, fakedConsumerArn);
		kinesisProxy.deregisterStreamConsumer(deregisterStream);
	}

	@Test
	public void testDeregisterStreamConsumerWithRecoverableException() throws Exception {
		final String fakedStream1 = "fakedstream1";
		final String fakedStreamArn = "fakedstreamarn";
		final String fakedConsumerArn = "fakedconsumerarn";
		Properties kinesisConsumerConfig = getProperties();
		final String consumerName = kinesisConsumerConfig.getProperty(ConsumerConfigConstants.EFO_CONSUMER_NAME);
		KinesisAsyncClient mockClient = mock(KinesisAsyncClient.class);
		DeregisterStreamConsumerResponse expectedResponse = DeregisterStreamConsumerResponse.builder().build();

		MutableInt retries = new MutableInt();
		final Throwable[] retriableExceptions = new Throwable[]{
			LimitExceededException.builder().build(),
			LimitExceededException.builder().build()
		};
		Mockito
			.when(mockClient.deregisterStreamConsumer((DeregisterStreamConsumerRequest) any()))
			.thenAnswer((Answer<CompletableFuture<DeregisterStreamConsumerResponse>>) invocationOnMock -> {
				if (retries.intValue() < retriableExceptions.length) {
					retries.increment();
					return CompletableFuture.failedFuture(retriableExceptions[retries.intValue() - 1]);
				}
				return CompletableFuture.completedFuture(expectedResponse);
			});
		List<String> streams = Lists.newArrayList(fakedStream1);
		KinesisProxyV2 kinesisProxy = new KinesisProxyV2(kinesisConsumerConfig, streams);

		Whitebox.getField(KinesisProxyV2.class, "kinesisAsyncClient").set(kinesisProxy, mockClient);
		FanOutStreamConsumerInfo deregisterStream =
			new FanOutStreamConsumerInfo(fakedStream1, fakedStreamArn, consumerName, fakedConsumerArn);
		kinesisProxy.deregisterStreamConsumer(deregisterStream);
	}

	@Test
	public void testDeregisterStreamConsumerWithRecoverableExceptionExceedsRetry() throws Exception {
		exception.expect(RuntimeException.class);
		exception.expectMessage(CoreMatchers.containsString("Retries exceeded for deregisterStream operation - all 2 retry attempts failed."));

		final String fakedStream1 = "fakedstream1";
		final String fakedStreamArn = "fakedstreamarn";
		final String fakedConsumerArn = "fakedconsumerarn";
		Properties kinesisConsumerConfig = getProperties();
		kinesisConsumerConfig.setProperty(ConsumerConfigConstants.DEREGISTER_STREAM_RETRIES, "2");
		final String consumerName = kinesisConsumerConfig.getProperty(ConsumerConfigConstants.EFO_CONSUMER_NAME);
		KinesisAsyncClient mockClient = mock(KinesisAsyncClient.class);
		DeregisterStreamConsumerResponse expectedResponse = DeregisterStreamConsumerResponse.builder().build();

		MutableInt retries = new MutableInt();
		final Throwable[] retriableExceptions = new Throwable[]{
			LimitExceededException.builder().build(),
			LimitExceededException.builder().build(),
			ProvisionedThroughputExceededException.builder().build()
		};
		Mockito
			.when(mockClient.deregisterStreamConsumer((DeregisterStreamConsumerRequest) any()))
			.thenAnswer((Answer<CompletableFuture<DeregisterStreamConsumerResponse>>) invocationOnMock -> {
				if (retries.intValue() < retriableExceptions.length) {
					retries.increment();
					return CompletableFuture.failedFuture(retriableExceptions[retries.intValue() - 1]);
				}
				return CompletableFuture.completedFuture(expectedResponse);
			});
		List<String> streams = Lists.newArrayList(fakedStream1);
		KinesisProxyV2 kinesisProxy = new KinesisProxyV2(kinesisConsumerConfig, streams);

		Whitebox.getField(KinesisProxyV2.class, "kinesisAsyncClient").set(kinesisProxy, mockClient);
		FanOutStreamConsumerInfo deregisterStream =
			new FanOutStreamConsumerInfo(fakedStream1, fakedStreamArn, consumerName, fakedConsumerArn);
		kinesisProxy.deregisterStreamConsumer(deregisterStream);
	}

	private Properties getProperties() {
		Properties kinesisConsumerConfig = new Properties();
		kinesisConsumerConfig.setProperty(ConsumerConfigConstants.AWS_REGION, "us-east-1");
		kinesisConsumerConfig.setProperty(ConsumerConfigConstants.RECORD_PUBLISHER_TYPE, "EFO");
		kinesisConsumerConfig.setProperty(ConsumerConfigConstants.EFO_CONSUMER_NAME, "fakedConsumer");
		return kinesisConsumerConfig;
	}

	private boolean isFanOutStreamConsumerInfoEqual(FanOutStreamConsumerInfo a, FanOutStreamConsumerInfo b) {
		return a.getConsumerArn().equals(b.getConsumerArn()) &&
			a.getConsumerName().equals(b.getConsumerName()) &&
			a.getStream().equals(b.getStream()) &&
			a.getStreamArn().equals(b.getStreamArn());
	}
}
