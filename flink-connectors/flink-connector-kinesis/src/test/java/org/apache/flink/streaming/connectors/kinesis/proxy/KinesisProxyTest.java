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

import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.model.StreamShardHandle;
import org.apache.flink.streaming.connectors.kinesis.testutils.KinesisShardIdGenerator;
import org.apache.flink.streaming.connectors.kinesis.util.AWSUtil;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonServiceException.ErrorType;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.ClientConfigurationFactory;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.ExpiredIteratorException;
import com.amazonaws.services.kinesis.model.ListShardsRequest;
import com.amazonaws.services.kinesis.model.ListShardsResult;
import com.amazonaws.services.kinesis.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.kinesis.model.Shard;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.junit.Assert;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

/**
 * Test for methods in the {@link KinesisProxy} class.
 */
public class KinesisProxyTest {

	@Test
	public void testIsRecoverableExceptionWithProvisionedThroughputExceeded() {
		final ProvisionedThroughputExceededException ex = new ProvisionedThroughputExceededException("asdf");
		ex.setErrorType(ErrorType.Client);
		assertTrue(KinesisProxy.isRecoverableException(ex));
	}

	@Test
	public void testIsRecoverableExceptionWithServiceException() {
		final AmazonServiceException ex = new AmazonServiceException("asdf");
		ex.setErrorType(ErrorType.Service);
		assertTrue(KinesisProxy.isRecoverableException(ex));
	}

	@Test
	public void testIsRecoverableExceptionWithExpiredIteratorException() {
		final ExpiredIteratorException ex = new ExpiredIteratorException("asdf");
		ex.setErrorType(ErrorType.Client);
		assertFalse(KinesisProxy.isRecoverableException(ex));
	}

	@Test
	public void testIsRecoverableExceptionWithNullErrorType() {
		final AmazonServiceException ex = new AmazonServiceException("asdf");
		ex.setErrorType(null);
		assertFalse(KinesisProxy.isRecoverableException(ex));
	}

	@Test
	public void testGetShardList() throws Exception {
		List<String> shardIds =
				Arrays.asList(
						"shardId-000000000000",
						"shardId-000000000001",
						"shardId-000000000002",
						"shardId-000000000003");
		String nextToken = "NextToken";
		String fakeStreamName = "fake-stream";
		List<Shard> shards = shardIds
						.stream()
						.map(shardId -> new Shard().withShardId(shardId))
						.collect(Collectors.toList());
		Properties kinesisConsumerConfig = new Properties();
		kinesisConsumerConfig.setProperty(ConsumerConfigConstants.AWS_REGION, "us-east-1");
		kinesisConsumerConfig.setProperty(ConsumerConfigConstants.AWS_ACCESS_KEY_ID, "fake_accesskey");
		kinesisConsumerConfig.setProperty(
				ConsumerConfigConstants.AWS_SECRET_ACCESS_KEY, "fake_secretkey");
		KinesisProxy kinesisProxy = new KinesisProxy(kinesisConsumerConfig);
		AmazonKinesis mockClient = mock(AmazonKinesis.class);
		Whitebox.setInternalState(kinesisProxy, "kinesisClient", mockClient);

		ListShardsResult responseWithMoreData =
				new ListShardsResult().withShards(shards.subList(0, 2)).withNextToken(nextToken);
		ListShardsResult responseFinal =
				new ListShardsResult().withShards(shards.subList(2, shards.size())).withNextToken(null);
		doReturn(responseWithMoreData)
				.when(mockClient)
				.listShards(argThat(initialListShardsRequestMatcher()));
		doReturn(responseFinal).
						when(mockClient).
						listShards(argThat(listShardsNextToken(nextToken)));
		HashMap<String, String> streamHashMap =
				createInitialSubscribedStreamsToLastDiscoveredShardsState(Arrays.asList(fakeStreamName));
		GetShardListResult shardListResult = kinesisProxy.getShardList(streamHashMap);

		Assert.assertEquals(shardListResult.hasRetrievedShards(), true);

		Set<String> expectedStreams = new HashSet<>();
		expectedStreams.add(fakeStreamName);
		Assert.assertEquals(shardListResult.getStreamsWithRetrievedShards(), expectedStreams);
		List<StreamShardHandle> actualShardList =
				shardListResult.getRetrievedShardListOfStream(fakeStreamName);
		List<StreamShardHandle> expectedStreamShard = new ArrayList<>();
		assertThat(actualShardList, hasSize(4));
		for (int i = 0; i < 4; i++) {
			StreamShardHandle shardHandle =
					new StreamShardHandle(
							fakeStreamName,
							new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(i)));
			expectedStreamShard.add(shardHandle);
		}

		Assert.assertThat(
				actualShardList,
				containsInAnyOrder(
						expectedStreamShard.toArray(new StreamShardHandle[actualShardList.size()])));
	}

	@Test
	public void testCustomConfigurationOverride() {
		Properties configProps = new Properties();
		configProps.setProperty(AWSConfigConstants.AWS_REGION, "us-east-1");
		KinesisProxy proxy = new KinesisProxy(configProps) {
			@Override
			protected AmazonKinesis createKinesisClient(Properties configProps) {
				ClientConfiguration clientConfig = new ClientConfigurationFactory().getConfig();
				clientConfig.setSocketTimeout(10000);
				return AWSUtil.createKinesisClient(configProps, clientConfig);
			}
		};
		AmazonKinesis kinesisClient = Whitebox.getInternalState(proxy, "kinesisClient");
		ClientConfiguration clientConfiguration = Whitebox.getInternalState(kinesisClient, "clientConfiguration");
		assertEquals(10000, clientConfiguration.getSocketTimeout());
	}

	@Test
	public void testClientConfigOverride() {

		Properties configProps = new Properties();
		configProps.setProperty(AWSConfigConstants.AWS_REGION, "us-east-1");
		configProps.setProperty(AWSUtil.AWS_CLIENT_CONFIG_PREFIX + "socketTimeout", "9999");

		KinesisProxyInterface proxy = KinesisProxy.create(configProps);

		AmazonKinesis kinesisClient = Whitebox.getInternalState(proxy, "kinesisClient");
		ClientConfiguration clientConfiguration = Whitebox.getInternalState(kinesisClient,
			"clientConfiguration");
		assertEquals(9999, clientConfiguration.getSocketTimeout());
	}

	protected static HashMap<String, String>
	createInitialSubscribedStreamsToLastDiscoveredShardsState(List<String> streams) {
		HashMap<String, String> initial = new HashMap<>();
		for (String stream : streams) {
			initial.put(stream, null);
		}
		return initial;
	}

	private static ListShardsRequestMatcher initialListShardsRequestMatcher() {
		return new ListShardsRequestMatcher(null, null);
	}

	private static ListShardsRequestMatcher listShardsNextToken(final String nextToken) {
		return new ListShardsRequestMatcher(null, nextToken);
	}

	private static class ListShardsRequestMatcher extends TypeSafeDiagnosingMatcher<ListShardsRequest> {
		private final String shardId;
		private final String nextToken;

		ListShardsRequestMatcher(String shardIdArg, String nextTokenArg) {
			shardId = shardIdArg;
			nextToken = nextTokenArg;
		}

		@Override
		protected boolean matchesSafely(final ListShardsRequest listShardsRequest, final Description description) {
			if (shardId == null) {
				if (listShardsRequest.getExclusiveStartShardId() != null) {
					return false;
				}
			} else {
				if (!shardId.equals(listShardsRequest.getExclusiveStartShardId())) {
					return false;
				}
			}

			if (listShardsRequest.getNextToken() != null) {
				if (!(listShardsRequest.getStreamName() == null
								&& listShardsRequest.getExclusiveStartShardId() == null)) {
					return false;
				}

				if (!listShardsRequest.getNextToken().equals(nextToken)) {
					return false;
				}
			} else {
				return nextToken == null;
			}
			return true;
		}

		@Override
		public void describeTo(final Description description) {
			description
							.appendText("A ListShardsRequest with a shardId: ")
							.appendValue(shardId)
							.appendText(" and empty nextToken");
		}
	}
}
