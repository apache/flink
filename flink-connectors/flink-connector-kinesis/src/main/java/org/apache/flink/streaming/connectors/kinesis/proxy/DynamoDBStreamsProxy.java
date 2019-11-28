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

import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.streaming.connectors.kinesis.model.StreamShardHandle;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.ClientConfigurationFactory;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.streamsadapter.AmazonDynamoDBStreamsAdapterClient;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.Shard;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants.AWS_ENDPOINT;
import static org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants.AWS_REGION;
import static org.apache.flink.streaming.connectors.kinesis.util.AWSUtil.getCredentialsProvider;
import static org.apache.flink.streaming.connectors.kinesis.util.AWSUtil.setAwsClientConfigProperties;

/**
 * DynamoDB streams proxy: interface interacting with the DynamoDB streams.
 */
public class DynamoDBStreamsProxy extends KinesisProxy {
	private static final Logger LOG = LoggerFactory.getLogger(DynamoDBStreamsProxy.class);

	/** Used for formatting Flink-specific user agent string when creating Kinesis client. */
	private static final String USER_AGENT_FORMAT = "Apache Flink %s (%s) DynamoDB Streams Connector";

	protected DynamoDBStreamsProxy(Properties configProps) {
		super(configProps);
	}

	/**
	 * Creates a DynamoDB streams proxy.
	 *
	 * @param configProps configuration properties
	 * @return the created DynamoDB streams proxy
	 */
	public static KinesisProxyInterface create(Properties configProps) {
		return new DynamoDBStreamsProxy(configProps);
	}

	/**
	 * Creates an AmazonDynamoDBStreamsAdapterClient.
	 * Uses it as the internal client interacting with the DynamoDB streams.
	 *
	 * @param configProps configuration properties
	 * @return an AWS DynamoDB streams adapter client
	 */
	@Override
	protected AmazonKinesis createKinesisClient(Properties configProps) {
		ClientConfiguration awsClientConfig = new ClientConfigurationFactory().getConfig();
		setAwsClientConfigProperties(awsClientConfig, configProps);

		AWSCredentialsProvider credentials = getCredentialsProvider(configProps);
		awsClientConfig.setUserAgentPrefix(
				String.format(
						USER_AGENT_FORMAT,
						EnvironmentInformation.getVersion(),
						EnvironmentInformation.getRevisionInformation().commitId));

		AmazonDynamoDBStreamsAdapterClient adapterClient =
				new AmazonDynamoDBStreamsAdapterClient(credentials, awsClientConfig);

		if (configProps.containsKey(AWS_ENDPOINT)) {
			adapterClient.setEndpoint(configProps.getProperty(AWS_ENDPOINT));
		} else {
			adapterClient.setRegion(Region.getRegion(
					Regions.fromName(configProps.getProperty(AWS_REGION))));
		}

		return adapterClient;
	}

	@Override
	public GetShardListResult getShardList(
			Map<String, String> streamNamesWithLastSeenShardIds) throws InterruptedException {
		GetShardListResult result = new GetShardListResult();

		for (Map.Entry<String, String> streamNameWithLastSeenShardId :
				streamNamesWithLastSeenShardIds.entrySet()) {
			String stream = streamNameWithLastSeenShardId.getKey();
			String lastSeenShardId = streamNameWithLastSeenShardId.getValue();
			result.addRetrievedShardsToStream(stream, getShardsOfStream(stream, lastSeenShardId));
		}
		return result;
	}

	private List<StreamShardHandle> getShardsOfStream(
			String streamName,
			@Nullable String lastSeenShardId)
			throws InterruptedException {
		List<StreamShardHandle> shardsOfStream = new ArrayList<>();

		DescribeStreamResult describeStreamResult;
		do {
			describeStreamResult = describeStream(streamName, lastSeenShardId);
			List<Shard> shards = describeStreamResult.getStreamDescription().getShards();
			for (Shard shard : shards) {
				shardsOfStream.add(new StreamShardHandle(streamName, shard));
			}

			if (shards.size() != 0) {
				lastSeenShardId = shards.get(shards.size() - 1).getShardId();
			}
		} while (describeStreamResult.getStreamDescription().isHasMoreShards());

		return shardsOfStream;
	}
}
