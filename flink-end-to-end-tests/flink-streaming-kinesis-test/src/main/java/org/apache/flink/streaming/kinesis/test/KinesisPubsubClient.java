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

package org.apache.flink.streaming.kinesis.test;

import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.kinesis.shaded.com.amazonaws.AmazonClientException;
import org.apache.flink.kinesis.shaded.com.amazonaws.auth.AWSCredentialsProvider;
import org.apache.flink.kinesis.shaded.com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import org.apache.flink.kinesis.shaded.com.amazonaws.client.builder.AwsClientBuilder;
import org.apache.flink.kinesis.shaded.com.amazonaws.services.kinesis.AmazonKinesis;
import org.apache.flink.kinesis.shaded.com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import org.apache.flink.kinesis.shaded.com.amazonaws.services.kinesis.model.GetRecordsResult;
import org.apache.flink.kinesis.shaded.com.amazonaws.services.kinesis.model.PutRecordRequest;
import org.apache.flink.kinesis.shaded.com.amazonaws.services.kinesis.model.PutRecordResult;
import org.apache.flink.kinesis.shaded.com.amazonaws.services.kinesis.model.Record;
import org.apache.flink.kinesis.shaded.com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.model.StreamShardHandle;
import org.apache.flink.streaming.connectors.kinesis.proxy.GetShardListResult;
import org.apache.flink.streaming.connectors.kinesis.proxy.KinesisProxy;
import org.apache.flink.streaming.connectors.kinesis.proxy.KinesisProxyInterface;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

class KinesisPubsubClient implements KinesisExampleTest.PubsubClient {
	private static final Logger LOG = LoggerFactory.getLogger(KinesisPubsubClient.class);

	private final AmazonKinesis kinesisClient;
	private final Properties properties;

	KinesisPubsubClient(Properties properties) {
		this.kinesisClient = createClientWithCredentials(properties);
		this.properties = properties;
	}

	@Override
	public void createTopic(String stream, int shards, Properties props) throws Exception {
		try {
			kinesisClient.describeStream(stream);
			kinesisClient.deleteStream(stream);
		} catch (ResourceNotFoundException rnfe) {
			// expected when stream doesn't exist
		}

		kinesisClient.createStream(stream, shards);
		Deadline deadline  = Deadline.fromNow(Duration.ofSeconds(5));
		while (deadline.hasTimeLeft()) {
			try {
				Thread.sleep(250); // sleep for a bit for stream to be created
				if (kinesisClient.describeStream(stream).getStreamDescription()
					.getShards().size() != shards) {
					// not fully created yet
					continue;
				}
				break;
			} catch (ResourceNotFoundException rnfe) {
				// not ready yet
			}
		}
	}

	@Override
	public void sendMessage(String topic, String msg) {
		PutRecordRequest putRecordRequest = new PutRecordRequest();
		putRecordRequest.setStreamName(topic);
		putRecordRequest.setPartitionKey("fakePartitionKey");
		putRecordRequest.withData(ByteBuffer.wrap(msg.getBytes()));
		PutRecordResult putRecordResult = kinesisClient.putRecord(putRecordRequest);
		LOG.info("added record: {}", putRecordResult.getSequenceNumber());
	}

	@Override
	public List<String> readAllMessages(String streamName) throws Exception {
		KinesisProxyInterface kinesisProxy = KinesisProxy.create(properties);
		Map<String, String> streamNamesWithLastSeenShardIds = new HashMap<>();
		streamNamesWithLastSeenShardIds.put(streamName, null);

		GetShardListResult shardListResult = kinesisProxy.getShardList(streamNamesWithLastSeenShardIds);
		int maxRecordsToFetch = 10;

		List<String> messages = new ArrayList<>();
		// retrieve records from all shards
		for (StreamShardHandle ssh : shardListResult.getRetrievedShardListOfStream(streamName)) {
			String shardIterator = kinesisProxy.getShardIterator(ssh, "TRIM_HORIZON", null);
			GetRecordsResult getRecordsResult = kinesisProxy.getRecords(shardIterator, maxRecordsToFetch);
			List<Record> aggregatedRecords = getRecordsResult.getRecords();
			for (Record record : aggregatedRecords) {
				messages.add(new String(record.getData().array()));
			}
		}
		return messages;
	}

	private static AmazonKinesis createClientWithCredentials(Properties props) throws AmazonClientException {
		AWSCredentialsProvider credentialsProvider = new EnvironmentVariableCredentialsProvider();
		return AmazonKinesisClientBuilder.standard()
			.withCredentials(credentialsProvider)
			.withEndpointConfiguration(
				new AwsClientBuilder.EndpointConfiguration(
					props.getProperty(ConsumerConfigConstants.AWS_ENDPOINT), "us-east-1"))
			.build();
	}

}
