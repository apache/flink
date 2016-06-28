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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.ClientConfigurationFactory;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.kinesis.model.LimitExceededException;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.model.StreamStatus;
import com.amazonaws.services.kinesis.model.Shard;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.streaming.connectors.kinesis.config.KinesisConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.model.KinesisStreamShard;
import org.apache.flink.streaming.connectors.kinesis.util.AWSUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A utility class that is used as a proxy to make calls to AWS Kinesis
 * for several functions, such as getting a list of shards and fetching
 * a batch of data records starting from a specified record sequence number.
 *
 * NOTE:
 * In the AWS KCL library, there is a similar implementation - {@link com.amazonaws.services.kinesis.clientlibrary.proxies.KinesisProxy}.
 * This implementation differs mainly in that we can make operations to arbitrary Kinesis streams, which is a needed
 * functionality for the Flink Kinesis Connecter since the consumer may simultaneously read from multiple Kinesis streams.
 */
public class KinesisProxy {

	private static final Logger LOG = LoggerFactory.getLogger(KinesisProxy.class);

	/** The actual Kinesis client from the AWS SDK that we will be using to make calls */
	private final AmazonKinesisClient kinesisClient;

	/** Configuration properties of this Flink Kinesis Connector */
	private final Properties configProps;

	/**
	 * Create a new KinesisProxy based on the supplied configuration properties
	 *
	 * @param configProps configuration properties containing AWS credential and AWS region info
	 */
	public KinesisProxy(Properties configProps) {
		this.configProps = checkNotNull(configProps);

		/* The AWS region that this proxy will be making calls to */
		String regionId = configProps.getProperty(KinesisConfigConstants.CONFIG_AWS_REGION);
		// set Flink as a user agent
		ClientConfiguration config = new ClientConfigurationFactory().getConfig();
		config.setUserAgent("Apache Flink " + EnvironmentInformation.getVersion() + " (" + EnvironmentInformation.getRevisionInformation().commitId + ") Kinesis Connector");
		AmazonKinesisClient client = new AmazonKinesisClient(AWSUtil.getCredentialsProvider(configProps).getCredentials(), config);

		client.setRegion(Region.getRegion(Regions.fromName(regionId)));

		this.kinesisClient = client;
	}

	/**
	 * Get the next batch of data records using a specific shard iterator
	 *
	 * @param shardIterator a shard iterator that encodes info about which shard to read and where to start reading
	 * @param maxRecordsToGet the maximum amount of records to retrieve for this batch
	 * @return the batch of retrieved records
	 */
	public GetRecordsResult getRecords(String shardIterator, int maxRecordsToGet) {
		final GetRecordsRequest getRecordsRequest = new GetRecordsRequest();
		getRecordsRequest.setShardIterator(shardIterator);
		getRecordsRequest.setLimit(maxRecordsToGet);

		GetRecordsResult getRecordsResult = null;

		int remainingRetryTimes = Integer.valueOf(
			configProps.getProperty(KinesisConfigConstants.CONFIG_STREAM_DESCRIBE_RETRIES, Integer.toString(KinesisConfigConstants.DEFAULT_STREAM_DESCRIBE_RETRY_TIMES)));
		long describeStreamBackoffTimeInMillis = Long.valueOf(
			configProps.getProperty(KinesisConfigConstants.CONFIG_STREAM_DESCRIBE_BACKOFF, Long.toString(KinesisConfigConstants.DEFAULT_STREAM_DESCRIBE_BACKOFF)));

		int i=0;
		while (i <= remainingRetryTimes && getRecordsResult == null) {
			try {
				getRecordsResult = kinesisClient.getRecords(getRecordsRequest);
			} catch (ProvisionedThroughputExceededException ex) {
				LOG.warn("Got ProvisionedThroughputExceededException. Backing off for "
					+ describeStreamBackoffTimeInMillis + " millis.");
				try {
					Thread.sleep(describeStreamBackoffTimeInMillis);
				} catch (InterruptedException interruptEx) {
					//
				}
			}
			i++;
		}

		if (getRecordsResult == null) {
			throw new RuntimeException("Rate Exceeded");
		}

		return getRecordsResult;
	}

	/**
	 * Get the list of shards associated with multiple Kinesis streams
	 *
	 * @param streamNames the list of Kinesis streams
	 * @return a list of {@link KinesisStreamShard}s
	 */
	public List<KinesisStreamShard> getShardList(List<String> streamNames) {
		List<KinesisStreamShard> shardList = new ArrayList<>();

		for (String stream : streamNames) {
			DescribeStreamResult describeStreamResult;
			String lastSeenShardId = null;

			do {
				describeStreamResult = describeStream(stream, lastSeenShardId);

				List<Shard> shards = describeStreamResult.getStreamDescription().getShards();
				for (Shard shard : shards) {
					shardList.add(new KinesisStreamShard(stream, shard));
				}
				lastSeenShardId = shards.get(shards.size() - 1).getShardId();
			} while (describeStreamResult.getStreamDescription().isHasMoreShards());
		}
		return shardList;
	}

	/**
	 * Get a shard iterator for a Kinesis shard
	 *
	 * @param shard the shard to get the iterator for
	 * @param shardIteratorType the iterator type to get
	 * @param startingSeqNum the sequence number that the iterator will start from
	 * @return the shard iterator
	 */
	public String getShardIterator(KinesisStreamShard shard, String shardIteratorType, String startingSeqNum) {
		return kinesisClient.getShardIterator(shard.getStreamName(), shard.getShardId(), shardIteratorType, startingSeqNum).getShardIterator();
	}

	/**
	 * Get metainfo for a Kinesis stream, which contains information about which shards this Kinesis stream possess.
	 *
	 * @param streamName the stream to describe
	 * @param startShardId which shard to start with for this describe operation (earlier shard's infos will not appear in result)
	 * @return the result of the describe stream operation
	 */
	private DescribeStreamResult describeStream(String streamName, String startShardId) {
		final DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
		describeStreamRequest.setStreamName(streamName);
		describeStreamRequest.setExclusiveStartShardId(startShardId);

		DescribeStreamResult describeStreamResult = null;
		String streamStatus = null;
		int remainingRetryTimes = Integer.valueOf(
			configProps.getProperty(KinesisConfigConstants.CONFIG_STREAM_DESCRIBE_RETRIES, Integer.toString(KinesisConfigConstants.DEFAULT_STREAM_DESCRIBE_RETRY_TIMES)));
		long describeStreamBackoffTimeInMillis = Long.valueOf(
			configProps.getProperty(KinesisConfigConstants.CONFIG_STREAM_DESCRIBE_BACKOFF, Long.toString(KinesisConfigConstants.DEFAULT_STREAM_DESCRIBE_BACKOFF)));

		// Call DescribeStream, with backoff and retries (if we get LimitExceededException).
		while ((remainingRetryTimes >= 0) && (describeStreamResult == null)) {
			try {
				describeStreamResult = kinesisClient.describeStream(describeStreamRequest);
				streamStatus = describeStreamResult.getStreamDescription().getStreamStatus();
			} catch (LimitExceededException le) {
				LOG.warn("Got LimitExceededException when describing stream " + streamName + ". Backing off for "
					+ describeStreamBackoffTimeInMillis + " millis.");
				try {
					Thread.sleep(describeStreamBackoffTimeInMillis);
				} catch (InterruptedException ie) {
					LOG.debug("Stream " + streamName + " : Sleep  was interrupted ", ie);
				}
			} catch (ResourceNotFoundException re) {
				throw new RuntimeException("Error while getting stream details", re);
			}
			remainingRetryTimes--;
		}

		if (streamStatus == null) {
			throw new RuntimeException("Can't get stream info from after 3 retries due to LimitExceededException");
		} else if (streamStatus.equals(StreamStatus.ACTIVE.toString()) ||
			streamStatus.equals(StreamStatus.UPDATING.toString())) {
			return describeStreamResult;
		} else {
			throw new RuntimeException("Stream is not Active or Updating");
		}
	}
}
