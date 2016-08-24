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

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
import com.amazonaws.services.kinesis.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.kinesis.model.LimitExceededException;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.model.StreamStatus;
import com.amazonaws.services.kinesis.model.Shard;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.model.KinesisStreamShard;
import org.apache.flink.streaming.connectors.kinesis.util.AWSUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Map;
import java.util.Random;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Kinesis proxy implementation - a utility class that is used as a proxy to make
 * calls to AWS Kinesis for several functions, such as getting a list of shards and
 * fetching a batch of data records starting from a specified record sequence number.
 *
 * NOTE:
 * In the AWS KCL library, there is a similar implementation - {@link com.amazonaws.services.kinesis.clientlibrary.proxies.KinesisProxy}.
 * This implementation differs mainly in that we can make operations to arbitrary Kinesis streams, which is a needed
 * functionality for the Flink Kinesis Connecter since the consumer may simultaneously read from multiple Kinesis streams.
 */
public class KinesisProxy implements KinesisProxyInterface {

	private static final Logger LOG = LoggerFactory.getLogger(KinesisProxy.class);

	/** The actual Kinesis client from the AWS SDK that we will be using to make calls */
	private final AmazonKinesisClient kinesisClient;

	/** Random seed used to calculate backoff jitter for Kinesis operations */
	private final static Random seed = new Random();

	// ------------------------------------------------------------------------
	//  describeStream() related performance settings
	// ------------------------------------------------------------------------

	/** Base backoff millis for the describe stream operation */
	private final long describeStreamBaseBackoffMillis;

	/** Maximum backoff millis for the describe stream operation */
	private final long describeStreamMaxBackoffMillis;

	/** Exponential backoff power constant for the describe stream operation */
	private final double describeStreamExpConstant;

	// ------------------------------------------------------------------------
	//  getRecords() related performance settings
	// ------------------------------------------------------------------------

	/** Base backoff millis for the get records operation */
	private final long getRecordsBaseBackoffMillis;

	/** Maximum backoff millis for the get records operation */
	private final long getRecordsMaxBackoffMillis;

	/** Exponential backoff power constant for the get records operation */
	private final double getRecordsExpConstant;

	/** Maximum attempts for the get records operation */
	private final int getRecordsMaxAttempts;

	// ------------------------------------------------------------------------
	//  getShardIterator() related performance settings
	// ------------------------------------------------------------------------

	/** Base backoff millis for the get shard iterator operation */
	private final long getShardIteratorBaseBackoffMillis;

	/** Maximum backoff millis for the get shard iterator operation */
	private final long getShardIteratorMaxBackoffMillis;

	/** Exponential backoff power constant for the get shard iterator operation */
	private final double getShardIteratorExpConstant;

	/** Maximum attempts for the get shard iterator operation */
	private final int getShardIteratorMaxAttempts;

	/**
	 * Create a new KinesisProxy based on the supplied configuration properties
	 *
	 * @param configProps configuration properties containing AWS credential and AWS region info
	 */
	private KinesisProxy(Properties configProps) {
		checkNotNull(configProps);

		this.kinesisClient = AWSUtil.createKinesisClient(configProps);

		this.describeStreamBaseBackoffMillis = Long.valueOf(
			configProps.getProperty(
				ConsumerConfigConstants.STREAM_DESCRIBE_BACKOFF_BASE,
				Long.toString(ConsumerConfigConstants.DEFAULT_STREAM_DESCRIBE_BACKOFF_BASE)));
		this.describeStreamMaxBackoffMillis = Long.valueOf(
			configProps.getProperty(
				ConsumerConfigConstants.STREAM_DESCRIBE_BACKOFF_MAX,
				Long.toString(ConsumerConfigConstants.DEFAULT_STREAM_DESCRIBE_BACKOFF_MAX)));
		this.describeStreamExpConstant = Double.valueOf(
			configProps.getProperty(
				ConsumerConfigConstants.STREAM_DESCRIBE_BACKOFF_EXPONENTIAL_CONSTANT,
				Double.toString(ConsumerConfigConstants.DEFAULT_STREAM_DESCRIBE_BACKOFF_EXPONENTIAL_CONSTANT)));

		this.getRecordsBaseBackoffMillis = Long.valueOf(
			configProps.getProperty(
				ConsumerConfigConstants.SHARD_GETRECORDS_BACKOFF_BASE,
				Long.toString(ConsumerConfigConstants.DEFAULT_SHARD_GETRECORDS_BACKOFF_BASE)));
		this.getRecordsMaxBackoffMillis = Long.valueOf(
			configProps.getProperty(
				ConsumerConfigConstants.SHARD_GETRECORDS_BACKOFF_MAX,
				Long.toString(ConsumerConfigConstants.DEFAULT_SHARD_GETRECORDS_BACKOFF_MAX)));
		this.getRecordsExpConstant = Double.valueOf(
			configProps.getProperty(
				ConsumerConfigConstants.SHARD_GETRECORDS_BACKOFF_EXPONENTIAL_CONSTANT,
				Double.toString(ConsumerConfigConstants.DEFAULT_SHARD_GETRECORDS_BACKOFF_EXPONENTIAL_CONSTANT)));
		this.getRecordsMaxAttempts = Integer.valueOf(
			configProps.getProperty(
				ConsumerConfigConstants.SHARD_GETRECORDS_RETRIES,
				Long.toString(ConsumerConfigConstants.DEFAULT_SHARD_GETRECORDS_RETRIES)));

		this.getShardIteratorBaseBackoffMillis = Long.valueOf(
			configProps.getProperty(
				ConsumerConfigConstants.SHARD_GETITERATOR_BACKOFF_BASE,
				Long.toString(ConsumerConfigConstants.DEFAULT_SHARD_GETITERATOR_BACKOFF_BASE)));
		this.getShardIteratorMaxBackoffMillis = Long.valueOf(
			configProps.getProperty(
				ConsumerConfigConstants.SHARD_GETITERATOR_BACKOFF_MAX,
				Long.toString(ConsumerConfigConstants.DEFAULT_SHARD_GETITERATOR_BACKOFF_MAX)));
		this.getShardIteratorExpConstant = Double.valueOf(
			configProps.getProperty(
				ConsumerConfigConstants.SHARD_GETITERATOR_BACKOFF_EXPONENTIAL_CONSTANT,
				Double.toString(ConsumerConfigConstants.DEFAULT_SHARD_GETITERATOR_BACKOFF_EXPONENTIAL_CONSTANT)));
		this.getShardIteratorMaxAttempts = Integer.valueOf(
			configProps.getProperty(
				ConsumerConfigConstants.SHARD_GETITERATOR_RETRIES,
				Long.toString(ConsumerConfigConstants.DEFAULT_SHARD_GETITERATOR_RETRIES)));

	}

	/**
	 * Creates a Kinesis proxy.
	 *
	 * @param configProps configuration properties
	 * @return the created kinesis proxy
	 */
	public static KinesisProxyInterface create(Properties configProps) {
		return new KinesisProxy(configProps);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public GetRecordsResult getRecords(String shardIterator, int maxRecordsToGet) throws InterruptedException {
		final GetRecordsRequest getRecordsRequest = new GetRecordsRequest();
		getRecordsRequest.setShardIterator(shardIterator);
		getRecordsRequest.setLimit(maxRecordsToGet);

		GetRecordsResult getRecordsResult = null;

		int attempt = 0;
		while (attempt <= getRecordsMaxAttempts && getRecordsResult == null) {
			try {
				getRecordsResult = kinesisClient.getRecords(getRecordsRequest);
			} catch (ProvisionedThroughputExceededException ex) {
				long backoffMillis = fullJitterBackoff(
					getRecordsBaseBackoffMillis, getRecordsMaxBackoffMillis, getRecordsExpConstant, attempt++);
				LOG.warn("Got ProvisionedThroughputExceededException. Backing off for "
					+ backoffMillis + " millis.");
				Thread.sleep(backoffMillis);
			}
		}

		if (getRecordsResult == null) {
			throw new RuntimeException("Rate Exceeded for getRecords operation - all " + getRecordsMaxAttempts +
				" retry attempts returned ProvisionedThroughputExceededException.");
		}

		return getRecordsResult;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public GetShardListResult getShardList(Map<String,String> streamNamesWithLastSeenShardIds) throws InterruptedException {
		GetShardListResult result = new GetShardListResult();

		for (Map.Entry<String,String> streamNameWithLastSeenShardId : streamNamesWithLastSeenShardIds.entrySet()) {
			String stream = streamNameWithLastSeenShardId.getKey();
			String lastSeenShardId = streamNameWithLastSeenShardId.getValue();
			result.addRetrievedShardsToStream(stream, getShardsOfStream(stream, lastSeenShardId));
		}
		return result;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getShardIterator(KinesisStreamShard shard, String shardIteratorType, String startingSeqNum) throws InterruptedException {
		GetShardIteratorResult getShardIteratorResult = null;

		int attempt = 0;
		while (attempt <= getShardIteratorMaxAttempts && getShardIteratorResult == null) {
			try {
				getShardIteratorResult =
					kinesisClient.getShardIterator(shard.getStreamName(), shard.getShard().getShardId(), shardIteratorType, startingSeqNum);
			} catch (ProvisionedThroughputExceededException ex) {
				long backoffMillis = fullJitterBackoff(
					getShardIteratorBaseBackoffMillis, getShardIteratorMaxBackoffMillis, getShardIteratorExpConstant, attempt++);
				LOG.warn("Got ProvisionedThroughputExceededException. Backing off for "
					+ backoffMillis + " millis.");
				Thread.sleep(backoffMillis);
			}
		}

		if (getShardIteratorResult == null) {
			throw new RuntimeException("Rate Exceeded for getShardIterator operation - all " + getShardIteratorMaxAttempts +
				" retry attempts returned ProvisionedThroughputExceededException.");
		}
		return getShardIteratorResult.getShardIterator();
	}

	private List<KinesisStreamShard> getShardsOfStream(String streamName, String lastSeenShardId) throws InterruptedException {
		List<KinesisStreamShard> shardsOfStream = new ArrayList<>();

		DescribeStreamResult describeStreamResult;
		do {
			describeStreamResult = describeStream(streamName, lastSeenShardId);

			List<Shard> shards = describeStreamResult.getStreamDescription().getShards();
			for (Shard shard : shards) {
				shardsOfStream.add(new KinesisStreamShard(streamName, shard));
			}

			if (shards.size() != 0) {
				lastSeenShardId = shards.get(shards.size() - 1).getShardId();
			}
		} while (describeStreamResult.getStreamDescription().isHasMoreShards());

		return shardsOfStream;
	}

	/**
	 * Get metainfo for a Kinesis stream, which contains information about which shards this Kinesis stream possess.
	 *
	 * This method is using a "full jitter" approach described in AWS's article,
	 * <a href="https://www.awsarchitectureblog.com/2015/03/backoff.html">"Exponential Backoff and Jitter"</a>.
	 * This is necessary because concurrent calls will be made by all parallel subtask's fetcher. This
	 * jitter backoff approach will help distribute calls across the fetchers over time.
	 *
	 * @param streamName the stream to describe
	 * @param startShardId which shard to start with for this describe operation (earlier shard's infos will not appear in result)
	 * @return the result of the describe stream operation
	 */
	private DescribeStreamResult describeStream(String streamName, String startShardId) throws InterruptedException {
		final DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
		describeStreamRequest.setStreamName(streamName);
		describeStreamRequest.setExclusiveStartShardId(startShardId);

		DescribeStreamResult describeStreamResult = null;

		// Call DescribeStream, with full-jitter backoff (if we get LimitExceededException).
		int attemptCount = 0;
		while (describeStreamResult == null) { // retry until we get a result
			try {
				describeStreamResult = kinesisClient.describeStream(describeStreamRequest);
			} catch (LimitExceededException le) {
				long backoffMillis = fullJitterBackoff(
					describeStreamBaseBackoffMillis, describeStreamMaxBackoffMillis, describeStreamExpConstant, attemptCount++);
				LOG.warn("Got LimitExceededException when describing stream " + streamName + ". Backing off for "
					+ backoffMillis + " millis.");
				Thread.sleep(backoffMillis);
			} catch (ResourceNotFoundException re) {
				throw new RuntimeException("Error while getting stream details", re);
			}
		}

		String streamStatus = describeStreamResult.getStreamDescription().getStreamStatus();
		if (!(streamStatus.equals(StreamStatus.ACTIVE.toString()) || streamStatus.equals(StreamStatus.UPDATING.toString()))) {
			if (LOG.isWarnEnabled()) {
				LOG.warn("The status of stream " + streamName + " is " + streamStatus + "; result of the current " +
					"describeStream operation will not contain any shard information.");
			}
		}

		return describeStreamResult;
	}

	private static long fullJitterBackoff(long base, long max, double power, int attempt) {
		long exponentialBackoff = (long) Math.min(max, base * Math.pow(power, attempt));
		return (long)(seed.nextDouble()*exponentialBackoff); // random jitter between 0 and the exponential backoff
	}
}
