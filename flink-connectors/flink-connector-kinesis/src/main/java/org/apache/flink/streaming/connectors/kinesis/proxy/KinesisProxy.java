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
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.model.StreamShardHandle;
import org.apache.flink.streaming.connectors.kinesis.util.AWSUtil;
import org.apache.flink.streaming.connectors.kinesis.util.KinesisConfigUtil;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.ClientConfigurationFactory;
import com.amazonaws.SdkClientException;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.ExpiredNextTokenException;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
import com.amazonaws.services.kinesis.model.InvalidArgumentException;
import com.amazonaws.services.kinesis.model.LimitExceededException;
import com.amazonaws.services.kinesis.model.ListShardsRequest;
import com.amazonaws.services.kinesis.model.ListShardsResult;
import com.amazonaws.services.kinesis.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.kinesis.model.ResourceInUseException;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Kinesis proxy implementation - a utility class that is used as a proxy to make
 * calls to AWS Kinesis for several functions, such as getting a list of shards and
 * fetching a batch of data records starting from a specified record sequence number.
 *
 * <p>NOTE:
 * In the AWS KCL library, there is a similar implementation - {@link com.amazonaws.services.kinesis.clientlibrary.proxies.KinesisProxy}.
 * This implementation differs mainly in that we can make operations to arbitrary Kinesis streams, which is a needed
 * functionality for the Flink Kinesis Connector since the consumer may simultaneously read from multiple Kinesis streams.
 */
@Internal
public class KinesisProxy implements KinesisProxyInterface {

	private static final Logger LOG = LoggerFactory.getLogger(KinesisProxy.class);

	/** The actual Kinesis client from the AWS SDK that we will be using to make calls. */
	private final AmazonKinesis kinesisClient;

	/** Random seed used to calculate backoff jitter for Kinesis operations. */
	private static final Random seed = new Random();

	// ------------------------------------------------------------------------
	//  listShards() related performance settings
	// ------------------------------------------------------------------------

	/** Base backoff millis for the list shards operation. */
	private final long listShardsBaseBackoffMillis;

	/** Maximum backoff millis for the list shards operation. */
	private final long listShardsMaxBackoffMillis;

	/** Exponential backoff power constant for the list shards operation. */
	private final double listShardsExpConstant;

	// ------------------------------------------------------------------------
	//  getRecords() related performance settings
	// ------------------------------------------------------------------------

	/** Base backoff millis for the get records operation. */
	private final long getRecordsBaseBackoffMillis;

	/** Maximum backoff millis for the get records operation. */
	private final long getRecordsMaxBackoffMillis;

	/** Exponential backoff power constant for the get records operation. */
	private final double getRecordsExpConstant;

	/** Maximum attempts for the get records operation. */
	private final int getRecordsMaxAttempts;

	// ------------------------------------------------------------------------
	//  getShardIterator() related performance settings
	// ------------------------------------------------------------------------

	/** Base backoff millis for the get shard iterator operation. */
	private final long getShardIteratorBaseBackoffMillis;

	/** Maximum backoff millis for the get shard iterator operation. */
	private final long getShardIteratorMaxBackoffMillis;

	/** Exponential backoff power constant for the get shard iterator operation. */
	private final double getShardIteratorExpConstant;

	/** Maximum attempts for the get shard iterator operation. */
	private final int getShardIteratorMaxAttempts;

	/**
	 * Create a new KinesisProxy based on the supplied configuration properties.
	 *
	 * @param configProps configuration properties containing AWS credential and AWS region info
	 */
	protected KinesisProxy(Properties configProps) {
		checkNotNull(configProps);
		KinesisConfigUtil.replaceDeprecatedConsumerKeys(configProps);

		this.kinesisClient = createKinesisClient(configProps);

		this.listShardsBaseBackoffMillis = Long.valueOf(
			configProps.getProperty(
				ConsumerConfigConstants.LIST_SHARDS_BACKOFF_BASE,
				Long.toString(ConsumerConfigConstants.DEFAULT_LIST_SHARDS_BACKOFF_BASE)));
		this.listShardsMaxBackoffMillis = Long.valueOf(
			configProps.getProperty(
				ConsumerConfigConstants.LIST_SHARDS_BACKOFF_MAX,
				Long.toString(ConsumerConfigConstants.DEFAULT_LIST_SHARDS_BACKOFF_MAX)));
		this.listShardsExpConstant = Double.valueOf(
			configProps.getProperty(
				ConsumerConfigConstants.LIST_SHARDS_BACKOFF_EXPONENTIAL_CONSTANT,
				Double.toString(ConsumerConfigConstants.DEFAULT_LIST_SHARDS_BACKOFF_EXPONENTIAL_CONSTANT)));

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
	 * Create the Kinesis client, using the provided configuration properties and default {@link ClientConfiguration}.
	 * Derived classes can override this method to customize the client configuration.
	 * @param configProps
	 * @return
	 */
	protected AmazonKinesis createKinesisClient(Properties configProps) {

		ClientConfiguration awsClientConfig = new ClientConfigurationFactory().getConfig();
		AWSUtil.setAwsClientConfigProperties(awsClientConfig, configProps);
		return AWSUtil.createKinesisClient(configProps, awsClientConfig);
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
			} catch (SdkClientException ex) {
				if (isRecoverableSdkClientException(ex)) {
					long backoffMillis = fullJitterBackoff(
						getRecordsBaseBackoffMillis, getRecordsMaxBackoffMillis, getRecordsExpConstant, attempt++);
					LOG.warn("Got recoverable SdkClientException. Backing off for "
						+ backoffMillis + " millis (" + ex.getMessage() + ")");
					Thread.sleep(backoffMillis);
				} else {
					throw ex;
				}
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
	public GetShardListResult getShardList(Map<String, String> streamNamesWithLastSeenShardIds) throws InterruptedException {
		GetShardListResult result = new GetShardListResult();

		for (Map.Entry<String, String> streamNameWithLastSeenShardId : streamNamesWithLastSeenShardIds.entrySet()) {
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
	public String getShardIterator(StreamShardHandle shard, String shardIteratorType, @Nullable Object startingMarker) throws InterruptedException {
		GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest()
			.withStreamName(shard.getStreamName())
			.withShardId(shard.getShard().getShardId())
			.withShardIteratorType(shardIteratorType);

		switch (ShardIteratorType.fromValue(shardIteratorType)) {
			case TRIM_HORIZON:
			case LATEST:
				break;
			case AT_TIMESTAMP:
				if (startingMarker instanceof Date) {
					getShardIteratorRequest.setTimestamp((Date) startingMarker);
				} else {
					throw new IllegalArgumentException("Invalid object given for GetShardIteratorRequest() when ShardIteratorType is AT_TIMESTAMP. Must be a Date object.");
				}
				break;
			case AT_SEQUENCE_NUMBER:
			case AFTER_SEQUENCE_NUMBER:
				if (startingMarker instanceof String) {
					getShardIteratorRequest.setStartingSequenceNumber((String) startingMarker);
				} else {
					throw new IllegalArgumentException("Invalid object given for GetShardIteratorRequest() when ShardIteratorType is AT_SEQUENCE_NUMBER or AFTER_SEQUENCE_NUMBER. Must be a String.");
				}
		}
		return getShardIterator(getShardIteratorRequest);
	}

	private String getShardIterator(GetShardIteratorRequest getShardIteratorRequest) throws InterruptedException {
		GetShardIteratorResult getShardIteratorResult = null;

		int attempt = 0;
		while (attempt <= getShardIteratorMaxAttempts && getShardIteratorResult == null) {
			try {
					getShardIteratorResult = kinesisClient.getShardIterator(getShardIteratorRequest);
			} catch (AmazonServiceException ex) {
				if (isRecoverableException(ex)) {
					long backoffMillis = fullJitterBackoff(
						getShardIteratorBaseBackoffMillis, getShardIteratorMaxBackoffMillis, getShardIteratorExpConstant, attempt++);
					LOG.warn("Got recoverable AmazonServiceException. Backing off for "
						+ backoffMillis + " millis (" + ex.getErrorMessage() + ")");
					Thread.sleep(backoffMillis);
				} else {
					throw ex;
				}
			}
		}

		if (getShardIteratorResult == null) {
			throw new RuntimeException("Rate Exceeded for getShardIterator operation - all " + getShardIteratorMaxAttempts +
				" retry attempts returned ProvisionedThroughputExceededException.");
		}
		return getShardIteratorResult.getShardIterator();
	}

	/**
	 * Determines whether the exception is recoverable using exponential-backoff.
	 *
	 * @param ex Exception to inspect
	 * @return <code>true</code> if the exception can be recovered from, else
	 *         <code>false</code>
	 */
	protected boolean isRecoverableSdkClientException(SdkClientException ex) {
		if (ex instanceof AmazonServiceException) {
			return KinesisProxy.isRecoverableException((AmazonServiceException) ex);
		}
		// customizations may decide to retry other errors, such as read timeouts
		return false;
	}

	/**
	 * Determines whether the exception is recoverable using exponential-backoff.
	 *
	 * @param ex Exception to inspect
	 * @return <code>true</code> if the exception can be recovered from, else
	 *         <code>false</code>
	 */
	protected static boolean isRecoverableException(AmazonServiceException ex) {
		if (ex.getErrorType() == null) {
			return false;
		}

		switch (ex.getErrorType()) {
			case Client:
				return ex instanceof ProvisionedThroughputExceededException;
			case Service:
			case Unknown:
				return true;
			default:
				return false;
		}
	}

	private List<StreamShardHandle> getShardsOfStream(String streamName, @Nullable String lastSeenShardId) throws InterruptedException {
		List<StreamShardHandle> shardsOfStream = new ArrayList<>();

		// List Shards returns just the first 1000 shard entries. In order to read the entire stream,
		// we need to use the returned nextToken to get additional shards.
		ListShardsResult listShardsResult;
		String startShardToken = null;
		do {
			listShardsResult = listShards(streamName, lastSeenShardId, startShardToken);
			if (listShardsResult == null) {
				// In case we have exceptions while retrieving all shards, ensure that incomplete shard list is not returned.
				// Hence clearing the incomplete shard list before returning it.
				shardsOfStream.clear();
				return shardsOfStream;
			}
			List<Shard> shards = listShardsResult.getShards();
			for (Shard shard : shards) {
				shardsOfStream.add(new StreamShardHandle(streamName, shard));
			}
			startShardToken = listShardsResult.getNextToken();
		} while (startShardToken != null);

		return shardsOfStream;
	}

	/**
	 * Get metainfo for a Kinesis stream, which contains information about which shards this Kinesis stream possess.
	 *
	 * <p>This method is using a "full jitter" approach described in AWS's article,
	 * <a href="https://www.awsarchitectureblog.com/2015/03/backoff.html">"Exponential Backoff and Jitter"</a>.
	 * This is necessary because concurrent calls will be made by all parallel subtask's fetcher. This
	 * jitter backoff approach will help distribute calls across the fetchers over time.
	 *
	 * @param streamName the stream to describe
	 * @param startShardId which shard to start with for this describe operation (earlier shard's infos will not appear in result)
	 * @return the result of the describe stream operation
	 */
	private ListShardsResult listShards(String streamName, @Nullable String startShardId,
																			@Nullable String startNextToken)
			throws InterruptedException {
		final ListShardsRequest listShardsRequest = new ListShardsRequest();
		if (startNextToken == null) {
			listShardsRequest.setExclusiveStartShardId(startShardId);
			listShardsRequest.setStreamName(streamName);
		} else {
			// Note the nextToken returned by AWS expires within 300 sec.
			listShardsRequest.setNextToken(startNextToken);
		}

		ListShardsResult listShardsResults = null;

		// Call ListShards, with full-jitter backoff (if we get LimitExceededException).
		int attemptCount = 0;
		// List Shards returns just the first 1000 shard entries. Make sure that all entries
		// are taken up.
		while (listShardsResults == null) { // retry until we get a result
			try {

				listShardsResults = kinesisClient.listShards(listShardsRequest);
			} catch (LimitExceededException le) {
				long backoffMillis = fullJitterBackoff(
						listShardsBaseBackoffMillis, listShardsMaxBackoffMillis, listShardsExpConstant, attemptCount++);
					LOG.warn("Got LimitExceededException when listing shards from stream " + streamName
									+ ". Backing off for " + backoffMillis + " millis.");
				Thread.sleep(backoffMillis);
			} catch (ResourceInUseException reInUse) {
				if (LOG.isWarnEnabled()) {
					// List Shards will throw an exception if stream in not in active state. Return and re-use previous state available.
					LOG.info("The stream is currently not in active state. Reusing the older state "
							+ "for the time being");
					break;
				}
			} catch (ResourceNotFoundException reNotFound) {
				throw new RuntimeException("Stream not found. Error while getting shard list.", reNotFound);
			} catch (InvalidArgumentException inArg) {
				throw new RuntimeException("Invalid Arguments to listShards.", inArg);
			} catch (ExpiredNextTokenException expiredToken) {
				LOG.warn("List Shards has an expired token. Reusing the previous state.");
				break;
			}
		}
		// Kinesalite (mock implementation of Kinesis) does not correctly exclude shards before
		// the exclusive start shard id in the returned shards list; check if we need to remove
		// these erroneously returned shards.
		if (startShardId != null && listShardsResults != null) {
			List<Shard> shards = listShardsResults.getShards();
			Iterator<Shard> shardItr = shards.iterator();
			while (shardItr.hasNext()) {
				if (StreamShardHandle.compareShardIds(shardItr.next().getShardId(), startShardId) <= 0) {
					shardItr.remove();
				}
			}
		}
		return listShardsResults;
	}

	protected static long fullJitterBackoff(long base, long max, double power, int attempt) {
		long exponentialBackoff = (long) Math.min(max, base * Math.pow(power, attempt));
		return (long) (seed.nextDouble() * exponentialBackoff); // random jitter between 0 and the exponential backoff
	}
}
