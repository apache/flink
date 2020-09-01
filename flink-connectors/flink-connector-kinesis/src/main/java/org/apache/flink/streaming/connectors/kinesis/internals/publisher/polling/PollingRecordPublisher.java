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

package org.apache.flink.streaming.connectors.kinesis.internals.publisher.polling;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.connectors.kinesis.internals.publisher.RecordBatch;
import org.apache.flink.streaming.connectors.kinesis.internals.publisher.RecordPublisher;
import org.apache.flink.streaming.connectors.kinesis.metrics.PollingRecordPublisherMetricsReporter;
import org.apache.flink.streaming.connectors.kinesis.model.SequenceNumber;
import org.apache.flink.streaming.connectors.kinesis.model.StartingPosition;
import org.apache.flink.streaming.connectors.kinesis.model.StreamShardHandle;
import org.apache.flink.streaming.connectors.kinesis.proxy.KinesisProxyInterface;
import org.apache.flink.util.Preconditions;

import com.amazonaws.services.kinesis.model.ExpiredIteratorException;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import static com.amazonaws.services.kinesis.model.ShardIteratorType.LATEST;
import static org.apache.flink.streaming.connectors.kinesis.internals.publisher.RecordPublisher.RecordPublisherRunResult.COMPLETE;
import static org.apache.flink.streaming.connectors.kinesis.internals.publisher.RecordPublisher.RecordPublisherRunResult.INCOMPLETE;

/**
 * A {@link RecordPublisher} that will read records from Kinesis and forward them to the subscriber.
 * Records are consumed by polling the GetRecords KDS API using a ShardIterator.
 */
@Internal
public class PollingRecordPublisher implements RecordPublisher {

	private static final Logger LOG = LoggerFactory.getLogger(PollingRecordPublisher.class);

	private final PollingRecordPublisherMetricsReporter metricsReporter;

	private final KinesisProxyInterface kinesisProxy;

	private final StreamShardHandle subscribedShard;

	private String nextShardItr;

	private StartingPosition nextStartingPosition;

	private final int maxNumberOfRecordsPerFetch;

	private final long expiredIteratorBackoffMillis;

	/**
	 * A Polling implementation of {@link RecordPublisher} that polls kinesis for records.
	 * The following KDS services are used: GetRecords and GetShardIterator.
	 *
	 * @param startingPosition the position in the stream to start consuming from
	 * @param subscribedShard the shard in which to consume from
	 * @param metricsReporter a metric reporter used to output metrics
	 * @param kinesisProxy the proxy used to communicate with kinesis
	 * @param maxNumberOfRecordsPerFetch the maximum number of records to retrieve per batch
	 * @param expiredIteratorBackoffMillis the duration to sleep in the event of an {@link ExpiredIteratorException}
	 */
	PollingRecordPublisher(
			final StartingPosition startingPosition,
			final StreamShardHandle subscribedShard,
			final PollingRecordPublisherMetricsReporter metricsReporter,
			final KinesisProxyInterface kinesisProxy,
			final int maxNumberOfRecordsPerFetch,
			final long expiredIteratorBackoffMillis) throws InterruptedException {
		this.nextStartingPosition = Preconditions.checkNotNull(startingPosition);
		this.subscribedShard = Preconditions.checkNotNull(subscribedShard);
		this.metricsReporter = Preconditions.checkNotNull(metricsReporter);
		this.kinesisProxy = Preconditions.checkNotNull(kinesisProxy);
		this.maxNumberOfRecordsPerFetch = maxNumberOfRecordsPerFetch;
		this.expiredIteratorBackoffMillis = expiredIteratorBackoffMillis;

		Preconditions.checkArgument(expiredIteratorBackoffMillis >= 0);
		Preconditions.checkArgument(maxNumberOfRecordsPerFetch > 0);

		this.nextShardItr = getShardIterator();
	}

	@Override
	public RecordPublisherRunResult run(final RecordBatchConsumer consumer) throws InterruptedException {
		return run(consumer, maxNumberOfRecordsPerFetch);
	}

	public RecordPublisherRunResult run(final RecordBatchConsumer consumer, int maxNumberOfRecords) throws InterruptedException {
		if (nextShardItr == null) {
			return COMPLETE;
		}

		metricsReporter.setMaxNumberOfRecordsPerFetch(maxNumberOfRecords);

		GetRecordsResult result = getRecords(nextShardItr, maxNumberOfRecords);

		RecordBatch recordBatch = new RecordBatch(result.getRecords(), subscribedShard, result.getMillisBehindLatest());
		SequenceNumber latestSeequenceNumber = consumer.accept(recordBatch);

		nextStartingPosition = StartingPosition.continueFromSequenceNumber(latestSeequenceNumber);
		nextShardItr = result.getNextShardIterator();
		return nextShardItr == null ? COMPLETE : INCOMPLETE;
	}

	/**
	 * Calls {@link KinesisProxyInterface#getRecords(String, int)}, while also handling unexpected
	 * AWS {@link ExpiredIteratorException}s to assure that we get results and don't just fail on
	 * such occasions. The returned shard iterator within the successful {@link GetRecordsResult} should
	 * be used for the next call to this method.
	 *
	 * <p>Note: it is important that this method is not called again before all the records from the last result have been
	 * fully collected with {@code ShardConsumer#deserializeRecordForCollectionAndUpdateState(UserRecord)}, otherwise
	 * {@code ShardConsumer#lastSequenceNum} may refer to a sub-record in the middle of an aggregated record, leading to
	 * incorrect shard iteration if the iterator had to be refreshed.
	 *
	 * @param shardItr shard iterator to use
	 * @param maxNumberOfRecords the maximum number of records to fetch for this getRecords attempt
	 * @return get records result
	 */
	private GetRecordsResult getRecords(String shardItr, int maxNumberOfRecords) throws InterruptedException {
		GetRecordsResult getRecordsResult = null;
		while (getRecordsResult == null) {
			try {
				getRecordsResult = kinesisProxy.getRecords(shardItr, maxNumberOfRecords);
			} catch (ExpiredIteratorException eiEx) {
				LOG.warn("Encountered an unexpected expired iterator {} for shard {};" +
					" refreshing the iterator ...", shardItr, subscribedShard);

				shardItr = getShardIterator();

				// sleep for the fetch interval before the next getRecords attempt with the refreshed iterator
				if (expiredIteratorBackoffMillis != 0) {
					Thread.sleep(expiredIteratorBackoffMillis);
				}
			}
		}
		return getRecordsResult;
	}

	/**
	 * Returns a shard iterator for the given {@link SequenceNumber}.
	 *
	 * @return shard iterator
	 */
	@Nullable
	private String getShardIterator() throws InterruptedException {
		if (nextStartingPosition.getShardIteratorType() == LATEST && subscribedShard.isClosed()) {
			return null;
		}

		return kinesisProxy.getShardIterator(
			subscribedShard,
			nextStartingPosition.getShardIteratorType().toString(),
			nextStartingPosition.getStartingMarker());
	}
}
