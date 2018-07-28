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

package org.apache.flink.streaming.connectors.kinesis.testutils;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.model.StreamShardHandle;
import org.apache.flink.streaming.connectors.kinesis.proxy.GetShardListResult;
import org.apache.flink.streaming.connectors.kinesis.proxy.KinesisProxyInterface;

import com.amazonaws.services.kinesis.model.ExpiredIteratorException;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.Shard;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Factory for different kinds of fake Kinesis behaviours using the {@link KinesisProxyInterface} interface.
 */
public class FakeKinesisBehavioursFactory {

	// ------------------------------------------------------------------------
	//  Behaviours related to shard listing and resharding, used in KinesisDataFetcherTest
	// ------------------------------------------------------------------------

	public static KinesisProxyInterface noShardsFoundForRequestedStreamsBehaviour() {

		return new KinesisProxyInterface() {
			@Override
			public GetShardListResult getShardList(Map<String, String> streamNamesWithLastSeenShardIds) {
				return new GetShardListResult(); // not setting any retrieved shards for result
			}

			@Override
			public String getShardIterator(StreamShardHandle shard, String shardIteratorType, Object startingMarker) {
				return null;
			}

			@Override
			public GetRecordsResult getRecords(String shardIterator, int maxRecordsToGet) {
				return null;
			}
		};

	}

	public static KinesisProxyInterface nonReshardedStreamsBehaviour(Map<String, Integer> streamsToShardCount) {
		return new NonReshardedStreamsKinesis(streamsToShardCount);

	}

	// ------------------------------------------------------------------------
	//  Behaviours related to fetching records, used mainly in ShardConsumerTest
	// ------------------------------------------------------------------------

	public static KinesisProxyInterface totalNumOfRecordsAfterNumOfGetRecordsCalls(
			final int numOfRecords,
			final int numOfGetRecordsCalls,
			final long millisBehindLatest) {
		return new SingleShardEmittingFixNumOfRecordsKinesis(numOfRecords, numOfGetRecordsCalls, millisBehindLatest);
	}

	public static KinesisProxyInterface totalNumOfRecordsAfterNumOfGetRecordsCallsWithUnexpectedExpiredIterator(
			final int numOfRecords,
			final int numOfGetRecordsCall,
			final int orderOfCallToExpire,
			final long millisBehindLatest) {
		return new SingleShardEmittingFixNumOfRecordsWithExpiredIteratorKinesis(
			numOfRecords, numOfGetRecordsCall, orderOfCallToExpire, millisBehindLatest);
	}

	public static KinesisProxyInterface initialNumOfRecordsAfterNumOfGetRecordsCallsWithAdaptiveReads(
			final int numOfRecords,
			final int numOfGetRecordsCalls,
			final long millisBehindLatest) {
		return new SingleShardEmittingAdaptiveNumOfRecordsKinesis(numOfRecords, numOfGetRecordsCalls,
				millisBehindLatest);
	}

	private static class SingleShardEmittingFixNumOfRecordsWithExpiredIteratorKinesis extends SingleShardEmittingFixNumOfRecordsKinesis {

		private final long millisBehindLatest;
		private final int orderOfCallToExpire;

		private boolean expiredOnceAlready = false;
		private boolean expiredIteratorRefreshed = false;

		public SingleShardEmittingFixNumOfRecordsWithExpiredIteratorKinesis(
				final int numOfRecords,
				final int numOfGetRecordsCalls,
				final int orderOfCallToExpire,
				final long millisBehindLatest) {
			super(numOfRecords, numOfGetRecordsCalls, millisBehindLatest);
			checkArgument(orderOfCallToExpire <= numOfGetRecordsCalls,
				"can not test unexpected expired iterator if orderOfCallToExpire is larger than numOfGetRecordsCalls");
			this.millisBehindLatest = millisBehindLatest;
			this.orderOfCallToExpire = orderOfCallToExpire;
		}

		@Override
		public GetRecordsResult getRecords(String shardIterator, int maxRecordsToGet) {
			if ((Integer.valueOf(shardIterator) == orderOfCallToExpire - 1) && !expiredOnceAlready) {
				// we fake only once the expired iterator exception at the specified get records attempt order
				expiredOnceAlready = true;
				throw new ExpiredIteratorException("Artificial expired shard iterator");
			} else if (expiredOnceAlready && !expiredIteratorRefreshed) {
				// if we've thrown the expired iterator exception already, but the iterator was not refreshed,
				// throw a hard exception to the test that is testing this Kinesis behaviour
				throw new RuntimeException("expired shard iterator was not refreshed on the next getRecords() call");
			} else {
				// assuming that the maxRecordsToGet is always large enough
				return new GetRecordsResult()
					.withRecords(shardItrToRecordBatch.get(shardIterator))
					.withMillisBehindLatest(millisBehindLatest)
					.withNextShardIterator(
						(Integer.valueOf(shardIterator) == totalNumOfGetRecordsCalls - 1)
							? null : String.valueOf(Integer.valueOf(shardIterator) + 1)); // last next shard iterator is null
			}
		}

		@Override
		public String getShardIterator(StreamShardHandle shard, String shardIteratorType, Object startingMarker) {
			if (!expiredOnceAlready) {
				// for the first call, just return the iterator of the first batch of records
				return "0";
			} else {
				// fake the iterator refresh when this is called again after getRecords throws expired iterator
				// exception on the orderOfCallToExpire attempt
				expiredIteratorRefreshed = true;
				return String.valueOf(orderOfCallToExpire - 1);
			}
		}
	}

	private static class SingleShardEmittingFixNumOfRecordsKinesis implements KinesisProxyInterface {

		protected final int totalNumOfGetRecordsCalls;

		protected final int totalNumOfRecords;

		private final long millisBehindLatest;

		protected final Map<String, List<Record>> shardItrToRecordBatch;

		public SingleShardEmittingFixNumOfRecordsKinesis(
				final int numOfRecords,
				final int numOfGetRecordsCalls,
				final long millistBehindLatest) {
			this.totalNumOfRecords = numOfRecords;
			this.totalNumOfGetRecordsCalls = numOfGetRecordsCalls;
			this.millisBehindLatest = millistBehindLatest;

			// initialize the record batches that we will be fetched
			this.shardItrToRecordBatch = new HashMap<>();

			int numOfAlreadyPartitionedRecords = 0;
			int numOfRecordsPerBatch = numOfRecords / numOfGetRecordsCalls + 1;
			for (int batch = 0; batch < totalNumOfGetRecordsCalls; batch++) {
				if (batch != totalNumOfGetRecordsCalls - 1) {
					shardItrToRecordBatch.put(
						String.valueOf(batch),
						createRecordBatchWithRange(
							numOfAlreadyPartitionedRecords,
							numOfAlreadyPartitionedRecords + numOfRecordsPerBatch));
					numOfAlreadyPartitionedRecords += numOfRecordsPerBatch;
				} else {
					shardItrToRecordBatch.put(
						String.valueOf(batch),
						createRecordBatchWithRange(
							numOfAlreadyPartitionedRecords,
							totalNumOfRecords));
				}
			}
		}

		@Override
		public GetRecordsResult getRecords(String shardIterator, int maxRecordsToGet) {
			// assuming that the maxRecordsToGet is always large enough
			return new GetRecordsResult()
				.withRecords(shardItrToRecordBatch.get(shardIterator))
				.withMillisBehindLatest(millisBehindLatest)
				.withNextShardIterator(
					(Integer.valueOf(shardIterator) == totalNumOfGetRecordsCalls - 1)
						? null : String.valueOf(Integer.valueOf(shardIterator) + 1)); // last next shard iterator is null
		}

		@Override
		public String getShardIterator(StreamShardHandle shard, String shardIteratorType, Object startingMarker) {
			// this will be called only one time per ShardConsumer;
			// so, simply return the iterator of the first batch of records
			return "0";
		}

		@Override
		public GetShardListResult getShardList(Map<String, String> streamNamesWithLastSeenShardIds) {
			return null;
		}

		public static List<Record> createRecordBatchWithRange(int min, int max) {
			List<Record> batch = new LinkedList<>();
			for (int i = min; i < max; i++) {
				batch.add(
					new Record()
						.withData(ByteBuffer.wrap(String.valueOf(i).getBytes(ConfigConstants.DEFAULT_CHARSET)))
						.withPartitionKey(UUID.randomUUID().toString())
						.withApproximateArrivalTimestamp(new Date(System.currentTimeMillis()))
						.withSequenceNumber(String.valueOf(i)));
			}
			return batch;
		}

	}

	private static class SingleShardEmittingAdaptiveNumOfRecordsKinesis implements
			KinesisProxyInterface {

		protected final int totalNumOfGetRecordsCalls;

		protected final int totalNumOfRecords;

		private final long millisBehindLatest;

		protected final Map<String, List<Record>> shardItrToRecordBatch;

		protected static long averageRecordSizeBytes;

		private static final long KINESIS_SHARD_BYTES_PER_SECOND_LIMIT = 2 * 1024L * 1024L;

		public SingleShardEmittingAdaptiveNumOfRecordsKinesis(final int numOfRecords,
				final int numOfGetRecordsCalls,
				final long millisBehindLatest) {
			this.totalNumOfRecords = numOfRecords;
			this.totalNumOfGetRecordsCalls = numOfGetRecordsCalls;
			this.millisBehindLatest = millisBehindLatest;
			this.averageRecordSizeBytes = 0L;

			// initialize the record batches that we will be fetched
			this.shardItrToRecordBatch = new HashMap<>();

			int numOfAlreadyPartitionedRecords = 0;
			int numOfRecordsPerBatch = numOfRecords;
			for (int batch = 0; batch < totalNumOfGetRecordsCalls; batch++) {
					shardItrToRecordBatch.put(
							String.valueOf(batch),
							createRecordBatchWithRange(
									numOfAlreadyPartitionedRecords,
									numOfAlreadyPartitionedRecords + numOfRecordsPerBatch));
					numOfAlreadyPartitionedRecords += numOfRecordsPerBatch;

				numOfRecordsPerBatch = (int) (KINESIS_SHARD_BYTES_PER_SECOND_LIMIT /
						(averageRecordSizeBytes * 1000L / ConsumerConfigConstants.DEFAULT_SHARD_GETRECORDS_INTERVAL_MILLIS));
			}
		}

		@Override
		public GetRecordsResult getRecords(String shardIterator, int maxRecordsToGet) {
			// assuming that the maxRecordsToGet is always large enough
			return new GetRecordsResult()
					.withRecords(shardItrToRecordBatch.get(shardIterator))
					.withMillisBehindLatest(millisBehindLatest)
					.withNextShardIterator(
							(Integer.valueOf(shardIterator) == totalNumOfGetRecordsCalls - 1)
									? null : String
									.valueOf(Integer.valueOf(shardIterator) + 1)); // last next shard iterator is null
		}

		@Override
		public String getShardIterator(StreamShardHandle shard, String shardIteratorType,
				Object startingMarker) {
			// this will be called only one time per ShardConsumer;
			// so, simply return the iterator of the first batch of records
			return "0";
		}

		@Override
		public GetShardListResult getShardList(Map<String, String> streamNamesWithLastSeenShardIds) {
			return null;
		}

		public static List<Record> createRecordBatchWithRange(int min, int max) {
			List<Record> batch = new LinkedList<>();
			long	sumRecordBatchBytes = 0L;
			// Create record of size 10Kb
			String data = createDataSize(10 * 1024L);

			for (int i = min; i < max; i++) {
				Record record = new Record()
								.withData(
										ByteBuffer.wrap(String.valueOf(data).getBytes(ConfigConstants.DEFAULT_CHARSET)))
								.withPartitionKey(UUID.randomUUID().toString())
								.withApproximateArrivalTimestamp(new Date(System.currentTimeMillis()))
								.withSequenceNumber(String.valueOf(i));
				batch.add(record);
				sumRecordBatchBytes += record.getData().remaining();

			}
			if (batch.size() != 0) {
				averageRecordSizeBytes = sumRecordBatchBytes / batch.size();
			}

			return batch;
		}

		private static String createDataSize(long msgSize) {
			char[] data = new char[(int) msgSize];
			return new String(data);

		}

	}

	private static class NonReshardedStreamsKinesis implements KinesisProxyInterface {

		private Map<String, List<StreamShardHandle>> streamsWithListOfShards = new HashMap<>();

		public NonReshardedStreamsKinesis(Map<String, Integer> streamsToShardCount) {
			for (Map.Entry<String, Integer> streamToShardCount : streamsToShardCount.entrySet()) {
				String streamName = streamToShardCount.getKey();
				int shardCount = streamToShardCount.getValue();

				if (shardCount == 0) {
					// don't do anything
				} else {
					List<StreamShardHandle> shardsOfStream = new ArrayList<>(shardCount);
					for (int i = 0; i < shardCount; i++) {
						shardsOfStream.add(
							new StreamShardHandle(
								streamName,
								new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(i))));
					}
					streamsWithListOfShards.put(streamName, shardsOfStream);
				}
			}
		}

		@Override
		public GetShardListResult getShardList(Map<String, String> streamNamesWithLastSeenShardIds) {
			GetShardListResult result = new GetShardListResult();
			for (Map.Entry<String, List<StreamShardHandle>> streamsWithShards : streamsWithListOfShards.entrySet()) {
				String streamName = streamsWithShards.getKey();
				for (StreamShardHandle shard : streamsWithShards.getValue()) {
					if (streamNamesWithLastSeenShardIds.get(streamName) == null) {
						result.addRetrievedShardToStream(streamName, shard);
					} else {
						if (StreamShardHandle.compareShardIds(
							shard.getShard().getShardId(), streamNamesWithLastSeenShardIds.get(streamName)) > 0) {
							result.addRetrievedShardToStream(streamName, shard);
						}
					}
				}
			}
			return result;
		}

		@Override
		public String getShardIterator(StreamShardHandle shard, String shardIteratorType, Object startingMarker) {
			return null;
		}

		@Override
		public GetRecordsResult getRecords(String shardIterator, int maxRecordsToGet) {
			return null;
		}
	}
}
