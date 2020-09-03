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
import org.apache.flink.util.Preconditions;

import com.amazonaws.services.kinesis.model.ExpiredIteratorException;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.HashKeyRange;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.SequenceNumberRange;
import com.amazonaws.services.kinesis.model.Shard;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.streaming.connectors.kinesis.testutils.TestUtils.createDummyStreamShardHandle;
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

	/**
	 * Creates a mocked Kinesis Proxy that will Emit aggregated records from a fake stream:
	 * - There will be {@code numOfGetRecordsCalls} batches available in the stream
	 * - Each batch will contain {@code numOfAggregatedRecords} aggregated records
	 * - Each aggregated record will contain {@code numOfChildRecords} child records
	 * Therefore this class will emit a total of
	 * {@code numOfGetRecordsCalls * numOfAggregatedRecords * numOfChildRecords} records.
	 *
	 * @param numOfAggregatedRecords the number of records per batch
	 * @param numOfChildRecords the number of child records in each aggregated record
	 * @param numOfGetRecordsCalls the number batches available in the fake stream
	 */
	public static KinesisProxyInterface aggregatedRecords(
		final int numOfAggregatedRecords,
		final int numOfChildRecords,
		final int numOfGetRecordsCalls) {
		return new SingleShardEmittingAggregatedRecordsKinesis(numOfAggregatedRecords, numOfChildRecords, numOfGetRecordsCalls);
	}

	public static KinesisProxyInterface blockingQueueGetRecords(Map<String, List<BlockingQueue<String>>> streamsToShardQueues) {
		return new BlockingQueueKinesis(streamsToShardQueues);
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
			if ((Integer.parseInt(shardIterator) == orderOfCallToExpire - 1) && !expiredOnceAlready) {
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
						(Integer.parseInt(shardIterator) == totalNumOfGetRecordsCalls - 1)
							? null : String.valueOf(Integer.parseInt(shardIterator) + 1)); // last next shard iterator is null
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
					(Integer.parseInt(shardIterator) == totalNumOfGetRecordsCalls - 1)
						? null : String.valueOf(Integer.parseInt(shardIterator) + 1)); // last next shard iterator is null
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

	private static class SingleShardEmittingAdaptiveNumOfRecordsKinesis extends SingleShardEmittingKinesis {

		protected static long averageRecordSizeBytes = 0L;

		private static final long KINESIS_SHARD_BYTES_PER_SECOND_LIMIT = 2 * 1024L * 1024L;

		public SingleShardEmittingAdaptiveNumOfRecordsKinesis(
				final int numOfRecords,
				final int numOfGetRecordsCalls,
				final long millisBehindLatest) {
			super(initShardItrToRecordBatch(numOfRecords, numOfGetRecordsCalls), millisBehindLatest);
		}

		private static Map<String, List<Record>> initShardItrToRecordBatch(
				final int numOfRecords,
				final int numOfGetRecordsCalls) {
			// initialize the record batches that we will be fetched
			Map<String, List<Record>> shardItrToRecordBatch = new HashMap<>();

			int numOfAlreadyPartitionedRecords = 0;
			int numOfRecordsPerBatch = numOfRecords;
			for (int batch = 0; batch < numOfGetRecordsCalls; batch++) {
				shardItrToRecordBatch.put(
					String.valueOf(batch),
					createRecordBatchWithRange(
						numOfAlreadyPartitionedRecords,
						numOfAlreadyPartitionedRecords + numOfRecordsPerBatch));
				numOfAlreadyPartitionedRecords += numOfRecordsPerBatch;

				numOfRecordsPerBatch = (int) (KINESIS_SHARD_BYTES_PER_SECOND_LIMIT /
					(averageRecordSizeBytes * 1000L / ConsumerConfigConstants.DEFAULT_SHARD_GETRECORDS_INTERVAL_MILLIS));
			}

			return shardItrToRecordBatch;
		}

		private static List<Record> createRecordBatchWithRange(int min, int max) {
			List<Record> batch = new LinkedList<>();
			long	sumRecordBatchBytes = 0L;
			// Create record of size 10Kb
			String data = createDataSize(10 * 1024L);

			for (int i = min; i < max; i++) {
				Record record = new Record()
								.withData(
										ByteBuffer.wrap(data.getBytes(ConfigConstants.DEFAULT_CHARSET)))
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

		private static String createDataSize(final long msgSize) {
			char[] data = new char[(int) msgSize];
			return new String(data);
		}
	}

	private static class SingleShardEmittingAggregatedRecordsKinesis extends SingleShardEmittingKinesis {

		public SingleShardEmittingAggregatedRecordsKinesis(
				final int numOfAggregatedRecords,
			final int numOfChildRecords,
			final int numOfGetRecordsCalls) {
			super(initShardItrToRecordBatch(numOfAggregatedRecords, numOfChildRecords, numOfGetRecordsCalls));
		}

		private static Map<String, List<Record>> initShardItrToRecordBatch(final int numOfAggregatedRecords,
			final int numOfChildRecords,
			final int numOfGetRecordsCalls) {

			Map<String, List<Record>> shardToRecordBatch = new HashMap<>();

			AtomicInteger sequenceNumber = new AtomicInteger();
			for (int batch = 0; batch < numOfGetRecordsCalls; batch++) {
				List<Record> recordBatch = TestUtils.createAggregatedRecordBatch(
					numOfAggregatedRecords, numOfChildRecords, sequenceNumber);

				shardToRecordBatch.put(String.valueOf(batch), recordBatch);
			}

			return shardToRecordBatch;
		}
	}

	/** A helper base class used to emit records from a single sharded fake Kinesis Stream. */
	private abstract static class SingleShardEmittingKinesis implements KinesisProxyInterface {

		private final long millisBehindLatest;

		private final Map<String, List<Record>> shardItrToRecordBatch;

		protected SingleShardEmittingKinesis(final Map<String, List<Record>> shardItrToRecordBatch) {
			this(shardItrToRecordBatch, 0L);
		}

		protected SingleShardEmittingKinesis(final Map<String, List<Record>> shardItrToRecordBatch, final long millisBehindLatest) {
			this.millisBehindLatest = millisBehindLatest;
			this.shardItrToRecordBatch = shardItrToRecordBatch;
		}

		@Override
		public GetRecordsResult getRecords(String shardIterator, int maxRecordsToGet) {
			int index = Integer.parseInt(shardIterator);
			// last next shard iterator is null
			String nextShardIterator = (index == shardItrToRecordBatch.size() - 1) ? null : String.valueOf(index + 1);

			// assuming that the maxRecordsToGet is always large enough
			return new GetRecordsResult()
				.withRecords(shardItrToRecordBatch.get(shardIterator))
				.withNextShardIterator(nextShardIterator)
				.withMillisBehindLatest(millisBehindLatest);
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
	}

	private static class NonReshardedStreamsKinesis implements KinesisProxyInterface {

		private final Map<String, List<StreamShardHandle>> streamsWithListOfShards = new HashMap<>();

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
							createDummyStreamShardHandle(streamName, KinesisShardIdGenerator.generateFromShardOrder(i)));
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
						if (compareShardIds(
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

		/**
		 * Utility function to compare two shard ids.
		 *
		 * @param firstShardId first shard id to compare
		 * @param secondShardId second shard id to compare
		 * @return a value less than 0 if the first shard id is smaller than the second shard id,
		 *         or a value larger than 0 the first shard is larger than the second shard id,
		 *         or 0 if they are equal
		 */
		private static int compareShardIds(String firstShardId, String secondShardId) {
			if (!isValidShardId(firstShardId)) {
				throw new IllegalArgumentException("The first shard id has invalid format.");
			}

			if (!isValidShardId(secondShardId)) {
				throw new IllegalArgumentException("The second shard id has invalid format.");
			}

			// digit segment of the shard id starts at index 8
			return Long.compare(Long.parseLong(firstShardId.substring(8)), Long.parseLong(secondShardId.substring(8)));
		}

		/**
		 * Checks if a shard id has valid format.
		 * Kinesis stream shard ids have 12-digit numbers left-padded with 0's,
		 * prefixed with "shardId-", ex. "shardId-000000000015".
		 *
		 * @param shardId the shard id to check
		 * @return whether the shard id is valid
		 */
		private static boolean isValidShardId(String shardId) {
			if (shardId == null) {
				return false;
			}
			return shardId.matches("^shardId-\\d{12}");
		}
	}

	private static class BlockingQueueKinesis implements KinesisProxyInterface {

		private final Map<String, List<StreamShardHandle>> streamsWithListOfShards = new HashMap<>();
		private final Map<String, BlockingQueue<String>> shardIteratorToQueueMap = new HashMap<>();

		private static String getShardIterator(StreamShardHandle shardHandle) {
			return shardHandle.getStreamName() + "-" + shardHandle.getShard().getShardId();
		}

		public BlockingQueueKinesis(Map<String, List<BlockingQueue<String>>> streamsToShardCount) {
			for (Map.Entry<String, List<BlockingQueue<String>>> streamToShardQueues : streamsToShardCount.entrySet()) {
				String streamName = streamToShardQueues.getKey();
				int shardCount = streamToShardQueues.getValue().size();

				if (shardCount == 0) {
					// don't do anything
				} else {
					List<StreamShardHandle> shardsOfStream = new ArrayList<>(shardCount);
					for (int i = 0; i < shardCount; i++) {
						StreamShardHandle shardHandle = new StreamShardHandle(
							streamName,
							new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(i))
								.withSequenceNumberRange(new SequenceNumberRange().withStartingSequenceNumber("0"))
								.withHashKeyRange(new HashKeyRange().withStartingHashKey("0").withEndingHashKey("0")));
						shardsOfStream.add(shardHandle);
						shardIteratorToQueueMap.put(getShardIterator(shardHandle), streamToShardQueues.getValue().get(i));
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
			return getShardIterator(shard);
		}

		@Override
		public GetRecordsResult getRecords(String shardIterator, int maxRecordsToGet) {
			BlockingQueue<String> queue = Preconditions.checkNotNull(this.shardIteratorToQueueMap.get(shardIterator),
			"no queue for iterator %s", shardIterator);
			List<Record> records = Collections.emptyList();
			try {
				String data = queue.take();
				Record record = new Record()
					.withData(
						ByteBuffer.wrap(data.getBytes(ConfigConstants.DEFAULT_CHARSET)))
					.withPartitionKey(UUID.randomUUID().toString())
					.withApproximateArrivalTimestamp(new Date(System.currentTimeMillis()))
					.withSequenceNumber(String.valueOf(0));
				records = Collections.singletonList(record);
			} catch (InterruptedException e) {
				shardIterator = null;
			}
			return new GetRecordsResult()
				.withRecords(records)
				.withMillisBehindLatest(0L)
				.withNextShardIterator(shardIterator);
		}
	}

}
