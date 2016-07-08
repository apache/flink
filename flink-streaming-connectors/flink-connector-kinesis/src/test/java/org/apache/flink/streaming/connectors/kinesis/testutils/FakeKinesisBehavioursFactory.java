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

import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.Shard;
import org.apache.flink.streaming.connectors.kinesis.model.KinesisStreamShard;
import org.apache.flink.streaming.connectors.kinesis.proxy.GetShardListResult;
import org.apache.flink.streaming.connectors.kinesis.proxy.KinesisProxyInterface;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Factory for different kinds of fake Kinesis behaviours using the {@link KinesisProxyInterface} interface.
 */
public class FakeKinesisBehavioursFactory {

	// ------------------------------------------------------------------------
	//  Behaviours related to shard listing and resharding, used in ShardDiscovererTest
	// ------------------------------------------------------------------------

	public static KinesisProxyInterface noShardsFoundForRequestedStreamsBehaviour() {

		return new KinesisProxyInterface() {
			@Override
			public GetShardListResult getShardList(Map<String, String> streamNamesWithLastSeenShardIds) {
				return new GetShardListResult(); // not setting any retrieved shards for result
			}

			@Override
			public String getShardIterator(KinesisStreamShard shard, String shardIteratorType, String startingSeqNum) {
				return null;
			}

			@Override
			public GetRecordsResult getRecords(String shardIterator, int maxRecordsToGet) {
				return null;
			}
		};

	}

	public static KinesisProxyInterface nonReshardedStreamsBehaviour(Map<String,Integer> streamsToShardCount) {
		return new NonReshardedStreamsKinesis(streamsToShardCount);

	}

	// ------------------------------------------------------------------------
	//  Behaviours related to fetching records, used mainly in ShardConsumerTest
	// ------------------------------------------------------------------------

	public static KinesisProxyInterface totalNumOfRecordsAfterNumOfGetRecordsCalls(final int numOfRecords, final int numOfGetRecordsCalls) {
		return new SingleShardEmittingFixNumOfRecordsKinesis(numOfRecords, numOfGetRecordsCalls);
	}

	private static class SingleShardEmittingFixNumOfRecordsKinesis implements KinesisProxyInterface {

		private final int totalNumOfGetRecordsCalls;

		private final int totalNumOfRecords;

		private final Map<String,List<Record>> shardItrToRecordBatch;

		public SingleShardEmittingFixNumOfRecordsKinesis(final int numOfRecords, final int numOfGetRecordsCalls) {
			this.totalNumOfRecords = numOfRecords;
			this.totalNumOfGetRecordsCalls = numOfGetRecordsCalls;

			// initialize the record batches that we will be fetched
			this.shardItrToRecordBatch = new HashMap<>();

			int numOfAlreadyPartitionedRecords = 0;
			int numOfRecordsPerBatch = numOfRecords/numOfGetRecordsCalls + 1;
			for (int batch=0; batch<totalNumOfGetRecordsCalls; batch++) {
				if (batch != totalNumOfGetRecordsCalls-1) {
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
				.withNextShardIterator(
					(Integer.valueOf(shardIterator) == totalNumOfGetRecordsCalls-1)
						? null : String.valueOf(Integer.valueOf(shardIterator)+1)); // last next shard iterator is null
		}

		@Override
		public String getShardIterator(KinesisStreamShard shard, String shardIteratorType, String startingSeqNum) {
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
						.withData(ByteBuffer.wrap(String.valueOf(i).getBytes()))
						.withPartitionKey(UUID.randomUUID().toString())
						.withApproximateArrivalTimestamp(new Date(System.currentTimeMillis()))
						.withSequenceNumber(String.valueOf(i)));
			}
			return batch;
		}

	}

	private static class NonReshardedStreamsKinesis implements KinesisProxyInterface {

		private Map<String, List<KinesisStreamShard>> streamsWithListOfShards = new HashMap<>();

		public NonReshardedStreamsKinesis(Map<String,Integer> streamsToShardCount) {
			for (Map.Entry<String,Integer> streamToShardCount : streamsToShardCount.entrySet()) {
				String streamName = streamToShardCount.getKey();
				int shardCount = streamToShardCount.getValue();

				if (shardCount == 0) {
					// don't do anything
				} else {
					List<KinesisStreamShard> shardsOfStream = new ArrayList<>(shardCount);
					for (int i=0; i < shardCount; i++) {
						shardsOfStream.add(
							new KinesisStreamShard(
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
			for (Map.Entry<String, List<KinesisStreamShard>> streamsWithShards : streamsWithListOfShards.entrySet()) {
				String streamName = streamsWithShards.getKey();
				for (KinesisStreamShard shard : streamsWithShards.getValue()) {
					if (streamNamesWithLastSeenShardIds.get(streamName) == null) {
						result.addRetrievedShardToStream(streamName, shard);
					} else {
						if (KinesisStreamShard.compareShardIds(
							shard.getShard().getShardId(), streamNamesWithLastSeenShardIds.get(streamName)) > 0) {
							result.addRetrievedShardToStream(streamName, shard);
						}
					}
				}
			}
			return result;
		}

		@Override
		public String getShardIterator(KinesisStreamShard shard, String shardIteratorType, String startingSeqNum) {
			return null;
		}

		@Override
		public GetRecordsResult getRecords(String shardIterator, int maxRecordsToGet) {
			return null;
		}
	}
}
