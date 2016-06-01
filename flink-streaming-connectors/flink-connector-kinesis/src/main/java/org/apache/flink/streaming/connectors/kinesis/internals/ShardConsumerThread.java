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

package org.apache.flink.streaming.connectors.kinesis.internals;

import com.amazonaws.services.kinesis.clientlibrary.types.UserRecord;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kinesis.config.KinesisConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.model.KinesisStreamShard;
import org.apache.flink.streaming.connectors.kinesis.model.SentinelSequenceNumber;
import org.apache.flink.streaming.connectors.kinesis.proxy.KinesisProxy;
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisDeserializationSchema;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Thread that does the actual data pulling from AWS Kinesis shards. Each thread is in charge of one Kinesis shard only.
 */
public class ShardConsumerThread<T> extends Thread {
	private final SourceFunction.SourceContext<T> sourceContext;
	private final KinesisDeserializationSchema<T> deserializer;
	private final HashMap<KinesisStreamShard, String> seqNoState;

	private final KinesisProxy kinesisProxy;

	private final KinesisDataFetcher ownerRef;

	private final KinesisStreamShard assignedShard;

	private final int maxNumberOfRecordsPerFetch;

	private String lastSequenceNum;
	private String nextShardItr;

	private volatile boolean running = true;

	public ShardConsumerThread(KinesisDataFetcher ownerRef,
							Properties props,
							KinesisStreamShard assignedShard,
							String lastSequenceNum,
							SourceFunction.SourceContext<T> sourceContext,
							KinesisDeserializationSchema<T> deserializer,
							HashMap<KinesisStreamShard, String> seqNumState) {
		this.ownerRef = checkNotNull(ownerRef);
		this.assignedShard = checkNotNull(assignedShard);
		this.lastSequenceNum = checkNotNull(lastSequenceNum);
		this.sourceContext = checkNotNull(sourceContext);
		this.deserializer = checkNotNull(deserializer);
		this.seqNoState = checkNotNull(seqNumState);
		this.kinesisProxy = new KinesisProxy(props);
		this.maxNumberOfRecordsPerFetch = Integer.valueOf(props.getProperty(
			KinesisConfigConstants.CONFIG_SHARD_RECORDS_PER_GET,
			Integer.toString(KinesisConfigConstants.DEFAULT_SHARD_RECORDS_PER_GET)));
	}

	@SuppressWarnings("unchecked")
	@Override
	public void run() {
		try {
			if (lastSequenceNum.equals(SentinelSequenceNumber.SENTINEL_LATEST_SEQUENCE_NUM.toString())) {
				// if the shard is already closed, there will be no latest next record to get for this shard
				if (assignedShard.isClosed()) {
					nextShardItr = null;
				} else {
					nextShardItr = kinesisProxy.getShardIterator(assignedShard, ShardIteratorType.LATEST.toString(), null);
				}
			} else if (lastSequenceNum.equals(SentinelSequenceNumber.SENTINEL_EARLIEST_SEQUENCE_NUM.toString())) {
				nextShardItr = kinesisProxy.getShardIterator(assignedShard, ShardIteratorType.TRIM_HORIZON.toString(), null);
			} else if (lastSequenceNum.equals(SentinelSequenceNumber.SENTINEL_SHARD_ENDING_SEQUENCE_NUM.toString())) {
				nextShardItr = null;
			} else {
				nextShardItr = kinesisProxy.getShardIterator(assignedShard, ShardIteratorType.AFTER_SEQUENCE_NUMBER.toString(), lastSequenceNum);
			}

			while(running) {
				if (nextShardItr == null) {
					lastSequenceNum = SentinelSequenceNumber.SENTINEL_SHARD_ENDING_SEQUENCE_NUM.toString();

					synchronized (sourceContext.getCheckpointLock()) {
						seqNoState.put(assignedShard, lastSequenceNum);
					}

					break;
				} else {
					GetRecordsResult getRecordsResult = kinesisProxy.getRecords(nextShardItr, maxNumberOfRecordsPerFetch);

					List<Record> fetchedRecords = getRecordsResult.getRecords();

					// each of the Kinesis records may be aggregated, so we must deaggregate them before proceeding
					fetchedRecords = deaggregateRecords(fetchedRecords, assignedShard.getStartingHashKey(), assignedShard.getEndingHashKey());

					for (Record record : fetchedRecords) {
						ByteBuffer recordData = record.getData();

						byte[] dataBytes = new byte[recordData.remaining()];
						recordData.get(dataBytes);

						byte[] keyBytes = record.getPartitionKey().getBytes();

						final T value = deserializer.deserialize(keyBytes, dataBytes,assignedShard.getStreamName(),
							record.getSequenceNumber());

						synchronized (sourceContext.getCheckpointLock()) {
							sourceContext.collect(value);
							seqNoState.put(assignedShard, record.getSequenceNumber());
						}

						lastSequenceNum = record.getSequenceNumber();
					}

					nextShardItr = getRecordsResult.getNextShardIterator();
				}
			}
		} catch (Throwable t) {
			ownerRef.stopWithError(t);
		}
	}

	public void cancel() {
		this.running = false;
		this.interrupt();
	}

	@SuppressWarnings("unchecked")
	protected static List<Record> deaggregateRecords(List<Record> records, String startingHashKey, String endingHashKey) {
		return (List<Record>) (List<?>) UserRecord.deaggregate(records, new BigInteger(startingHashKey), new BigInteger(endingHashKey));
	}
}
