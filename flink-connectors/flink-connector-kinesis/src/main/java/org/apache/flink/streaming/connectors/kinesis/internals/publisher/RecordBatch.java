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

package org.apache.flink.streaming.connectors.kinesis.internals.publisher;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.connectors.kinesis.model.StreamShardHandle;
import org.apache.flink.util.Preconditions;

import com.amazonaws.services.kinesis.clientlibrary.types.UserRecord;
import com.amazonaws.services.kinesis.model.Record;

import javax.annotation.Nullable;

import java.math.BigInteger;
import java.util.List;

/**
 * A batch of UserRecords received from Kinesis.
 * Input records are de-aggregated using KCL 1.x library.
 * It is expected that AWS SDK v2.x messages are converted to KCL 1.x {@link UserRecord}.
 */
@Internal
public class RecordBatch {

	private final int aggregatedRecordSize;

	private final List<UserRecord> deaggregatedRecords;

	private final long totalSizeInBytes;

	private final Long millisBehindLatest;

	public RecordBatch(
			final List<Record> records,
			final StreamShardHandle subscribedShard,
			@Nullable final Long millisBehindLatest) {
		Preconditions.checkNotNull(subscribedShard);
		this.aggregatedRecordSize = Preconditions.checkNotNull(records).size();
		this.deaggregatedRecords = deaggregateRecords(records, subscribedShard);
		this.totalSizeInBytes = this.deaggregatedRecords.stream().mapToInt(r -> r.getData().remaining()).sum();
		this.millisBehindLatest = millisBehindLatest;
	}

	public int getAggregatedRecordSize() {
		return aggregatedRecordSize;
	}

	public int getDeaggregatedRecordSize() {
		return deaggregatedRecords.size();
	}

	public List<UserRecord> getDeaggregatedRecords() {
		return deaggregatedRecords;
	}

	public long getTotalSizeInBytes() {
		return totalSizeInBytes;
	}

	public long getAverageRecordSizeBytes() {
		return deaggregatedRecords.isEmpty() ? 0 : getTotalSizeInBytes() / getDeaggregatedRecordSize();
	}

	@Nullable
	public Long getMillisBehindLatest() {
		return millisBehindLatest;
	}

	private List<UserRecord> deaggregateRecords(final List<Record> records, final StreamShardHandle subscribedShard) {
		BigInteger start = new BigInteger(subscribedShard.getShard().getHashKeyRange().getStartingHashKey());
		BigInteger end = new BigInteger(subscribedShard.getShard().getHashKeyRange().getEndingHashKey());

		return UserRecord.deaggregate(records, start, end);
	}
}
