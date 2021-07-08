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

package org.apache.flink.streaming.connectors.kinesis.model;

import org.apache.flink.annotation.Internal;

import com.amazonaws.services.kinesis.model.ShardIteratorType;

import javax.annotation.Nullable;

import java.util.Date;

import static com.amazonaws.services.kinesis.model.ShardIteratorType.AFTER_SEQUENCE_NUMBER;
import static com.amazonaws.services.kinesis.model.ShardIteratorType.AT_SEQUENCE_NUMBER;
import static com.amazonaws.services.kinesis.model.ShardIteratorType.AT_TIMESTAMP;
import static com.amazonaws.services.kinesis.model.ShardIteratorType.LATEST;
import static com.amazonaws.services.kinesis.model.ShardIteratorType.TRIM_HORIZON;
import static org.apache.flink.streaming.connectors.kinesis.model.SentinelSequenceNumber.isSentinelSequenceNumber;

/** The position in which to start consuming from a stream. */
@Internal
public class StartingPosition {

    private final ShardIteratorType shardIteratorType;

    private final Object startingMarker;

    private StartingPosition(
            final ShardIteratorType shardIteratorType, @Nullable final Object startingMarker) {
        this.shardIteratorType = shardIteratorType;
        this.startingMarker = startingMarker;
    }

    public ShardIteratorType getShardIteratorType() {
        return shardIteratorType;
    }

    @Nullable
    public Object getStartingMarker() {
        return startingMarker;
    }

    public static StartingPosition fromTimestamp(final Date date) {
        return new StartingPosition(AT_TIMESTAMP, date);
    }

    /**
     * Returns the starting position for the next record to consume from the given sequence number.
     * The difference between {@code restartFromSequenceNumber()} and {@code
     * continueFromSequenceNumber()} is that for {@code restartFromSequenceNumber()} aggregated
     * records are reread to support subsequence failure.
     *
     * @param sequenceNumber the last successful sequence number, or sentinel marker
     * @return the start position in which to consume from
     */
    public static StartingPosition continueFromSequenceNumber(final SequenceNumber sequenceNumber) {
        return fromSequenceNumber(sequenceNumber, false);
    }

    /**
     * Returns the starting position to restart record consumption from the given sequence number
     * after failure. The difference between {@code restartFromSequenceNumber()} and {@code
     * continueFromSequenceNumber()} is that for {@code restartFromSequenceNumber()} aggregated
     * records are reread to support subsequence failure.
     *
     * @param sequenceNumber the last successful sequence number, or sentinel marker
     * @return the start position in which to consume from
     */
    public static StartingPosition restartFromSequenceNumber(final SequenceNumber sequenceNumber) {
        return fromSequenceNumber(sequenceNumber, true);
    }

    private static StartingPosition fromSequenceNumber(
            final SequenceNumber sequenceNumber, final boolean restart) {
        if (isSentinelSequenceNumber(sequenceNumber)) {
            return new StartingPosition(fromSentinelSequenceNumber(sequenceNumber), null);
        } else {
            // we will be starting from an actual sequence number (due to restore from failure).
            return new StartingPosition(
                    getShardIteratorType(sequenceNumber, restart),
                    sequenceNumber.getSequenceNumber());
        }
    }

    private static ShardIteratorType getShardIteratorType(
            final SequenceNumber sequenceNumber, final boolean restart) {
        return restart && sequenceNumber.isAggregated()
                ? AT_SEQUENCE_NUMBER
                : AFTER_SEQUENCE_NUMBER;
    }

    private static ShardIteratorType fromSentinelSequenceNumber(
            final SequenceNumber sequenceNumber) {
        if (sequenceNumber.equals(SentinelSequenceNumber.SENTINEL_LATEST_SEQUENCE_NUM.get())) {
            return LATEST;
        } else if (sequenceNumber.equals(
                SentinelSequenceNumber.SENTINEL_EARLIEST_SEQUENCE_NUM.get())) {
            return TRIM_HORIZON;
        } else {
            throw new IllegalArgumentException("Unexpected sentinel type: " + sequenceNumber);
        }
    }
}
