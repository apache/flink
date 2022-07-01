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

import com.amazonaws.services.kinesis.model.ShardIteratorType;
import org.junit.jupiter.api.Test;

import java.util.Date;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link StartingPosition}. */
public class StartingPositionTest {

    @Test
    public void testStartingPositionFromTimestamp() {
        Date date = new Date();
        StartingPosition position = StartingPosition.fromTimestamp(date);
        assertThat(position.getShardIteratorType()).isEqualTo(ShardIteratorType.AT_TIMESTAMP);
        assertThat(position.getStartingMarker()).isEqualTo(date);
    }

    @Test
    public void testStartingPositionRestartFromSequenceNumber() {
        SequenceNumber sequenceNumber = new SequenceNumber("100");
        StartingPosition position = StartingPosition.restartFromSequenceNumber(sequenceNumber);
        assertThat(position.getShardIteratorType())
                .isEqualTo(ShardIteratorType.AFTER_SEQUENCE_NUMBER);
        assertThat(position.getStartingMarker()).isEqualTo("100");
    }

    @Test
    public void testStartingPositionRestartFromAggregatedSequenceNumber() {
        SequenceNumber sequenceNumber = new SequenceNumber("200", 3);
        StartingPosition position = StartingPosition.restartFromSequenceNumber(sequenceNumber);
        assertThat(position.getShardIteratorType()).isEqualTo(ShardIteratorType.AT_SEQUENCE_NUMBER);
        assertThat(position.getStartingMarker()).isEqualTo("200");
    }

    @Test
    public void testStartingPositionContinueFromAggregatedSequenceNumber() {
        SequenceNumber sequenceNumber = new SequenceNumber("200", 3);
        StartingPosition position = StartingPosition.continueFromSequenceNumber(sequenceNumber);
        assertThat(position.getShardIteratorType())
                .isEqualTo(ShardIteratorType.AFTER_SEQUENCE_NUMBER);
        assertThat(position.getStartingMarker()).isEqualTo("200");
    }

    @Test
    public void testStartingPositionRestartFromSentinelEarliest() {
        SequenceNumber sequenceNumber = SentinelSequenceNumber.SENTINEL_EARLIEST_SEQUENCE_NUM.get();
        StartingPosition position = StartingPosition.restartFromSequenceNumber(sequenceNumber);
        assertThat(position.getShardIteratorType()).isEqualTo(ShardIteratorType.TRIM_HORIZON);
        assertThat(position.getStartingMarker()).isNull();
    }

    @Test
    public void testStartingPositionRestartFromSentinelLatest() {
        SequenceNumber sequenceNumber = SentinelSequenceNumber.SENTINEL_LATEST_SEQUENCE_NUM.get();
        StartingPosition position = StartingPosition.restartFromSequenceNumber(sequenceNumber);
        assertThat(position.getShardIteratorType()).isEqualTo(ShardIteratorType.LATEST);
        assertThat(position.getStartingMarker()).isNull();
    }

    @Test
    public void testStartingPositionRestartFromSentinelEnding() {
        assertThatThrownBy(
                        () -> {
                            SequenceNumber sequenceNumber =
                                    SentinelSequenceNumber.SENTINEL_SHARD_ENDING_SEQUENCE_NUM.get();
                            StartingPosition position =
                                    StartingPosition.restartFromSequenceNumber(sequenceNumber);
                            assertThat(position.getShardIteratorType())
                                    .isEqualTo(ShardIteratorType.LATEST);
                            assertThat(position.getStartingMarker()).isNull();
                        })
                .isInstanceOf(IllegalArgumentException.class);
    }
}
