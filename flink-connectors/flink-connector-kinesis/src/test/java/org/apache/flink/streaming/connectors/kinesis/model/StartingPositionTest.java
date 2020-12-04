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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Date;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Tests for {@link StartingPosition}.
 */
public class StartingPositionTest {

	@Rule
	public final ExpectedException thrown = ExpectedException.none();

	@Test
	public void testStartingPositionFromTimestamp() {
		Date date = new Date();
		StartingPosition position = StartingPosition.fromTimestamp(date);
		assertEquals(ShardIteratorType.AT_TIMESTAMP, position.getShardIteratorType());
		assertEquals(date, position.getStartingMarker());
	}

	@Test
	public void testStartingPositionRestartFromSequenceNumber() {
		SequenceNumber sequenceNumber = new SequenceNumber("100");
		StartingPosition position = StartingPosition.restartFromSequenceNumber(sequenceNumber);
		assertEquals(ShardIteratorType.AFTER_SEQUENCE_NUMBER, position.getShardIteratorType());
		assertEquals("100", position.getStartingMarker());
	}

	@Test
	public void testStartingPositionRestartFromAggregatedSequenceNumber() {
		SequenceNumber sequenceNumber = new SequenceNumber("200", 3);
		StartingPosition position = StartingPosition.restartFromSequenceNumber(sequenceNumber);
		assertEquals(ShardIteratorType.AT_SEQUENCE_NUMBER, position.getShardIteratorType());
		assertEquals("200", position.getStartingMarker());
	}

	@Test
	public void testStartingPositionContinueFromAggregatedSequenceNumber() {
		SequenceNumber sequenceNumber = new SequenceNumber("200", 3);
		StartingPosition position = StartingPosition.continueFromSequenceNumber(sequenceNumber);
		assertEquals(ShardIteratorType.AFTER_SEQUENCE_NUMBER, position.getShardIteratorType());
		assertEquals("200", position.getStartingMarker());
	}

	@Test
	public void testStartingPositionRestartFromSentinelEarliest() {
		SequenceNumber sequenceNumber = SentinelSequenceNumber.SENTINEL_EARLIEST_SEQUENCE_NUM.get();
		StartingPosition position = StartingPosition.restartFromSequenceNumber(sequenceNumber);
		assertEquals(ShardIteratorType.TRIM_HORIZON, position.getShardIteratorType());
		assertNull(position.getStartingMarker());
	}

	@Test
	public void testStartingPositionRestartFromSentinelLatest() {
		SequenceNumber sequenceNumber = SentinelSequenceNumber.SENTINEL_LATEST_SEQUENCE_NUM.get();
		StartingPosition position = StartingPosition.restartFromSequenceNumber(sequenceNumber);
		assertEquals(ShardIteratorType.LATEST, position.getShardIteratorType());
		assertNull(position.getStartingMarker());
	}

	@Test
	public void testStartingPositionRestartFromSentinelEnding() {
		thrown.expect(IllegalArgumentException.class);

		SequenceNumber sequenceNumber = SentinelSequenceNumber.SENTINEL_SHARD_ENDING_SEQUENCE_NUM.get();
		StartingPosition position = StartingPosition.restartFromSequenceNumber(sequenceNumber);
		assertEquals(ShardIteratorType.LATEST, position.getShardIteratorType());
		assertNull(position.getStartingMarker());
	}

}
