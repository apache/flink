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

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.testutils.TestUtils;

import com.amazonaws.services.kinesis.model.Record;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.flink.streaming.connectors.kinesis.testutils.TestUtils.createDummyStreamShardHandle;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link RecordBatch}.
 */
public class RecordBatchTest {

	@Test
	public void testDeaggregateRecordsPassThrough() {
		RecordBatch result = new RecordBatch(Arrays.asList(
			record("1"),
			record("2"),
			record("3"),
			record("4")
		), createDummyStreamShardHandle(), 100L);

		assertEquals(4, result.getAggregatedRecordSize());
		assertEquals(4, result.getDeaggregatedRecordSize());
		assertEquals(128, result.getTotalSizeInBytes());
		assertEquals(32, result.getAverageRecordSizeBytes());
	}

	@Test
	public void testDeaggregateRecordsWithAggregatedRecords() {
		final List<Record> records = TestUtils.createAggregatedRecordBatch(5, 5, new AtomicInteger());
		RecordBatch result = new RecordBatch(records, createDummyStreamShardHandle(), 100L);

		assertEquals(5, result.getAggregatedRecordSize());
		assertEquals(25, result.getDeaggregatedRecordSize());
		assertEquals(25 * 1024, result.getTotalSizeInBytes());
		assertEquals(1024, result.getAverageRecordSizeBytes());
	}

	@Test
	public void testGetAverageRecordSizeBytesEmptyList() {
		RecordBatch result = new RecordBatch(emptyList(), createDummyStreamShardHandle(), 100L);

		assertEquals(0, result.getAggregatedRecordSize());
		assertEquals(0, result.getDeaggregatedRecordSize());
		assertEquals(0, result.getAverageRecordSizeBytes());
	}

	@Test
	public void testGetMillisBehindLatest() {
		RecordBatch result = new RecordBatch(singletonList(record("1")), createDummyStreamShardHandle(), 100L);

		assertEquals(Long.valueOf(100), result.getMillisBehindLatest());
	}

	private Record record(final String sequenceNumber) {
		byte[] data = RandomStringUtils.randomAlphabetic(32)
			.getBytes(ConfigConstants.DEFAULT_CHARSET);

		return new Record()
			.withData(ByteBuffer.wrap(data))
			.withPartitionKey("pk")
			.withSequenceNumber(sequenceNumber);
	}

}
