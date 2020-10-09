/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.kafka.source.reader;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.kafka.source.KafkaSourceTestEnv;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializer;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Unit tests for {@link KafkaPartitionSplitReader}.
 */
public class KafkaPartitionSplitReaderTest {
	private static final int NUM_SUBTASKS = 3;
	private static final String TOPIC1 = "topic1";
	private static final String TOPIC2 = "topic2";

	private static Map<Integer, Map<String, KafkaPartitionSplit>> splitsByOwners;
	private static Map<TopicPartition, Long> earliestOffsets;

	@BeforeClass
	public static void setup() throws Throwable {
		KafkaSourceTestEnv.setup();
		KafkaSourceTestEnv.setupTopic(TOPIC1, true, true);
		KafkaSourceTestEnv.setupTopic(TOPIC2, true, true);
		splitsByOwners = KafkaSourceTestEnv.getSplitsByOwners(Arrays.asList(TOPIC1, TOPIC2), NUM_SUBTASKS);
		earliestOffsets = KafkaSourceTestEnv
				.getEarliestOffsets(KafkaSourceTestEnv.getPartitionsForTopics(Arrays.asList(TOPIC1, TOPIC2)));
	}

	@AfterClass
	public static void tearDown() throws Exception {
		KafkaSourceTestEnv.tearDown();
	}

	@Test
	public void testHandleSplitChangesAndFetch() throws IOException {
		KafkaPartitionSplitReader<Integer> reader = createReader();
		assignSplitsAndFetchUntilFinish(reader, 0);
		assignSplitsAndFetchUntilFinish(reader, 1);
	}

	@Test
	public void testWakeUp() throws InterruptedException {
		KafkaPartitionSplitReader<Integer> reader = createReader();
		TopicPartition nonExistingTopicPartition = new TopicPartition("NotExist", 0);
		assignSplits(
				reader,
				Collections.singletonMap(
						KafkaPartitionSplit.toSplitId(nonExistingTopicPartition),
						new KafkaPartitionSplit(nonExistingTopicPartition, 0)));
		AtomicReference<Throwable> error = new AtomicReference<>();
		Thread t = new Thread(() -> {
			try {
				reader.fetch();
			} catch (Throwable e) {
				error.set(e);
			}
		}, "testWakeUp-thread");
		t.start();
		long deadline = System.currentTimeMillis() + 5000L;
		while (t.isAlive() && System.currentTimeMillis() < deadline) {
			reader.wakeUp();
			Thread.sleep(10);
		}
		assertNull(error.get());
	}

	// ------------------

	private void assignSplitsAndFetchUntilFinish(KafkaPartitionSplitReader<Integer> reader, int readerId)
			throws IOException {
		Map<String, KafkaPartitionSplit> splits = assignSplits(reader, splitsByOwners.get(readerId));

		Map<String, Integer> numConsumedRecords = new HashMap<>();
		Set<String> finishedSplits = new HashSet<>();
		while (finishedSplits.size() < splits.size()) {
			RecordsWithSplitIds<Tuple3<Integer, Long, Long>> recordsBySplitIds = reader.fetch();
			String splitId = recordsBySplitIds.nextSplit();
			while (splitId != null) {
				// Collect the records in this split.
				List<Tuple3<Integer, Long, Long>> splitFetch = new ArrayList<>();
				Tuple3<Integer, Long, Long> record;
				while ((record = recordsBySplitIds.nextRecordFromSplit()) != null) {
					splitFetch.add(record);
				}

				// Compute the expected next offset for the split.
				TopicPartition tp = splits.get(splitId).getTopicPartition();
				long earliestOffset = earliestOffsets.get(tp);
				int numConsumedRecordsForSplit = numConsumedRecords.getOrDefault(splitId, 0);
				long expectedStartingOffset = earliestOffset + numConsumedRecordsForSplit;

				// verify the consumed records.
				if (verifyConsumed(splits.get(splitId), expectedStartingOffset, splitFetch)) {
					finishedSplits.add(splitId);
				}
				numConsumedRecords.compute(splitId, (ignored, recordCount) ->
														recordCount == null ? splitFetch.size() : recordCount + splitFetch.size());
				splitId = recordsBySplitIds.nextSplit();
			}
		}

		// Verify the number of records consumed from each split.
		numConsumedRecords.forEach((splitId, recordCount) -> {
			TopicPartition tp = splits.get(splitId).getTopicPartition();
			long earliestOffset = earliestOffsets.get(tp);
			long expectedRecordCount = KafkaSourceTestEnv.NUM_RECORDS_PER_PARTITION - earliestOffset;
			assertEquals(String.format("%s should have %d records.", splits.get(splitId), expectedRecordCount),
					expectedRecordCount, (long) recordCount);
		});
	}

	// ------------------

	private KafkaPartitionSplitReader<Integer> createReader() {
		Properties props = new Properties();
		props.putAll(KafkaSourceTestEnv.getConsumerProperties(ByteArrayDeserializer.class));
		props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
		return new KafkaPartitionSplitReader<>(
				props,
				KafkaRecordDeserializer.valueOnly(IntegerDeserializer.class),
				0);
	}

	private Map<String, KafkaPartitionSplit> assignSplits(
			KafkaPartitionSplitReader<Integer> reader,
			Map<String, KafkaPartitionSplit> splits) {
		SplitsChange<KafkaPartitionSplit> splitsChange = new SplitsAddition<>(new ArrayList<>(splits.values()));
		reader.handleSplitsChanges(splitsChange);
		return splits;
	}

	private boolean verifyConsumed(
			final KafkaPartitionSplit split,
			final long expectedStartingOffset,
			final Collection<Tuple3<Integer, Long, Long>> consumed) {
		long expectedOffset = expectedStartingOffset;

		for (Tuple3<Integer, Long, Long> record : consumed) {
			int expectedValue = (int) expectedOffset;
			long expectedTimestamp = expectedOffset * 1000L;

			assertEquals(expectedValue, (int) record.f0);
			assertEquals(expectedOffset, (long) record.f1);
			assertEquals(expectedTimestamp, (long) record.f2);

			expectedOffset++;
		}
		if (split.getStoppingOffset().isPresent()) {
			return expectedOffset == split.getStoppingOffset().get();
		} else {
			return false;
		}
	}
}
