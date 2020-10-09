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

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.KafkaSourceTestEnv;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializer;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.connector.testutils.source.reader.SourceReaderTestBase;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Unit tests for {@link KafkaSourceReader}.
 */
public class KafkaSourceReaderTest extends SourceReaderTestBase<KafkaPartitionSplit> {
	private static final String TOPIC = "KafkaSourceReaderTest";
	private static final String GROUP_ID = "KafkaSourceReaderTestConsumerGroup";

	@BeforeClass
	public static void setup() throws Throwable {
		KafkaSourceTestEnv.setup();
		try (AdminClient adminClient = KafkaSourceTestEnv.getAdminClient()) {
			adminClient.createTopics(Collections.singleton(new NewTopic(TOPIC, NUM_SPLITS, (short) 1)));
		}
		KafkaSourceTestEnv.produceToKafka(getRecords(), StringSerializer.class, IntegerSerializer.class);
	}

	@AfterClass
	public static void tearDown() throws Exception {
		KafkaSourceTestEnv.tearDown();
	}

	// -----------------------------------------

	@Test
	public void testOffsetCommitOnCheckpointComplete() throws Exception {
		try (KafkaSourceReader<Integer> reader =
				(KafkaSourceReader<Integer>) createReader(Boundedness.CONTINUOUS_UNBOUNDED)) {
			reader.addSplits(getSplits(NUM_SPLITS, NUM_RECORDS_PER_SPLIT, Boundedness.CONTINUOUS_UNBOUNDED));
			ValidatingSourceOutput output = new ValidatingSourceOutput();
			long checkpointId = 0;
			do {
				checkpointId++;
				reader.pollNext(output);
				// Create a checkpoint for each message consumption, but not complete them.
				reader.snapshotState(checkpointId);
			} while (output.count() < TOTAL_NUM_RECORDS);

			// The completion of the last checkpoint should subsume all the previous checkpoitns.
			assertEquals(checkpointId, reader.getOffsetsToCommit().size());
			reader.notifyCheckpointComplete(checkpointId);
		}

		// Verify the committed offsets.
		try (AdminClient adminClient = KafkaSourceTestEnv.getAdminClient()) {
			Map<TopicPartition, OffsetAndMetadata> committedOffsets =
				adminClient.listConsumerGroupOffsets(GROUP_ID).partitionsToOffsetAndMetadata().get();
			assertEquals(NUM_SPLITS, committedOffsets.size());
			committedOffsets.forEach(
				(tp, offsetAndMetadata) -> assertEquals(NUM_RECORDS_PER_SPLIT, offsetAndMetadata.offset()));
		}
	}

	// ------------------------------------------

	@Override
	protected SourceReader<Integer, KafkaPartitionSplit> createReader() {
		return createReader(Boundedness.BOUNDED);
	}

	@Override
	protected List<KafkaPartitionSplit> getSplits(int numSplits, int numRecordsPerSplit, Boundedness boundedness) {
		List<KafkaPartitionSplit> splits = new ArrayList<>();
		for (int i = 0; i < numRecordsPerSplit; i++) {
			splits.add(getSplit(i, numRecordsPerSplit, boundedness));
		}
		return splits;
	}

	@Override
	protected KafkaPartitionSplit getSplit(int splitId, int numRecords, Boundedness boundedness) {
		long stoppingOffset =
				boundedness == Boundedness.BOUNDED ? NUM_RECORDS_PER_SPLIT : KafkaPartitionSplit.NO_STOPPING_OFFSET;
		return new KafkaPartitionSplit(
				new TopicPartition(TOPIC, splitId),
				0L,
				stoppingOffset);
	}

	@Override
	protected long getNextRecordIndex(KafkaPartitionSplit split) {
		return split.getStartingOffset();
	}

	// ---------------------

	private SourceReader<Integer, KafkaPartitionSplit> createReader(Boundedness boundedness) {
		KafkaSourceBuilder<Integer> builder = KafkaSource.<Integer>builder()
			.setClientIdPrefix("KafkaSourceReaderTest")
			.setDeserializer(KafkaRecordDeserializer.valueOnly(IntegerDeserializer.class))
			.setPartitions(Collections.singleton(new TopicPartition("AnyTopic", 0)))
			.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaSourceTestEnv.brokerConnectionStrings)
			.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);

		if (boundedness == Boundedness.BOUNDED) {
			builder.setBounded(OffsetsInitializer.latest());
		}

		return builder.build().createReader(new SourceReaderContext() {
			@Override
			public MetricGroup metricGroup() {
				return new UnregisteredMetricsGroup();
			}

			@Override
			public Configuration getConfiguration() {
				return null;
			}

			@Override
			public String getLocalHostName() {
				return null;
			}

			@Override
			public int getIndexOfSubtask() {
				return 0;
			}

			@Override
			public void sendSourceEventToCoordinator(SourceEvent sourceEvent) {

			}
		});
	}

	// ---------------------

	private static List<ProducerRecord<String, Integer>> getRecords() {
		List<ProducerRecord<String, Integer>> records = new ArrayList<>();
		for (int part = 0; part < NUM_SPLITS; part++) {
			for (int i = 0; i < NUM_RECORDS_PER_SPLIT; i++) {
				records.add(new ProducerRecord<>(
						TOPIC,
						part,
						TOPIC + "-" + part,
						part * NUM_RECORDS_PER_SPLIT + i));
			}
		}
		return records;
	}
}
