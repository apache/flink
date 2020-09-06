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

package org.apache.flink.streaming.connectors.kafka;

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.testutils.migration.MigrationVersion;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

/**
 * Tests for checking whether {@link FlinkKafkaProducer011} can restore from snapshots that were
 * done using previous Flink versions' {@link FlinkKafkaProducer011}.
 *
 * <p>For regenerating the binary snapshot files run {@link #writeSnapshot()} on the corresponding
 * Flink release-* branch.
 */
@RunWith(Parameterized.class)
public class FlinkKafkaProducer011MigrationTest extends KafkaMigrationTestBase {
	@Parameterized.Parameters(name = "Migration Savepoint: {0}")
	public static Collection<MigrationVersion> parameters() {
		return Arrays.asList(
			MigrationVersion.v1_8,
			MigrationVersion.v1_9,
			MigrationVersion.v1_10,
			MigrationVersion.v1_11);
	}

	public FlinkKafkaProducer011MigrationTest(MigrationVersion testMigrateVersion) {
		super(testMigrateVersion);
	}

	@Override
	protected Properties createProperties() {
		Properties properties = new Properties();
		properties.putAll(standardProps);
		properties.putAll(secureProps);
		properties.put(ProducerConfig.CLIENT_ID_CONFIG, "producer-client-id");
		properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "producer-transaction-id");
		properties.put(FlinkKafkaProducer011.KEY_DISABLE_METRICS, "true");
		return properties;
	}

	@Override
	protected OneInputStreamOperatorTestHarness<Integer, Object> createTestHarness() throws Exception {
		FlinkKafkaProducer011<Integer> kafkaProducer = new FlinkKafkaProducer011<>(
			TOPIC,
			integerKeyedSerializationSchema,
			createProperties(),
			FlinkKafkaProducer011.Semantic.EXACTLY_ONCE
		).ignoreFailuresAfterTransactionTimeout();

		return new OneInputStreamOperatorTestHarness<>(
			new StreamSink<>(kafkaProducer),
			1,
			1,
			0,
			IntSerializer.INSTANCE,
			new OperatorID(1, 1));
	}
}
