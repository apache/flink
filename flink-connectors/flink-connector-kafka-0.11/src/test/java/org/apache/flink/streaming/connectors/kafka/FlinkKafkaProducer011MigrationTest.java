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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OperatorSnapshotUtil;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper;
import org.apache.flink.testutils.migration.MigrationVersion;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
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
public class FlinkKafkaProducer011MigrationTest extends KafkaTestBase {

	/**
	 * TODO change this to the corresponding savepoint version to be written (e.g. {@link MigrationVersion#v1_3} for 1.3)
	 * TODO and remove all @Ignore annotations on write*Snapshot() methods to generate savepoints
	 * TODO Note: You should generate the savepoint based on the release branch instead of the master.
	 */
	private final MigrationVersion flinkGenerateSavepointVersion = null;

	private static String topic = "flink-kafka-producer-migration-test";

	protected TypeInformationSerializationSchema<Integer> integerSerializationSchema =
		new TypeInformationSerializationSchema<>(BasicTypeInfo.INT_TYPE_INFO, new ExecutionConfig());
	protected KeyedSerializationSchema<Integer> integerKeyedSerializationSchema =
		new KeyedSerializationSchemaWrapper<>(integerSerializationSchema);

	private final MigrationVersion testMigrateVersion;

	@Parameterized.Parameters(name = "Migration Savepoint: {0}")
	public static Collection<MigrationVersion> parameters() {
		return Arrays.asList(
			MigrationVersion.v1_7);
	}

	public FlinkKafkaProducer011MigrationTest(MigrationVersion testMigrateVersion) {
		this.testMigrateVersion = testMigrateVersion;
	}

	@BeforeClass
	public static void selectKafkaTestImplementation() {
		// A specific kafka test implementation that doesn't erase the tmp file when there is no tmp file yet.
		setKafkaTestImplementation("org.apache.flink.streaming.connectors.kafka.KafkaMigrationTestEnvironmentImpl");
	}

	/**
	 * Manually run this to write binary snapshot data.
	 */
	@Ignore
	@SuppressWarnings("warning")
	@Test
	public void writeSnapshot() throws Exception {

		prepare();

		OneInputStreamOperatorTestHarness testHarness = createTestHarness();
		testHarness.setup();
		testHarness.open();

		// Create a committed transaction
		testHarness.processElement(42, 0L);
		testHarness.snapshot(0L, 1L);
		testHarness.notifyOfCompletedCheckpoint(0L);

		// Create a Pending transaction
		testHarness.processElement(43, 2L);

		OperatorSubtaskState snapshot = testHarness.snapshot(1L, 3L);
		OperatorSnapshotUtil.writeStateHandle(snapshot, "src/test/resources/kafka-producer011-migration-test-flink-" + flinkGenerateSavepointVersion + "-snapshot");

		testHarness.close();

		shutDownServices();
	}

	@SuppressWarnings("warning")
	@Test
	public void testRestoreKafkaTempDirectory() throws Exception {

		prepare();

		OneInputStreamOperatorTestHarness<Integer, Object> testHarness = createTestHarness();

		try {
			initializeState(testHarness);

			// We should have committed transaction 42
			assertExactlyOnceForTopic(createProperties(), topic, 0, Arrays.asList(42));

			testHarness.close();
		} catch (Exception e) {
			shutDownServices();
			LOG.trace(e.getMessage());

		}

		shutDownServices();
	}

	@SuppressWarnings("warning")
	@Test
	public void testRestoreProducer() throws Exception {

		prepare();

		OneInputStreamOperatorTestHarness testHarness = createTestHarness();

		try {
			initializeState(testHarness);

			// Create a committed transaction
			testHarness.processElement(44, 4L);
			testHarness.snapshot(2L, 5L);
			testHarness.notifyOfCompletedCheckpoint(2L);

			// Create a pending transaction
			testHarness.processElement(45, 6L);
			testHarness.snapshot(3L, 6L);

			// We should have:
			// - committed transaction 42
			// - transaction 43 aborted
			// - committed transaction 44
			// - transaction 45 aborted
			assertExactlyOnceForTopic(createProperties(), topic, 0, Arrays.asList(42, 44));

			testHarness.close();
		} catch (Exception e) {
			shutDownServices();
			LOG.trace(e.getMessage());
		}

		shutDownServices();
	}

	private Properties createProperties() {
		Properties properties = new Properties();
		properties.putAll(standardProps);
		properties.putAll(secureProps);
		properties.put(ProducerConfig.CLIENT_ID_CONFIG, "producer-client-id");
		properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "producer-transaction-id");
		properties.put(FlinkKafkaProducer011.KEY_DISABLE_METRICS, "true");
		return properties;
	}

	private OneInputStreamOperatorTestHarness<Integer, Object> createTestHarness() throws Exception {

		FlinkKafkaProducer011<Integer> kafkaProducer = new FlinkKafkaProducer011<>(
			topic,
			integerKeyedSerializationSchema,
			createProperties(),
			FlinkKafkaProducer011.Semantic.EXACTLY_ONCE
		);

		return new OneInputStreamOperatorTestHarness<>(
			new StreamSink<>(kafkaProducer),
			1,
			1,
			0,
			IntSerializer.INSTANCE,
			new OperatorID(1, 1));
	}

	private void initializeState(OneInputStreamOperatorTestHarness testHarness) throws Exception{

		testHarness.setup();
		testHarness.initializeState(
			OperatorSnapshotUtil.getResourceFilename(
				"kafka-producer011-migration-test-flink-" + testMigrateVersion + "-snapshot"
			)
		);
		testHarness.open();
	}
}
