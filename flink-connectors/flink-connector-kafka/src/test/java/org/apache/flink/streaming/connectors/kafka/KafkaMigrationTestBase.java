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

package org.apache.flink.streaming.connectors.kafka;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OperatorSnapshotUtil;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.testutils.migration.MigrationVersion;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Optional;
import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The base class with migration tests for the Kafka Exactly-Once Producer.
 */
@SuppressWarnings("serial")
public abstract class KafkaMigrationTestBase extends KafkaTestBase {

	protected static final Logger LOG = LoggerFactory.getLogger(KafkaMigrationTestBase.class);
	protected static final String TOPIC = "flink-kafka-producer-migration-test";

	protected final MigrationVersion testMigrateVersion;
	protected final TypeInformationSerializationSchema<Integer> integerSerializationSchema =
		new TypeInformationSerializationSchema<>(BasicTypeInfo.INT_TYPE_INFO, new ExecutionConfig());
	protected final KeyedSerializationSchema<Integer> integerKeyedSerializationSchema =
		new KeyedSerializationSchemaWrapper<>(integerSerializationSchema);

	/**
	 * TODO change this to the corresponding savepoint version to be written (e.g. {@link MigrationVersion#v1_3} for 1.3)
	 * TODO and remove all @Ignore annotations on write*Snapshot() methods to generate savepoints
	 * TODO Note: You should generate the savepoint based on the release branch instead of the master.
	 */
	protected final Optional<MigrationVersion> flinkGenerateSavepointVersion = Optional.empty();

	public KafkaMigrationTestBase(MigrationVersion testMigrateVersion) {
		this.testMigrateVersion = checkNotNull(testMigrateVersion);
	}

	public String getOperatorSnapshotPath() {
		return getOperatorSnapshotPath(testMigrateVersion);
	}

	public String getOperatorSnapshotPath(MigrationVersion version) {
		return "src/test/resources/kafka-migration-kafka-producer-flink-" + version + "-snapshot";
	}

	/**
	 * Override {@link KafkaTestBase}. Kafka Migration Tests are starting up Kafka/ZooKeeper cluster manually
	 */
	@BeforeClass
	public static void prepare() throws Exception {
	}

	/**
	 * Override {@link KafkaTestBase}. Kafka Migration Tests are starting up Kafka/ZooKeeper cluster manually
	 */
	@AfterClass
	public static void shutDownServices() throws Exception {
	}

	/**
	 * Manually run this to write binary snapshot data.
	 */
	@Ignore
	@Test
	public void writeSnapshot() throws Exception {
		try {
			checkState(flinkGenerateSavepointVersion.isPresent());
			startClusters();

			OperatorSubtaskState snapshot = initializeTestState();
			OperatorSnapshotUtil.writeStateHandle(snapshot, getOperatorSnapshotPath(flinkGenerateSavepointVersion.get()));
		}
		finally {
			shutdownClusters();
		}
	}

	private OperatorSubtaskState initializeTestState() throws Exception {
		try (OneInputStreamOperatorTestHarness testHarness = createTestHarness()) {
			testHarness.setup();
			testHarness.open();

			// Create a committed transaction
			testHarness.processElement(42, 0L);

			// TODO: when stop with savepoint is available, replace this code with it (with stop with savepoint
			// there won't be any pending transactions)
			OperatorSubtaskState snapshot = testHarness.snapshot(0L, 1L);
			// We kind of simulate stop with savepoint by making sure that notifyOfCompletedCheckpoint is called
			testHarness.notifyOfCompletedCheckpoint(0L);

			// Create a Pending transaction
			testHarness.processElement(43, 2L);
			return snapshot;
		}
	}

	@SuppressWarnings("warning")
	@Test
	public void testRestoreProducer() throws Exception {
		try {
			startClusters();

			initializeTestState();

			try (OneInputStreamOperatorTestHarness testHarness = createTestHarness()) {
				initializeState(testHarness);

				// Create a committed transaction
				testHarness.processElement(44, 4L);
				testHarness.snapshot(2L, 5L);
				testHarness.notifyOfCompletedCheckpoint(2L);

				// Create a pending transaction
				testHarness.processElement(45, 6L);

				// We should have:
				// - committed transaction 42
				// - transaction 43 aborted
				// - committed transaction 44
				// - transaction 45 pending
				assertExactlyOnceForTopic(createProperties(), TOPIC, 0, Arrays.asList(42, 44));
			}
		}
		finally {
			shutdownClusters();
		}
	}

	protected abstract OneInputStreamOperatorTestHarness<Integer, Object> createTestHarness() throws Exception;

	protected abstract Properties createProperties();

	protected void initializeState(OneInputStreamOperatorTestHarness testHarness) throws Exception{
		testHarness.setup();
		testHarness.initializeState(getOperatorSnapshotPath());
		testHarness.open();
	}
}
