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

import org.apache.flink.api.common.typeutils.TypeSerializerSnapshotMigrationTestBase;
import org.apache.flink.testutils.migration.MigrationVersion;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;

/**
 * Migration tests for the {@link FlinkKafkaProducer011.TransactionStateSerializer}
 * and {@link FlinkKafkaProducer011.ContextStateSerializer}.
 */
@RunWith(Parameterized.class)
public class KafkaSerializerSnapshotsMigrationTest extends TypeSerializerSnapshotMigrationTestBase<Object> {

	public KafkaSerializerSnapshotsMigrationTest(TestSpecification<Object> testSpecification) {
		super(testSpecification);
	}

	@SuppressWarnings("unchecked")
	@Parameterized.Parameters(name = "Test Specification = {0}")
	public static Collection<TestSpecification<?>> testSpecifications() {

		final TestSpecifications testSpecifications = new TestSpecifications(MigrationVersion.v1_6, MigrationVersion.v1_7);

		testSpecifications.add(
			"transaction-state-serializer",
			FlinkKafkaProducer011.TransactionStateSerializer.class,
			FlinkKafkaProducer011.TransactionStateSerializer.TransactionStateSerializerSnapshot.class,
			FlinkKafkaProducer011.TransactionStateSerializer::new);
		testSpecifications.add(
			"context-state-serializer",
			FlinkKafkaProducer011.ContextStateSerializer.class,
			FlinkKafkaProducer011.ContextStateSerializer.ContextStateSerializerSnapshot.class,
			FlinkKafkaProducer011.ContextStateSerializer::new);

		return testSpecifications.get();
	}
}
