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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerMatchers;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerUpgradeTestBase;
import org.apache.flink.streaming.connectors.kafka.internal.FlinkKafkaProducer;
import org.apache.flink.testutils.migration.MigrationVersion;

import org.hamcrest.Matcher;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.Matchers.is;

/**
 * A {@link TypeSerializerUpgradeTestBase} for the {@link FlinkKafkaProducer011.TransactionStateSerializer}
 * and {@link FlinkKafkaProducer011.ContextStateSerializer}.
 */
@RunWith(Parameterized.class)
public class Kafka011SerializerUpgradeTest extends TypeSerializerUpgradeTestBase<Object, Object> {

	public Kafka011SerializerUpgradeTest(TestSpecification<Object, Object> testSpecification) {
		super(testSpecification);
	}

	@SuppressWarnings("unchecked")
	@Parameterized.Parameters(name = "Test Specification = {0}")
	public static Collection<TestSpecification<?, ?>> testSpecifications() throws Exception {

		ArrayList<TestSpecification<?, ?>> testSpecifications = new ArrayList<>();
		for (MigrationVersion migrationVersion : MIGRATION_VERSIONS) {
			testSpecifications.add(
				new TestSpecification<>(
					"transaction-state-serializer",
					migrationVersion,
					TransactionStateSerializerSetup.class,
					TransactionStateSerializerVerifier.class));
			testSpecifications.add(
				new TestSpecification<>(
					"context-state-serializer",
					migrationVersion,
					ContextStateSerializerSetup.class,
					ContextStateSerializerVerifier.class));
		}
		return testSpecifications;
	}

	// ----------------------------------------------------------------------------------------------
	//  Specification for "transaction-state-serializer"
	// ----------------------------------------------------------------------------------------------

	/**
	 * This class is only public to work with {@link org.apache.flink.api.common.typeutils.ClassRelocator}.
	 */
	public static final class TransactionStateSerializerSetup implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<FlinkKafkaProducer011.KafkaTransactionState> {
		@Override
		public TypeSerializer<FlinkKafkaProducer011.KafkaTransactionState> createPriorSerializer() {
			return new FlinkKafkaProducer011.TransactionStateSerializer();
		}

		@Override
		public FlinkKafkaProducer011.KafkaTransactionState createTestData() {
			@SuppressWarnings("unchecked")
			FlinkKafkaProducer<byte[], byte[]> mock = Mockito.mock(FlinkKafkaProducer.class);
			return new FlinkKafkaProducer011.KafkaTransactionState("1234", 3456, (short) 789, mock);
		}
	}

	/**
	 * This class is only public to work with {@link org.apache.flink.api.common.typeutils.ClassRelocator}.
	 */
	public static final class TransactionStateSerializerVerifier implements TypeSerializerUpgradeTestBase.UpgradeVerifier<FlinkKafkaProducer011.KafkaTransactionState> {
		@Override
		public TypeSerializer<FlinkKafkaProducer011.KafkaTransactionState> createUpgradedSerializer() {
			return new FlinkKafkaProducer011.TransactionStateSerializer();
		}

		@Override
		public Matcher<FlinkKafkaProducer011.KafkaTransactionState> testDataMatcher() {
			@SuppressWarnings("unchecked")
			FlinkKafkaProducer<byte[], byte[]> mock = Mockito.mock(FlinkKafkaProducer.class);
			return is(new FlinkKafkaProducer011.KafkaTransactionState("1234", 3456, (short) 789, mock));
		}

		@Override
		public Matcher<TypeSerializerSchemaCompatibility<FlinkKafkaProducer011.KafkaTransactionState>> schemaCompatibilityMatcher(MigrationVersion version) {
			return TypeSerializerMatchers.isCompatibleAsIs();
		}
	}

	// ----------------------------------------------------------------------------------------------
	//  Specification for "context-state-serializer"
	// ----------------------------------------------------------------------------------------------

	/**
	 * This class is only public to work with {@link org.apache.flink.api.common.typeutils.ClassRelocator}.
	 */
	public static final class ContextStateSerializerSetup implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<FlinkKafkaProducer011.KafkaTransactionContext> {
		@Override
		public TypeSerializer<FlinkKafkaProducer011.KafkaTransactionContext> createPriorSerializer() {
			return new FlinkKafkaProducer011.ContextStateSerializer();
		}

		@Override
		public FlinkKafkaProducer011.KafkaTransactionContext createTestData() {
			Set<String> transactionIds = new HashSet<>();
			transactionIds.add("123");
			transactionIds.add("456");
			transactionIds.add("789");
			return new FlinkKafkaProducer011.KafkaTransactionContext(transactionIds);
		}
	}

	/**
	 * This class is only public to work with {@link org.apache.flink.api.common.typeutils.ClassRelocator}.
	 */
	public static final class ContextStateSerializerVerifier implements TypeSerializerUpgradeTestBase.UpgradeVerifier<FlinkKafkaProducer011.KafkaTransactionContext> {
		@Override
		public TypeSerializer<FlinkKafkaProducer011.KafkaTransactionContext> createUpgradedSerializer() {
			return new FlinkKafkaProducer011.ContextStateSerializer();
		}

		@Override
		public Matcher<FlinkKafkaProducer011.KafkaTransactionContext> testDataMatcher() {
			Set<String> transactionIds = new HashSet<>();
			transactionIds.add("123");
			transactionIds.add("456");
			transactionIds.add("789");
			return is(new FlinkKafkaProducer011.KafkaTransactionContext(transactionIds));
		}

		@Override
		public Matcher<TypeSerializerSchemaCompatibility<FlinkKafkaProducer011.KafkaTransactionContext>> schemaCompatibilityMatcher(MigrationVersion version) {
			return TypeSerializerMatchers.isCompatibleAsIs();
		}
	}
}
