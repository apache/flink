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
import org.apache.flink.streaming.connectors.kafka.internals.FlinkKafkaInternalProducer;
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
 * A {@link TypeSerializerUpgradeTestBase} for {@link FlinkKafkaProducer.TransactionStateSerializer}
 * and {@link FlinkKafkaProducer.ContextStateSerializer}.
 */
@RunWith(Parameterized.class)
public class KafkaSerializerUpgradeTest extends TypeSerializerUpgradeTestBase<Object, Object> {

    public KafkaSerializerUpgradeTest(TestSpecification<Object, Object> testSpecification) {
        super(testSpecification);
    }

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
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class TransactionStateSerializerSetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<
                    FlinkKafkaProducer.KafkaTransactionState> {
        @Override
        public TypeSerializer<FlinkKafkaProducer.KafkaTransactionState> createPriorSerializer() {
            return new FlinkKafkaProducer.TransactionStateSerializer();
        }

        @Override
        public FlinkKafkaProducer.KafkaTransactionState createTestData() {
            @SuppressWarnings("unchecked")
            FlinkKafkaInternalProducer<byte[], byte[]> mock =
                    Mockito.mock(FlinkKafkaInternalProducer.class);
            return new FlinkKafkaProducer.KafkaTransactionState("1234", 3456, (short) 789, mock);
        }
    }

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class TransactionStateSerializerVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<
                    FlinkKafkaProducer.KafkaTransactionState> {
        @Override
        public TypeSerializer<FlinkKafkaProducer.KafkaTransactionState> createUpgradedSerializer() {
            return new FlinkKafkaProducer.TransactionStateSerializer();
        }

        @Override
        public Matcher<FlinkKafkaProducer.KafkaTransactionState> testDataMatcher() {
            @SuppressWarnings("unchecked")
            FlinkKafkaInternalProducer<byte[], byte[]> mock =
                    Mockito.mock(FlinkKafkaInternalProducer.class);
            return is(
                    new FlinkKafkaProducer.KafkaTransactionState("1234", 3456, (short) 789, mock));
        }

        @Override
        public Matcher<TypeSerializerSchemaCompatibility<FlinkKafkaProducer.KafkaTransactionState>>
                schemaCompatibilityMatcher(MigrationVersion version) {
            return TypeSerializerMatchers.isCompatibleAsIs();
        }
    }

    // ----------------------------------------------------------------------------------------------
    //  Specification for "context-state-serializer"
    // ----------------------------------------------------------------------------------------------

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class ContextStateSerializerSetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<
                    FlinkKafkaProducer.KafkaTransactionContext> {
        @Override
        public TypeSerializer<FlinkKafkaProducer.KafkaTransactionContext> createPriorSerializer() {
            return new FlinkKafkaProducer.ContextStateSerializer();
        }

        @Override
        public FlinkKafkaProducer.KafkaTransactionContext createTestData() {
            Set<String> transactionIds = new HashSet<>();
            transactionIds.add("123");
            transactionIds.add("456");
            transactionIds.add("789");
            return new FlinkKafkaProducer.KafkaTransactionContext(transactionIds);
        }
    }

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class ContextStateSerializerVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<
                    FlinkKafkaProducer.KafkaTransactionContext> {
        @Override
        public TypeSerializer<FlinkKafkaProducer.KafkaTransactionContext>
                createUpgradedSerializer() {
            return new FlinkKafkaProducer.ContextStateSerializer();
        }

        @Override
        public Matcher<FlinkKafkaProducer.KafkaTransactionContext> testDataMatcher() {
            Set<String> transactionIds = new HashSet<>();
            transactionIds.add("123");
            transactionIds.add("456");
            transactionIds.add("789");
            return is(new FlinkKafkaProducer.KafkaTransactionContext(transactionIds));
        }

        @Override
        public Matcher<
                        TypeSerializerSchemaCompatibility<
                                FlinkKafkaProducer.KafkaTransactionContext>>
                schemaCompatibilityMatcher(MigrationVersion version) {
            return TypeSerializerMatchers.isCompatibleAsIs();
        }
    }
}
