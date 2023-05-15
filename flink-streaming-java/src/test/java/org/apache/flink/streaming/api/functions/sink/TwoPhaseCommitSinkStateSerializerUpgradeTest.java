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

package org.apache.flink.streaming.api.functions.sink;

import org.apache.flink.FlinkVersion;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerMatchers;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerUpgradeTestBase;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;

import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static org.hamcrest.Matchers.is;

/**
 * A {@link TypeSerializerUpgradeTestBase} for {@link TwoPhaseCommitSinkFunction.StateSerializer}.
 */
@VisibleForTesting
public class TwoPhaseCommitSinkStateSerializerUpgradeTest
        extends TypeSerializerUpgradeTestBase<
                TwoPhaseCommitSinkFunction.State<Integer, String>,
                TwoPhaseCommitSinkFunction.State<Integer, String>> {

    public Collection<TestSpecification<?, ?>> createTestSpecifications(FlinkVersion flinkVersion)
            throws Exception {

        ArrayList<TestSpecification<?, ?>> testSpecifications = new ArrayList<>();
        testSpecifications.add(
                new TestSpecification<>(
                        "two-phase-commit-sink-state-serializer",
                        flinkVersion,
                        TwoPhaseCommitSinkStateSerializerSetup.class,
                        TwoPhaseCommitSinkStateSerializerVerifier.class));
        return testSpecifications;
    }

    public static TypeSerializer<TwoPhaseCommitSinkFunction.State<Integer, String>>
            intStringStateSerializerSupplier() {
        return new TwoPhaseCommitSinkFunction.StateSerializer<>(
                IntSerializer.INSTANCE, StringSerializer.INSTANCE);
    }

    // ----------------------------------------------------------------------------------------------
    //  Specification for "two-phase-commit-sink-state-serializer"
    // ----------------------------------------------------------------------------------------------
    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class TwoPhaseCommitSinkStateSerializerSetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<
                    TwoPhaseCommitSinkFunction.State<Integer, String>> {

        @Override
        public TypeSerializer<TwoPhaseCommitSinkFunction.State<Integer, String>>
                createPriorSerializer() {
            return intStringStateSerializerSupplier();
        }

        @Override
        public TwoPhaseCommitSinkFunction.State<Integer, String> createTestData() {
            TwoPhaseCommitSinkFunction.TransactionHolder<Integer> pendingTransaction =
                    new TwoPhaseCommitSinkFunction.TransactionHolder<>(12, 1523467890);
            List<TwoPhaseCommitSinkFunction.TransactionHolder<Integer>> list = new ArrayList<>();
            list.add(new TwoPhaseCommitSinkFunction.TransactionHolder<>(123, 1567234890));
            Optional<String> optional = Optional.of("flink");
            return new TwoPhaseCommitSinkFunction.State<>(pendingTransaction, list, optional);
        }
    }

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class TwoPhaseCommitSinkStateSerializerVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<
                    TwoPhaseCommitSinkFunction.State<Integer, String>> {
        @Override
        public TypeSerializer<TwoPhaseCommitSinkFunction.State<Integer, String>>
                createUpgradedSerializer() {
            return intStringStateSerializerSupplier();
        }

        @Override
        public Matcher<TwoPhaseCommitSinkFunction.State<Integer, String>> testDataMatcher() {
            TwoPhaseCommitSinkFunction.TransactionHolder<Integer> pendingTransaction =
                    new TwoPhaseCommitSinkFunction.TransactionHolder<>(12, 1523467890);
            List<TwoPhaseCommitSinkFunction.TransactionHolder<Integer>> list = new ArrayList<>();
            list.add(new TwoPhaseCommitSinkFunction.TransactionHolder<>(123, 1567234890));
            Optional<String> optional = Optional.of("flink");
            return is(new TwoPhaseCommitSinkFunction.State<>(pendingTransaction, list, optional));
        }

        @Override
        public Matcher<
                        TypeSerializerSchemaCompatibility<
                                TwoPhaseCommitSinkFunction.State<Integer, String>>>
                schemaCompatibilityMatcher(FlinkVersion version) {
            if (version.isNewerVersionThan(FlinkVersion.v1_13)) {
                return TypeSerializerMatchers.isCompatibleAsIs();
            } else {
                return TypeSerializerMatchers.isCompatibleAfterMigration();
            }
        }
    }
}
