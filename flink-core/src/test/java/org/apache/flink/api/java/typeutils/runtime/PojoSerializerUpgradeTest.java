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

package org.apache.flink.api.java.typeutils.runtime;

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.common.typeutils.TypeSerializerUpgradeTestBase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/** A {@link TypeSerializerUpgradeTestBase} for the {@link PojoSerializer}. */
class PojoSerializerUpgradeTest extends TypeSerializerUpgradeTestBase<Object, Object> {

    @Override
    public Collection<FlinkVersion> getMigrationVersions() {
        // for PojoSerializer we also test against 1.7, 1.8, and 1.9 because we have snapshots
        // for this which go beyond what we have for the usual subclasses of
        // TypeSerializerUpgradeTestBase. We don't have snapshot data for 1.10, but the
        // PojoSerializer has not been changed in quite a while anyways.
        List<FlinkVersion> testVersions = new ArrayList<>();
        testVersions.add(FlinkVersion.v1_7);
        testVersions.add(FlinkVersion.v1_8);
        testVersions.add(FlinkVersion.v1_9);
        testVersions.addAll(super.getMigrationVersions());
        return testVersions;
    }

    public Collection<TestSpecification<?, ?>> createTestSpecifications(FlinkVersion flinkVersion)
            throws Exception {
        Collection<TestSpecification<?, ?>> testSpecifications = new ArrayList<>();
        testSpecifications.add(
                new TestSpecification<>(
                        "pojo-serializer-identical-schema",
                        flinkVersion,
                        PojoSerializerUpgradeTestSpecifications.IdenticalPojoSchemaSetup.class,
                        PojoSerializerUpgradeTestSpecifications.IdenticalPojoSchemaVerifier.class));
        testSpecifications.add(
                new TestSpecification<>(
                        "pojo-serializer-with-modified-schema",
                        flinkVersion,
                        PojoSerializerUpgradeTestSpecifications.ModifiedPojoSchemaSetup.class,
                        PojoSerializerUpgradeTestSpecifications.ModifiedPojoSchemaVerifier.class));
        testSpecifications.add(
                new TestSpecification<>(
                        "pojo-serializer-with-different-field-types",
                        flinkVersion,
                        PojoSerializerUpgradeTestSpecifications.DifferentFieldTypePojoSchemaSetup
                                .class,
                        PojoSerializerUpgradeTestSpecifications.DifferentFieldTypePojoSchemaVerifier
                                .class));
        testSpecifications.add(
                new TestSpecification<>(
                        "pojo-serializer-with-modified-schema-in-registered-subclass",
                        flinkVersion,
                        PojoSerializerUpgradeTestSpecifications
                                .ModifiedRegisteredPojoSubclassSchemaSetup.class,
                        PojoSerializerUpgradeTestSpecifications
                                .ModifiedRegisteredPojoSubclassSchemaVerifier.class));
        testSpecifications.add(
                new TestSpecification<>(
                        "pojo-serializer-with-different-field-types-in-registered-subclass",
                        flinkVersion,
                        PojoSerializerUpgradeTestSpecifications
                                .DifferentFieldTypePojoSubclassSchemaSetup.class,
                        PojoSerializerUpgradeTestSpecifications
                                .DifferentFieldTypePojoSubclassSchemaVerifier.class));
        testSpecifications.add(
                new TestSpecification<>(
                        "pojo-serializer-with-non-registered-subclass",
                        flinkVersion,
                        PojoSerializerUpgradeTestSpecifications.NonRegisteredPojoSubclassSetup
                                .class,
                        PojoSerializerUpgradeTestSpecifications.NonRegisteredPojoSubclassVerifier
                                .class));
        testSpecifications.add(
                new TestSpecification<>(
                        "pojo-serializer-with-different-subclass-registration-order",
                        flinkVersion,
                        PojoSerializerUpgradeTestSpecifications
                                .DifferentPojoSubclassRegistrationOrderSetup.class,
                        PojoSerializerUpgradeTestSpecifications
                                .DifferentPojoSubclassRegistrationOrderVerifier.class));
        testSpecifications.add(
                new TestSpecification<>(
                        "pojo-serializer-with-missing-registered-subclass",
                        flinkVersion,
                        PojoSerializerUpgradeTestSpecifications.MissingRegisteredPojoSubclassSetup
                                .class,
                        PojoSerializerUpgradeTestSpecifications
                                .MissingRegisteredPojoSubclassVerifier.class));
        testSpecifications.add(
                new TestSpecification<>(
                        "pojo-serializer-with-new-registered-subclass",
                        flinkVersion,
                        PojoSerializerUpgradeTestSpecifications.NewRegisteredPojoSubclassSetup
                                .class,
                        PojoSerializerUpgradeTestSpecifications.NewRegisteredPojoSubclassVerifier
                                .class));
        testSpecifications.add(
                new TestSpecification<>(
                        "pojo-serializer-with-new-and-missing-registered-subclasses",
                        flinkVersion,
                        PojoSerializerUpgradeTestSpecifications
                                .NewAndMissingRegisteredPojoSubclassesSetup.class,
                        PojoSerializerUpgradeTestSpecifications
                                .NewAndMissingRegisteredPojoSubclassesVerifier.class));

        return testSpecifications;
    }
}
