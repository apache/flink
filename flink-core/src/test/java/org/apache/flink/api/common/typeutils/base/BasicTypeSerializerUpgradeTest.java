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

package org.apache.flink.api.common.typeutils.base;

import org.apache.flink.api.common.typeutils.TypeSerializerUpgradeTestBase;
import org.apache.flink.testutils.migration.MigrationVersion;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;

/** A {@link TypeSerializerUpgradeTestBase} for BaseType Serializers. */
@RunWith(Parameterized.class)
public class BasicTypeSerializerUpgradeTest extends TypeSerializerUpgradeTestBase<Object, Object> {

    public BasicTypeSerializerUpgradeTest(TestSpecification<Object, Object> testSpecification) {
        super(testSpecification);
    }

    @Parameterized.Parameters(name = "Test Specification = {0}")
    public static Collection<TestSpecification<?, ?>> testSpecifications() throws Exception {

        ArrayList<TestSpecification<?, ?>> testSpecifications = new ArrayList<>();
        for (MigrationVersion migrationVersion : MIGRATION_VERSIONS) {
            testSpecifications.add(
                    new TestSpecification<>(
                            "big-dec-serializer",
                            migrationVersion,
                            BasicTypeSerializerUpgradeTestSpecifications.BigDecSerializerSetup
                                    .class,
                            BasicTypeSerializerUpgradeTestSpecifications.BigDecSerializerVerifier
                                    .class));
            testSpecifications.add(
                    new TestSpecification<>(
                            "big-int-serializer",
                            migrationVersion,
                            BasicTypeSerializerUpgradeTestSpecifications.BigIntSerializerSetup
                                    .class,
                            BasicTypeSerializerUpgradeTestSpecifications.BigIntSerializerVerifier
                                    .class));
            testSpecifications.add(
                    new TestSpecification<>(
                            "boolean-serializer",
                            migrationVersion,
                            BasicTypeSerializerUpgradeTestSpecifications.BooleanSerializerSetup
                                    .class,
                            BasicTypeSerializerUpgradeTestSpecifications.BooleanSerializerVerifier
                                    .class));
            testSpecifications.add(
                    new TestSpecification<>(
                            "boolean-value-serializer",
                            migrationVersion,
                            BasicTypeSerializerUpgradeTestSpecifications.BooleanValueSerializerSetup
                                    .class,
                            BasicTypeSerializerUpgradeTestSpecifications
                                    .BooleanValueSerializerVerifier.class));
            testSpecifications.add(
                    new TestSpecification<>(
                            "byte-serializer",
                            migrationVersion,
                            BasicTypeSerializerUpgradeTestSpecifications.ByteSerializerSetup.class,
                            BasicTypeSerializerUpgradeTestSpecifications.ByteSerializerVerifier
                                    .class));
            testSpecifications.add(
                    new TestSpecification<>(
                            "byte-value-serializer",
                            migrationVersion,
                            BasicTypeSerializerUpgradeTestSpecifications.ByteValueSerializerSetup
                                    .class,
                            BasicTypeSerializerUpgradeTestSpecifications.ByteValueSerializerVerifier
                                    .class));
            testSpecifications.add(
                    new TestSpecification<>(
                            "char-serializer",
                            migrationVersion,
                            BasicTypeSerializerUpgradeTestSpecifications.CharSerializerSetup.class,
                            BasicTypeSerializerUpgradeTestSpecifications.CharSerializerVerifier
                                    .class));
            testSpecifications.add(
                    new TestSpecification<>(
                            "char-value-serializer",
                            migrationVersion,
                            BasicTypeSerializerUpgradeTestSpecifications.CharValueSerializerSetup
                                    .class,
                            BasicTypeSerializerUpgradeTestSpecifications.CharValueSerializerVerifier
                                    .class));
            testSpecifications.add(
                    new TestSpecification<>(
                            "date-serializer",
                            migrationVersion,
                            BasicTypeSerializerUpgradeTestSpecifications.DateSerializerSetup.class,
                            BasicTypeSerializerUpgradeTestSpecifications.DateSerializerVerifier
                                    .class));
            testSpecifications.add(
                    new TestSpecification<>(
                            "double-serializer",
                            migrationVersion,
                            BasicTypeSerializerUpgradeTestSpecifications.DoubleSerializerSetup
                                    .class,
                            BasicTypeSerializerUpgradeTestSpecifications.DoubleSerializerVerifier
                                    .class));
            testSpecifications.add(
                    new TestSpecification<>(
                            "double-value-serializer",
                            migrationVersion,
                            BasicTypeSerializerUpgradeTestSpecifications.DoubleValueSerializerSetup
                                    .class,
                            BasicTypeSerializerUpgradeTestSpecifications
                                    .DoubleValueSerializerVerifier.class));
            testSpecifications.add(
                    new TestSpecification<>(
                            "float-serializer",
                            migrationVersion,
                            BasicTypeSerializerUpgradeTestSpecifications.FloatSerializerSetup.class,
                            BasicTypeSerializerUpgradeTestSpecifications.FloatSerializerVerifier
                                    .class));
            testSpecifications.add(
                    new TestSpecification<>(
                            "float-value-serializer",
                            migrationVersion,
                            BasicTypeSerializerUpgradeTestSpecifications.FloatValueSerializerSetup
                                    .class,
                            BasicTypeSerializerUpgradeTestSpecifications
                                    .FloatValueSerializerVerifier.class));
            testSpecifications.add(
                    new TestSpecification<>(
                            "int-serializer",
                            migrationVersion,
                            BasicTypeSerializerUpgradeTestSpecifications.IntSerializerSetup.class,
                            BasicTypeSerializerUpgradeTestSpecifications.IntSerializerVerifier
                                    .class));
            testSpecifications.add(
                    new TestSpecification<>(
                            "int-value-serializer",
                            migrationVersion,
                            BasicTypeSerializerUpgradeTestSpecifications.IntValueSerializerSetup
                                    .class,
                            BasicTypeSerializerUpgradeTestSpecifications.IntValueSerializerVerifier
                                    .class));
            testSpecifications.add(
                    new TestSpecification<>(
                            "long-serializer",
                            migrationVersion,
                            BasicTypeSerializerUpgradeTestSpecifications.LongSerializerSetup.class,
                            BasicTypeSerializerUpgradeTestSpecifications.LongSerializerVerifier
                                    .class));
            testSpecifications.add(
                    new TestSpecification<>(
                            "long-value-serializer",
                            migrationVersion,
                            BasicTypeSerializerUpgradeTestSpecifications.LongValueSerializerSetup
                                    .class,
                            BasicTypeSerializerUpgradeTestSpecifications.LongValueSerializerVerifier
                                    .class));
            testSpecifications.add(
                    new TestSpecification<>(
                            "null-value-serializer",
                            migrationVersion,
                            BasicTypeSerializerUpgradeTestSpecifications.NullValueSerializerSetup
                                    .class,
                            BasicTypeSerializerUpgradeTestSpecifications.NullValueSerializerVerifier
                                    .class));
            testSpecifications.add(
                    new TestSpecification<>(
                            "short-serializer",
                            migrationVersion,
                            BasicTypeSerializerUpgradeTestSpecifications.ShortSerializerSetup.class,
                            BasicTypeSerializerUpgradeTestSpecifications.ShortSerializerVerifier
                                    .class));
            testSpecifications.add(
                    new TestSpecification<>(
                            "short-value-serializer",
                            migrationVersion,
                            BasicTypeSerializerUpgradeTestSpecifications.ShortValueSerializerSetup
                                    .class,
                            BasicTypeSerializerUpgradeTestSpecifications
                                    .ShortValueSerializerVerifier.class));
            testSpecifications.add(
                    new TestSpecification<>(
                            "sql-date-serializer",
                            migrationVersion,
                            BasicTypeSerializerUpgradeTestSpecifications.SqlDateSerializerSetup
                                    .class,
                            BasicTypeSerializerUpgradeTestSpecifications.SqlDateSerializerVerifier
                                    .class));
            testSpecifications.add(
                    new TestSpecification<>(
                            "sql-time-serializer",
                            migrationVersion,
                            BasicTypeSerializerUpgradeTestSpecifications.SqlTimeSerializerSetup
                                    .class,
                            BasicTypeSerializerUpgradeTestSpecifications.SqlTimeSerializerVerifier
                                    .class));
            testSpecifications.add(
                    new TestSpecification<>(
                            "sql-timestamp-serializer",
                            migrationVersion,
                            BasicTypeSerializerUpgradeTestSpecifications.SqlTimestampSerializerSetup
                                    .class,
                            BasicTypeSerializerUpgradeTestSpecifications
                                    .SqlTimestampSerializerVerifier.class));
            testSpecifications.add(
                    new TestSpecification<>(
                            "string-serializer",
                            migrationVersion,
                            BasicTypeSerializerUpgradeTestSpecifications.StringSerializerSetup
                                    .class,
                            BasicTypeSerializerUpgradeTestSpecifications.StringSerializerVerifier
                                    .class));
            testSpecifications.add(
                    new TestSpecification<>(
                            "string-value-serializer",
                            migrationVersion,
                            BasicTypeSerializerUpgradeTestSpecifications.StringValueSerializerSetup
                                    .class,
                            BasicTypeSerializerUpgradeTestSpecifications
                                    .StringValueSerializerVerifier.class));
        }

        return testSpecifications;
    }
}
