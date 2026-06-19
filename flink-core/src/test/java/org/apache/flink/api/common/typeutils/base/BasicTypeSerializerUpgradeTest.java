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

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.common.typeutils.TypeSerializerUpgradeTestBase;

import java.util.ArrayList;
import java.util.Collection;

/** A {@link TypeSerializerUpgradeTestBase} for BaseType Serializers. */
class BasicTypeSerializerUpgradeTest extends TypeSerializerUpgradeTestBase<Object, Object> {

    public Collection<TestSpecification<?, ?>> createTestSpecifications(FlinkVersion flinkVersion)
            throws Exception {

        ArrayList<TestSpecification<?, ?>> testSpecifications = new ArrayList<>();
        testSpecifications.add(
                new TestSpecification<>(
                        "big-dec-serializer",
                        flinkVersion,
                        BasicTypeSerializerUpgradeTestSpecifications.BigDecSerializerSetup.class,
                        BasicTypeSerializerUpgradeTestSpecifications.BigDecSerializerVerifier
                                .class));
        testSpecifications.add(
                new TestSpecification<>(
                        "big-int-serializer",
                        flinkVersion,
                        BasicTypeSerializerUpgradeTestSpecifications.BigIntSerializerSetup.class,
                        BasicTypeSerializerUpgradeTestSpecifications.BigIntSerializerVerifier
                                .class));
        testSpecifications.add(
                new TestSpecification<>(
                        "boolean-serializer",
                        flinkVersion,
                        BasicTypeSerializerUpgradeTestSpecifications.BooleanSerializerSetup.class,
                        BasicTypeSerializerUpgradeTestSpecifications.BooleanSerializerVerifier
                                .class));
        testSpecifications.add(
                new TestSpecification<>(
                        "boolean-value-serializer",
                        flinkVersion,
                        BasicTypeSerializerUpgradeTestSpecifications.BooleanValueSerializerSetup
                                .class,
                        BasicTypeSerializerUpgradeTestSpecifications.BooleanValueSerializerVerifier
                                .class));
        testSpecifications.add(
                new TestSpecification<>(
                        "byte-serializer",
                        flinkVersion,
                        BasicTypeSerializerUpgradeTestSpecifications.ByteSerializerSetup.class,
                        BasicTypeSerializerUpgradeTestSpecifications.ByteSerializerVerifier.class));
        testSpecifications.add(
                new TestSpecification<>(
                        "byte-value-serializer",
                        flinkVersion,
                        BasicTypeSerializerUpgradeTestSpecifications.ByteValueSerializerSetup.class,
                        BasicTypeSerializerUpgradeTestSpecifications.ByteValueSerializerVerifier
                                .class));
        testSpecifications.add(
                new TestSpecification<>(
                        "char-serializer",
                        flinkVersion,
                        BasicTypeSerializerUpgradeTestSpecifications.CharSerializerSetup.class,
                        BasicTypeSerializerUpgradeTestSpecifications.CharSerializerVerifier.class));
        testSpecifications.add(
                new TestSpecification<>(
                        "char-value-serializer",
                        flinkVersion,
                        BasicTypeSerializerUpgradeTestSpecifications.CharValueSerializerSetup.class,
                        BasicTypeSerializerUpgradeTestSpecifications.CharValueSerializerVerifier
                                .class));
        testSpecifications.add(
                new TestSpecification<>(
                        "date-serializer",
                        flinkVersion,
                        BasicTypeSerializerUpgradeTestSpecifications.DateSerializerSetup.class,
                        BasicTypeSerializerUpgradeTestSpecifications.DateSerializerVerifier.class));
        testSpecifications.add(
                new TestSpecification<>(
                        "double-serializer",
                        flinkVersion,
                        BasicTypeSerializerUpgradeTestSpecifications.DoubleSerializerSetup.class,
                        BasicTypeSerializerUpgradeTestSpecifications.DoubleSerializerVerifier
                                .class));
        testSpecifications.add(
                new TestSpecification<>(
                        "double-value-serializer",
                        flinkVersion,
                        BasicTypeSerializerUpgradeTestSpecifications.DoubleValueSerializerSetup
                                .class,
                        BasicTypeSerializerUpgradeTestSpecifications.DoubleValueSerializerVerifier
                                .class));
        testSpecifications.add(
                new TestSpecification<>(
                        "float-serializer",
                        flinkVersion,
                        BasicTypeSerializerUpgradeTestSpecifications.FloatSerializerSetup.class,
                        BasicTypeSerializerUpgradeTestSpecifications.FloatSerializerVerifier
                                .class));
        testSpecifications.add(
                new TestSpecification<>(
                        "float-value-serializer",
                        flinkVersion,
                        BasicTypeSerializerUpgradeTestSpecifications.FloatValueSerializerSetup
                                .class,
                        BasicTypeSerializerUpgradeTestSpecifications.FloatValueSerializerVerifier
                                .class));
        testSpecifications.add(
                new TestSpecification<>(
                        "int-serializer",
                        flinkVersion,
                        BasicTypeSerializerUpgradeTestSpecifications.IntSerializerSetup.class,
                        BasicTypeSerializerUpgradeTestSpecifications.IntSerializerVerifier.class));
        testSpecifications.add(
                new TestSpecification<>(
                        "int-value-serializer",
                        flinkVersion,
                        BasicTypeSerializerUpgradeTestSpecifications.IntValueSerializerSetup.class,
                        BasicTypeSerializerUpgradeTestSpecifications.IntValueSerializerVerifier
                                .class));
        testSpecifications.add(
                new TestSpecification<>(
                        "long-serializer",
                        flinkVersion,
                        BasicTypeSerializerUpgradeTestSpecifications.LongSerializerSetup.class,
                        BasicTypeSerializerUpgradeTestSpecifications.LongSerializerVerifier.class));
        testSpecifications.add(
                new TestSpecification<>(
                        "long-value-serializer",
                        flinkVersion,
                        BasicTypeSerializerUpgradeTestSpecifications.LongValueSerializerSetup.class,
                        BasicTypeSerializerUpgradeTestSpecifications.LongValueSerializerVerifier
                                .class));
        testSpecifications.add(
                new TestSpecification<>(
                        "null-value-serializer",
                        flinkVersion,
                        BasicTypeSerializerUpgradeTestSpecifications.NullValueSerializerSetup.class,
                        BasicTypeSerializerUpgradeTestSpecifications.NullValueSerializerVerifier
                                .class));
        testSpecifications.add(
                new TestSpecification<>(
                        "short-serializer",
                        flinkVersion,
                        BasicTypeSerializerUpgradeTestSpecifications.ShortSerializerSetup.class,
                        BasicTypeSerializerUpgradeTestSpecifications.ShortSerializerVerifier
                                .class));
        testSpecifications.add(
                new TestSpecification<>(
                        "short-value-serializer",
                        flinkVersion,
                        BasicTypeSerializerUpgradeTestSpecifications.ShortValueSerializerSetup
                                .class,
                        BasicTypeSerializerUpgradeTestSpecifications.ShortValueSerializerVerifier
                                .class));
        testSpecifications.add(
                new TestSpecification<>(
                        "sql-date-serializer",
                        flinkVersion,
                        BasicTypeSerializerUpgradeTestSpecifications.SqlDateSerializerSetup.class,
                        BasicTypeSerializerUpgradeTestSpecifications.SqlDateSerializerVerifier
                                .class));
        testSpecifications.add(
                new TestSpecification<>(
                        "sql-time-serializer",
                        flinkVersion,
                        BasicTypeSerializerUpgradeTestSpecifications.SqlTimeSerializerSetup.class,
                        BasicTypeSerializerUpgradeTestSpecifications.SqlTimeSerializerVerifier
                                .class));
        testSpecifications.add(
                new TestSpecification<>(
                        "sql-timestamp-serializer",
                        flinkVersion,
                        BasicTypeSerializerUpgradeTestSpecifications.SqlTimestampSerializerSetup
                                .class,
                        BasicTypeSerializerUpgradeTestSpecifications.SqlTimestampSerializerVerifier
                                .class));
        testSpecifications.add(
                new TestSpecification<>(
                        "string-serializer",
                        flinkVersion,
                        BasicTypeSerializerUpgradeTestSpecifications.StringSerializerSetup.class,
                        BasicTypeSerializerUpgradeTestSpecifications.StringSerializerVerifier
                                .class));
        testSpecifications.add(
                new TestSpecification<>(
                        "string-value-serializer",
                        flinkVersion,
                        BasicTypeSerializerUpgradeTestSpecifications.StringValueSerializerSetup
                                .class,
                        BasicTypeSerializerUpgradeTestSpecifications.StringValueSerializerVerifier
                                .class));

        return testSpecifications;
    }
}
