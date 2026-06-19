/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.common.typeutils.base.array;

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.common.typeutils.TypeSerializerUpgradeTestBase;

import java.util.ArrayList;
import java.util.Collection;

/** Migration tests for primitive array type serializers' snapshots. */
class PrimitiveArraySerializerUpgradeTest extends TypeSerializerUpgradeTestBase<Object, Object> {

    public Collection<TestSpecification<?, ?>> createTestSpecifications(FlinkVersion flinkVersion)
            throws Exception {
        ArrayList<TestSpecification<?, ?>> testSpecifications = new ArrayList<>();
        testSpecifications.add(
                new TestSpecification<>(
                        "boolean-primitive-array-serializer",
                        flinkVersion,
                        PrimitiveArraySerializerUpgradeTestSpecifications.PrimitiveBooleanArraySetup
                                .class,
                        PrimitiveArraySerializerUpgradeTestSpecifications
                                .PrimitiveBooleanArrayVerifier.class));
        testSpecifications.add(
                new TestSpecification<>(
                        "byte-primitive-array-serializer",
                        flinkVersion,
                        PrimitiveArraySerializerUpgradeTestSpecifications.PrimitiveByteArraySetup
                                .class,
                        PrimitiveArraySerializerUpgradeTestSpecifications.PrimitiveByteArrayVerifier
                                .class));
        testSpecifications.add(
                new TestSpecification<>(
                        "char-primitive-array-serializer",
                        flinkVersion,
                        PrimitiveArraySerializerUpgradeTestSpecifications.PrimitiveCharArraySetup
                                .class,
                        PrimitiveArraySerializerUpgradeTestSpecifications.PrimitiveCharArrayVerifier
                                .class));
        testSpecifications.add(
                new TestSpecification<>(
                        "double-primitive-array-serializer",
                        flinkVersion,
                        PrimitiveArraySerializerUpgradeTestSpecifications.PrimitiveDoubleArraySetup
                                .class,
                        PrimitiveArraySerializerUpgradeTestSpecifications
                                .PrimitiveDoubleArrayVerifier.class));
        testSpecifications.add(
                new TestSpecification<>(
                        "float-primitive-array-serializer",
                        flinkVersion,
                        PrimitiveArraySerializerUpgradeTestSpecifications.PrimitiveFloatArraySetup
                                .class,
                        PrimitiveArraySerializerUpgradeTestSpecifications
                                .PrimitiveFloatArrayVerifier.class));
        testSpecifications.add(
                new TestSpecification<>(
                        "int-primitive-array-serializer",
                        flinkVersion,
                        PrimitiveArraySerializerUpgradeTestSpecifications.PrimitiveIntArraySetup
                                .class,
                        PrimitiveArraySerializerUpgradeTestSpecifications.PrimitiveIntArrayVerifier
                                .class));
        testSpecifications.add(
                new TestSpecification<>(
                        "long-primitive-array-serializer",
                        flinkVersion,
                        PrimitiveArraySerializerUpgradeTestSpecifications.PrimitiveLongArraySetup
                                .class,
                        PrimitiveArraySerializerUpgradeTestSpecifications.PrimitiveLongArrayVerifier
                                .class));
        testSpecifications.add(
                new TestSpecification<>(
                        "short-primitive-array-serializer",
                        flinkVersion,
                        PrimitiveArraySerializerUpgradeTestSpecifications.PrimitiveShortArraySetup
                                .class,
                        PrimitiveArraySerializerUpgradeTestSpecifications
                                .PrimitiveShortArrayVerifier.class));
        testSpecifications.add(
                new TestSpecification<>(
                        "string-array-serializer",
                        flinkVersion,
                        PrimitiveArraySerializerUpgradeTestSpecifications.PrimitiveStringArraySetup
                                .class,
                        PrimitiveArraySerializerUpgradeTestSpecifications
                                .PrimitiveStringArrayVerifier.class));

        return testSpecifications;
    }
}
