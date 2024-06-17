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

package org.apache.flink.api.common.typeutils;

import org.assertj.core.api.Condition;

import java.util.function.Predicate;

/**
 * A Collection of useful {@link Condition}s for {@link TypeSerializer} and {@link
 * TypeSerializerSchemaCompatibility}.
 */
public final class TypeSerializerConditions {

    private TypeSerializerConditions() {}

    public static <T> Condition<TypeSerializerSchemaCompatibility<T>> isCompatibleAsIs() {
        return new Condition<>(
                TypeSerializerSchemaCompatibility::isCompatibleAsIs,
                "type serializer schema that is a compatible as is");
    }

    public static <T> Condition<TypeSerializerSchemaCompatibility<T>> isIncompatible() {
        return new Condition<>(
                TypeSerializerSchemaCompatibility::isIncompatible,
                "type serializer schema that is incompatible");
    }

    public static <T> Condition<TypeSerializerSchemaCompatibility<T>> isCompatibleAfterMigration() {
        return new Condition<>(
                TypeSerializerSchemaCompatibility::isCompatibleAfterMigration,
                "type serializer schema that is compatible after migration");
    }

    public static <T>
            Condition<TypeSerializerSchemaCompatibility<T>>
                    isCompatibleWithReconfiguredSerializer() {
        return new Condition<>(
                TypeSerializerSchemaCompatibility::isCompatibleWithReconfiguredSerializer,
                "type serializer schema that is compatible with a reconfigured serializer");
    }

    public static <T>
            Condition<TypeSerializerSchemaCompatibility<T>> isCompatibleWithReconfiguredSerializer(
                    Predicate<? super TypeSerializer<T>> reconfiguredSerializerMatcher) {
        return new Condition<>(
                compatibility ->
                        compatibility.isCompatibleWithReconfiguredSerializer()
                                && reconfiguredSerializerMatcher.test(
                                        compatibility.getReconfiguredSerializer()),
                "type serializer schema that is compatible with a reconfigured serializer matching "
                        + reconfiguredSerializerMatcher);
    }

    public static <T> Condition<TypeSerializerSchemaCompatibility<T>> hasSameCompatibilityAs(
            TypeSerializerSchemaCompatibility<T> expectedCompatibility) {
        return new Condition<>(
                actual ->
                        actual.isCompatibleAsIs() == expectedCompatibility.isCompatibleAsIs()
                                && actual.isIncompatible() == expectedCompatibility.isIncompatible()
                                && actual.isCompatibleAfterMigration()
                                        == expectedCompatibility.isCompatibleAfterMigration()
                                && actual.isCompatibleWithReconfiguredSerializer()
                                        == expectedCompatibility
                                                .isCompatibleWithReconfiguredSerializer(),
                "same compatibility as " + expectedCompatibility);
    }
}
