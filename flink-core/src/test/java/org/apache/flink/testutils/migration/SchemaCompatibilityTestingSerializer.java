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

package org.apache.flink.testutils.migration;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import javax.annotation.Nullable;

import java.util.Objects;
import java.util.function.Function;

/**
 * The {@link SchemaCompatibilityTestingSerializer} is a mock serializer that can be used for schema
 * compatibility and serializer upgrade related tests.
 *
 * <p>To test serializers compatibility we can start by either obtaining a {@link
 * TypeSerializerSnapshot} and restoring a {@link TypeSerializer} or the other way around, starting
 * with a serializer and calling {@link TypeSerializer#snapshotConfiguration()} to obtain a snapshot
 * class.
 *
 * <p>To start from a snapshot, the class {@link SchemaCompatibilityTestingSnapshot} can be
 * configured to return a predefined {@link TypeSerializerSchemaCompatibility} result when {@link
 * TypeSerializerSnapshot#resolveSchemaCompatibility(TypeSerializer)} would be called, the following
 * static factory methods return a pre-configured snapshot class:
 *
 * <ul>
 *   <li>{@code thatIsCompatibleWithNextSerializer}.
 *   <li>{@code thatIsCompatibleWithNextSerializerAfterReconfiguration}
 *   <li>{@code thatIsCompatibleWithNextSerializerAfterMigration}
 *   <li>{@code thatIsIncompatibleWithTheNextSerializer}
 * </ul>
 *
 * <p>Here is a simple test example.
 *
 * <pre>{@code
 * @Test
 * public void example() {
 *     TypeSerializerSnapshot<?> snapshot = SchemaCompatibilityTestingSnapshot.thatIsCompatibleWithNextSerializer();
 *
 *     TypeSerializerSchemaCompatibility<?> result = snapshot.resolveSchemaCompatibility(new SchemaCompatibilityTestingSerializer());
 *
 *     assertTrue(result.compatibleAsIs());
 * }
 * }</pre>
 *
 * <p>To start from a serializer, simply create a new instance of {@code
 * SchemaCompatibilityTestingSerializer} and call {@link
 * SchemaCompatibilityTestingSerializer#snapshotConfiguration()} to obtain a {@link
 * SchemaCompatibilityTestingSnapshot}. To control the behaviour of the returned snapshot it is
 * possible to pass a function from a {@code newSerializer} to a {@link
 * TypeSerializerSchemaCompatibility}.
 *
 * <p>It is also possible to pass in a {@code String} identifier when constructing a snapshot or a
 * serializer to tie a specific snapshot instance to a specific serializer instance, this might be
 * useful when testing composite serializers.
 */
@SuppressWarnings({"WeakerAccess", "serial"})
public final class SchemaCompatibilityTestingSerializer extends TypeSerializer<Integer> {

    private static final long serialVersionUID = 2588814752302505240L;

    private final Function<TypeSerializer<Integer>, TypeSerializerSchemaCompatibility<Integer>>
            resolver;

    @Nullable private final String tokenForEqualityChecks;

    public SchemaCompatibilityTestingSerializer() {
        this(null, ALWAYS_COMPATIBLE);
    }

    public SchemaCompatibilityTestingSerializer(String tokenForEqualityChecks) {
        this(tokenForEqualityChecks, ALWAYS_COMPATIBLE);
    }

    public SchemaCompatibilityTestingSerializer(
            @Nullable String tokenForEqualityChecks,
            Function<TypeSerializer<Integer>, TypeSerializerSchemaCompatibility<Integer>>
                    resolver) {
        this.resolver = resolver;
        this.tokenForEqualityChecks = tokenForEqualityChecks;
    }

    @Override
    public boolean equals(Object obj) {
        return (obj instanceof SchemaCompatibilityTestingSerializer)
                && Objects.equals(
                        tokenForEqualityChecks,
                        ((SchemaCompatibilityTestingSerializer) obj).tokenForEqualityChecks);
    }

    @Override
    public int hashCode() {
        return Objects.hash(getClass(), tokenForEqualityChecks);
    }

    @Override
    public String toString() {
        return "SchemaCompatibilityTestingSerializer{"
                + "tokenForEqualityChecks='"
                + tokenForEqualityChecks
                + '\''
                + '}';
    }

    @Override
    public TypeSerializerSnapshot<Integer> snapshotConfiguration() {
        return new SchemaCompatibilityTestingSnapshot(tokenForEqualityChecks, resolver);
    }

    // -----------------------------------------------------------------------------------------------------------
    // Serialization related methods are not supported
    // -----------------------------------------------------------------------------------------------------------

    @Override
    public boolean isImmutableType() {
        throw new UnsupportedOperationException();
    }

    @Override
    public TypeSerializer<Integer> duplicate() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Integer createInstance() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Integer copy(Integer from) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Integer copy(Integer from, Integer reuse) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getLength() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void serialize(Integer record, DataOutputView target) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Integer deserialize(DataInputView source) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Integer deserialize(Integer reuse, DataInputView source) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) {
        throw new UnsupportedOperationException();
    }

    // -----------------------------------------------------------------------------------------------------------
    // Utils
    // -----------------------------------------------------------------------------------------------------------

    private static final Function<
                    TypeSerializer<Integer>, TypeSerializerSchemaCompatibility<Integer>>
            ALWAYS_COMPATIBLE = unused -> TypeSerializerSchemaCompatibility.compatibleAsIs();

    // -----------------------------------------------------------------------------------------------------------
    // Snapshot class
    // -----------------------------------------------------------------------------------------------------------

    /** A configurable {@link TypeSerializerSnapshot} for this serializer. */
    @SuppressWarnings("WeakerAccess")
    public static final class SchemaCompatibilityTestingSnapshot
            implements TypeSerializerSnapshot<Integer> {

        public static SchemaCompatibilityTestingSnapshot thatIsCompatibleWithNextSerializer() {
            return thatIsCompatibleWithNextSerializer(null);
        }

        public static SchemaCompatibilityTestingSnapshot thatIsCompatibleWithNextSerializer(
                String tokenForEqualityChecks) {
            return new SchemaCompatibilityTestingSnapshot(
                    tokenForEqualityChecks,
                    unused -> TypeSerializerSchemaCompatibility.compatibleAsIs());
        }

        public static SchemaCompatibilityTestingSnapshot
                thatIsCompatibleWithNextSerializerAfterReconfiguration() {
            return thatIsCompatibleWithNextSerializerAfterReconfiguration(null);
        }

        public static SchemaCompatibilityTestingSnapshot
                thatIsCompatibleWithNextSerializerAfterReconfiguration(
                        String tokenForEqualityChecks) {
            SchemaCompatibilityTestingSerializer reconfiguredSerializer =
                    new SchemaCompatibilityTestingSerializer(
                            tokenForEqualityChecks, ALWAYS_COMPATIBLE);
            return new SchemaCompatibilityTestingSnapshot(
                    tokenForEqualityChecks,
                    unused ->
                            TypeSerializerSchemaCompatibility.compatibleWithReconfiguredSerializer(
                                    reconfiguredSerializer));
        }

        public static SchemaCompatibilityTestingSnapshot
                thatIsCompatibleWithNextSerializerAfterMigration() {
            return thatIsCompatibleWithNextSerializerAfterMigration(null);
        }

        public static SchemaCompatibilityTestingSnapshot
                thatIsCompatibleWithNextSerializerAfterMigration(String tokenForEqualityChecks) {
            return new SchemaCompatibilityTestingSnapshot(
                    tokenForEqualityChecks,
                    unused -> TypeSerializerSchemaCompatibility.compatibleAfterMigration());
        }

        public static SchemaCompatibilityTestingSnapshot thatIsIncompatibleWithTheNextSerializer() {
            return thatIsIncompatibleWithTheNextSerializer(null);
        }

        public static SchemaCompatibilityTestingSnapshot thatIsIncompatibleWithTheNextSerializer(
                String tokenForEqualityChecks) {
            return new SchemaCompatibilityTestingSnapshot(
                    tokenForEqualityChecks,
                    unused -> TypeSerializerSchemaCompatibility.incompatible());
        }

        @Nullable private final String tokenForEqualityChecks;
        private final Function<TypeSerializer<Integer>, TypeSerializerSchemaCompatibility<Integer>>
                resolver;

        SchemaCompatibilityTestingSnapshot(
                @Nullable String tokenForEqualityChecks,
                Function<TypeSerializer<Integer>, TypeSerializerSchemaCompatibility<Integer>>
                        resolver) {
            this.tokenForEqualityChecks = tokenForEqualityChecks;
            this.resolver = resolver;
        }

        @Override
        public TypeSerializerSchemaCompatibility<Integer> resolveSchemaCompatibility(
                TypeSerializer<Integer> newSerializer) {
            if (!(newSerializer instanceof SchemaCompatibilityTestingSerializer)) {
                return TypeSerializerSchemaCompatibility.incompatible();
            }
            SchemaCompatibilityTestingSerializer schemaCompatibilityTestingSerializer =
                    (SchemaCompatibilityTestingSerializer) newSerializer;
            if (!(Objects.equals(
                    schemaCompatibilityTestingSerializer.tokenForEqualityChecks,
                    tokenForEqualityChecks))) {
                return TypeSerializerSchemaCompatibility.incompatible();
            }
            return resolver.apply(newSerializer);
        }

        @Override
        public int getCurrentVersion() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeSnapshot(DataOutputView out) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void readSnapshot(
                int readVersion, DataInputView in, ClassLoader userCodeClassLoader) {
            throw new UnsupportedOperationException();
        }

        @Override
        public TypeSerializer<Integer> restoreSerializer() {
            return new SchemaCompatibilityTestingSerializer(tokenForEqualityChecks, resolver);
        }
    }
}
