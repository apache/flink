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

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** Utilities for the {@link CompositeTypeSerializerSnapshot}. */
@Internal
public class CompositeTypeSerializerUtil {

    /**
     * Delegates compatibility checks to a {@link CompositeTypeSerializerSnapshot} instance. This
     * can be used by legacy snapshot classes, which have a newer implementation implemented as a
     * {@link CompositeTypeSerializerSnapshot}.
     *
     * @param legacySerializerSnapshot the legacy serializer snapshot to check for compatibility.
     * @param newCompositeSnapshot an instance of the new snapshot class to delegate compatibility
     *     checks to. This instance should already contain the outer snapshot information.
     * @param legacyNestedSnapshots the nested serializer snapshots of the legacy composite
     *     snapshot.
     * @return the result compatibility.
     */
    public static <T> TypeSerializerSchemaCompatibility<T> delegateCompatibilityCheckToNewSnapshot(
            TypeSerializerSnapshot<T> legacySerializerSnapshot,
            CompositeTypeSerializerSnapshot<T, ? extends TypeSerializer<T>> newCompositeSnapshot,
            TypeSerializerSnapshot<?>... legacyNestedSnapshots) {

        checkArgument(legacyNestedSnapshots.length > 0);
        return newCompositeSnapshot.internalResolveSchemaCompatibility(
                legacySerializerSnapshot, legacyNestedSnapshots);
    }

    /**
     * Overrides the existing nested serializer's snapshots with the provided {@code
     * nestedSnapshots}.
     *
     * @param compositeSnapshot the composite snapshot to overwrite its nested serializers.
     * @param nestedSnapshots the nested snapshots to overwrite with.
     */
    public static void setNestedSerializersSnapshots(
            CompositeTypeSerializerSnapshot<?, ?> compositeSnapshot,
            TypeSerializerSnapshot<?>... nestedSnapshots) {

        NestedSerializersSnapshotDelegate delegate =
                new NestedSerializersSnapshotDelegate(nestedSnapshots);
        compositeSnapshot.setNestedSerializersSnapshotDelegate(delegate);
    }

    /**
     * Constructs an {@link IntermediateCompatibilityResult} with the given array of nested
     * serializers and their corresponding serializer snapshots.
     *
     * <p>This result is considered "intermediate", because the actual final result is not yet built
     * if it isn't defined. This is the case if the final result is supposed to be {@link
     * TypeSerializerSchemaCompatibility#compatibleWithReconfiguredSerializer(TypeSerializer)},
     * where construction of the reconfigured serializer instance should be done by the caller.
     *
     * <p>For other cases, i.e. {@link TypeSerializerSchemaCompatibility#compatibleAsIs()}, {@link
     * TypeSerializerSchemaCompatibility#compatibleAfterMigration()}, and {@link
     * TypeSerializerSchemaCompatibility#incompatible()}, these results are considered final.
     *
     * @param newNestedSerializerSnapshots the new nested serializer snapshots to check for
     *     compatibility.
     * @param oldNestedSerializerSnapshots the associated previous nested serializers' snapshots.
     * @return the intermediate compatibility result of the new nested serializer snapshots.
     */
    public static <T> IntermediateCompatibilityResult<T> constructIntermediateCompatibilityResult(
            TypeSerializerSnapshot<?>[] newNestedSerializerSnapshots,
            TypeSerializerSnapshot<?>[] oldNestedSerializerSnapshots) {

        Preconditions.checkArgument(
                newNestedSerializerSnapshots.length == oldNestedSerializerSnapshots.length,
                "Different number of new serializer snapshots and existing serializer snapshots.");

        TypeSerializer<?>[] nestedSerializers =
                new TypeSerializer[newNestedSerializerSnapshots.length];

        // check nested serializers for compatibility
        boolean nestedSerializerRequiresTtlMigration = false;
        boolean nestedSerializerRequiresMigration = false;
        boolean hasReconfiguredNestedSerializers = false;
        for (int i = 0; i < oldNestedSerializerSnapshots.length; i++) {
            TypeSerializerSchemaCompatibility<?> compatibility =
                    resolveCompatibility(
                            newNestedSerializerSnapshots[i], oldNestedSerializerSnapshots[i]);

            // if any one of the new nested serializers is incompatible, we can just short circuit
            // the result
            if (compatibility.isIncompatible()) {
                return IntermediateCompatibilityResult.definedIncompatibleResult();
            }

            if (compatibility.isCompatibleAfterTtlMigration()) {
                nestedSerializerRequiresTtlMigration = true;
            } else if (compatibility.isCompatibleAfterMigration()) {
                nestedSerializerRequiresMigration = true;
            } else if (compatibility.isCompatibleWithReconfiguredSerializer()) {
                hasReconfiguredNestedSerializers = true;
                nestedSerializers[i] = compatibility.getReconfiguredSerializer();
            } else if (compatibility.isCompatibleAsIs()) {
                nestedSerializers[i] = newNestedSerializerSnapshots[i].restoreSerializer();
            } else {
                throw new IllegalStateException("Undefined compatibility type.");
            }
        }

        if (nestedSerializerRequiresTtlMigration) {
            return IntermediateCompatibilityResult.definedCompatibleAfterTtlMigrationResult();
        }

        if (nestedSerializerRequiresMigration) {
            return IntermediateCompatibilityResult.definedCompatibleAfterMigrationResult();
        }

        if (hasReconfiguredNestedSerializers) {
            return IntermediateCompatibilityResult.undefinedReconfigureResult(nestedSerializers);
        }

        // ends up here if everything is compatible as is
        return IntermediateCompatibilityResult.definedCompatibleAsIsResult(nestedSerializers);
    }

    public static class IntermediateCompatibilityResult<T> {

        private final TypeSerializerSchemaCompatibility.Type compatibilityType;
        private final TypeSerializer<?>[] nestedSerializers;

        static <T> IntermediateCompatibilityResult<T> definedCompatibleAsIsResult(
                TypeSerializer<?>[] originalSerializers) {
            return new IntermediateCompatibilityResult<>(
                    TypeSerializerSchemaCompatibility.Type.COMPATIBLE_AS_IS, originalSerializers);
        }

        static <T> IntermediateCompatibilityResult<T> definedIncompatibleResult() {
            return new IntermediateCompatibilityResult<>(
                    TypeSerializerSchemaCompatibility.Type.INCOMPATIBLE, null);
        }

        static <T> IntermediateCompatibilityResult<T> definedCompatibleAfterTtlMigrationResult() {
            return new IntermediateCompatibilityResult<>(
                    TypeSerializerSchemaCompatibility.Type.COMPATIBLE_AFTER_TTL_MIGRATION, null);
        }

        static <T> IntermediateCompatibilityResult<T> definedCompatibleAfterMigrationResult() {
            return new IntermediateCompatibilityResult<>(
                    TypeSerializerSchemaCompatibility.Type.COMPATIBLE_AFTER_MIGRATION, null);
        }

        static <T> IntermediateCompatibilityResult<T> undefinedReconfigureResult(
                TypeSerializer<?>[] reconfiguredNestedSerializers) {
            return new IntermediateCompatibilityResult<>(
                    TypeSerializerSchemaCompatibility.Type.COMPATIBLE_WITH_RECONFIGURED_SERIALIZER,
                    reconfiguredNestedSerializers);
        }

        private IntermediateCompatibilityResult(
                TypeSerializerSchemaCompatibility.Type compatibilityType,
                TypeSerializer<?>[] nestedSerializers) {
            this.compatibilityType = checkNotNull(compatibilityType);
            this.nestedSerializers = nestedSerializers;
        }

        public boolean isCompatibleWithReconfiguredSerializer() {
            return compatibilityType
                    == TypeSerializerSchemaCompatibility.Type
                            .COMPATIBLE_WITH_RECONFIGURED_SERIALIZER;
        }

        public boolean isCompatibleAsIs() {
            return compatibilityType == TypeSerializerSchemaCompatibility.Type.COMPATIBLE_AS_IS;
        }

        public boolean isCompatibleAfterMigration() {
            return compatibilityType
                    == TypeSerializerSchemaCompatibility.Type.COMPATIBLE_AFTER_MIGRATION;
        }

        public boolean isCompatibleAfterTtlMigration() {
            return compatibilityType
                    == TypeSerializerSchemaCompatibility.Type.COMPATIBLE_AFTER_TTL_MIGRATION;
        }

        public boolean isIncompatible() {
            return compatibilityType == TypeSerializerSchemaCompatibility.Type.INCOMPATIBLE;
        }

        public TypeSerializerSchemaCompatibility<T> getFinalResult() {
            checkState(
                    compatibilityType
                            != TypeSerializerSchemaCompatibility.Type
                                    .COMPATIBLE_WITH_RECONFIGURED_SERIALIZER,
                    "unable to build final result if intermediate compatibility type is COMPATIBLE_WITH_RECONFIGURED_SERIALIZER.");
            switch (compatibilityType) {
                case COMPATIBLE_AS_IS:
                    return TypeSerializerSchemaCompatibility.compatibleAsIs();
                case COMPATIBLE_AFTER_MIGRATION:
                    return TypeSerializerSchemaCompatibility.compatibleAfterMigration();
                case INCOMPATIBLE:
                    return TypeSerializerSchemaCompatibility.incompatible();
                default:
                    throw new IllegalStateException("unrecognized compatibility type.");
            }
        }

        public TypeSerializer<?>[] getNestedSerializers() {
            checkState(
                    compatibilityType == TypeSerializerSchemaCompatibility.Type.COMPATIBLE_AS_IS
                            || compatibilityType
                                    == TypeSerializerSchemaCompatibility.Type
                                            .COMPATIBLE_WITH_RECONFIGURED_SERIALIZER,
                    "only intermediate compatibility types COMPATIBLE_AS_IS and COMPATIBLE_WITH_RECONFIGURED_SERIALIZER have nested serializers.");
            return nestedSerializers;
        }
    }

    @SuppressWarnings("unchecked")
    private static <E> TypeSerializerSchemaCompatibility<E> resolveCompatibility(
            TypeSerializerSnapshot<?> newSnapshot, TypeSerializerSnapshot<?> oldSnapshot) {

        TypeSerializerSnapshot<E> typedNewSnapshot = (TypeSerializerSnapshot<E>) newSnapshot;
        TypeSerializerSnapshot<E> typedOldSnapshot = (TypeSerializerSnapshot<E>) oldSnapshot;

        return typedNewSnapshot.resolveSchemaCompatibility(typedOldSnapshot);
    }
}
