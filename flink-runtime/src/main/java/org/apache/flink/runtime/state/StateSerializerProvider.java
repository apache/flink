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

package org.apache.flink.runtime.state;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.UnloadableDummyTypeSerializer;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link StateSerializerProvider} wraps logic on how to obtain serializers for registered state,
 * either with the previous schema of state in checkpoints or the current schema of state.
 *
 * <p>A provider can be created from either a registered state serializer, or the snapshot of the
 * previous state serializer. For the former case, if the state was restored and a snapshot of the
 * previous state serializer was retrieved later on, the snapshot can be set on the provider which
 * also additionally checks the compatibility of the initially registered serializer. Similarly for
 * the latter case, if a new state serializer is registered later on, it can be set on the provider,
 * which then also checks the compatibility of the new registered serializer.
 *
 * <p>Simply put, the provider works both directions - either creating it first with a registered
 * serializer or the previous serializer's snapshot, and then setting the previous serializer's
 * snapshot (if the provider was created with a registered serializer) or a new registered state
 * serializer (if the provider was created with a serializer snapshot). Either way, the new
 * registered serializer is checked for schema compatibility once both the new serializer and the
 * previous serializer snapshot is present.
 *
 * @param <T> the type of the state.
 */
@Internal
public abstract class StateSerializerProvider<T> {

    /**
     * The registered serializer for the state.
     *
     * <p>In the case that this provider was created from a restored serializer snapshot via {@link
     * #fromPreviousSerializerSnapshot(TypeSerializerSnapshot)}, but a new serializer was never
     * registered for the state (i.e., this is the case if a restored state was never accessed),
     * this would be {@code null}.
     */
    @Nullable TypeSerializer<T> registeredSerializer;

    /**
     * The state's previous serializer's snapshot.
     *
     * <p>In the case that this provider was created from a registered state serializer instance via
     * {@link #fromNewRegisteredSerializer(TypeSerializer)}, but a serializer snapshot was never
     * supplied to this provider (i.e. because the registered serializer was for a new state, not a
     * restored one), this would be {@code null}.
     */
    @Nullable TypeSerializerSnapshot<T> previousSerializerSnapshot;

    /**
     * The restore serializer, lazily created only when the restore serializer is accessed.
     *
     * <p>NOTE: It is important to only create this lazily, so that off-heap state do not fail
     * eagerly when restoring state that has a {@link UnloadableDummyTypeSerializer} as the previous
     * serializer. This should be relevant only for restores from Flink versions prior to 1.7.x.
     */
    @Nullable private TypeSerializer<T> cachedRestoredSerializer;

    private boolean isRegisteredWithIncompatibleSerializer = false;

    /**
     * Creates a {@link StateSerializerProvider} for restored state from the previous serializer's
     * snapshot.
     *
     * <p>Once a new serializer is registered for the state, it should be provided via the {@link
     * #registerNewSerializerForRestoredState(TypeSerializer)} method.
     *
     * @param stateSerializerSnapshot the previous serializer's snapshot.
     * @param <T> the type of the state.
     * @return a new {@link StateSerializerProvider}.
     */
    public static <T> StateSerializerProvider<T> fromPreviousSerializerSnapshot(
            TypeSerializerSnapshot<T> stateSerializerSnapshot) {
        return new LazilyRegisteredStateSerializerProvider<>(stateSerializerSnapshot);
    }

    /**
     * Creates a {@link StateSerializerProvider} from the registered state serializer.
     *
     * <p>If the state is a restored one, and the previous serializer's snapshot is obtained later
     * on, is should be supplied via the {@link
     * #setPreviousSerializerSnapshotForRestoredState(TypeSerializerSnapshot)} method.
     *
     * @param registeredStateSerializer the new state's registered serializer.
     * @param <T> the type of the state.
     * @return a new {@link StateSerializerProvider}.
     */
    public static <T> StateSerializerProvider<T> fromNewRegisteredSerializer(
            TypeSerializer<T> registeredStateSerializer) {
        return new EagerlyRegisteredStateSerializerProvider<>(registeredStateSerializer);
    }

    private StateSerializerProvider(@Nonnull TypeSerializer<T> stateSerializer) {
        this.registeredSerializer = stateSerializer;
        this.previousSerializerSnapshot = null;
    }

    private StateSerializerProvider(@Nonnull TypeSerializerSnapshot<T> previousSerializerSnapshot) {
        this.previousSerializerSnapshot = previousSerializerSnapshot;
        this.registeredSerializer = null;
    }

    /**
     * Gets the serializer that recognizes the current serialization schema of the state. This is
     * the serializer that should be used for regular state serialization and deserialization after
     * state has been restored.
     *
     * <p>If this provider was created from a restored state's serializer snapshot, while a new
     * serializer (with a new schema) was not registered for the state (i.e., because the state was
     * never accessed after it was restored), then the schema of state remains identical. Therefore,
     * in this case, it is guaranteed that the serializer returned by this method is the same as the
     * one returned by {@link #previousSchemaSerializer()}.
     *
     * <p>If this provider was created from a serializer instance, then this always returns the that
     * same serializer instance. If later on a snapshot of the previous serializer is supplied via
     * {@link #setPreviousSerializerSnapshotForRestoredState(TypeSerializerSnapshot)}, then the
     * initially supplied serializer instance will be checked for compatibility.
     *
     * @return a serializer that reads and writes in the current schema of the state.
     */
    @Nonnull
    public final TypeSerializer<T> currentSchemaSerializer() {
        if (registeredSerializer != null) {
            checkState(
                    !isRegisteredWithIncompatibleSerializer,
                    "Unable to provide a serializer with the current schema, because the restored state was "
                            + "registered with a new serializer that has incompatible schema.");

            return registeredSerializer;
        }

        // if we are not yet registered with a new serializer,
        // we can just use the restore serializer to read / write the state.
        return previousSchemaSerializer();
    }

    /**
     * Gets the serializer that recognizes the previous serialization schema of the state. This is
     * the serializer that should be used for restoring the state, i.e. when the state is still in
     * the previous serialization schema.
     *
     * <p>This method only returns a serializer if this provider has the previous serializer's
     * snapshot. Otherwise, trying to access the previous schema serializer will fail with an
     * exception.
     *
     * @return a serializer that reads and writes in the previous schema of the state.
     */
    @Nonnull
    public final TypeSerializer<T> previousSchemaSerializer() {
        if (cachedRestoredSerializer != null) {
            return cachedRestoredSerializer;
        }

        if (previousSerializerSnapshot == null) {
            throw new UnsupportedOperationException(
                    "This provider does not contain the state's previous serializer's snapshot. Cannot provider a serializer for previous schema.");
        }

        this.cachedRestoredSerializer = previousSerializerSnapshot.restoreSerializer();
        return cachedRestoredSerializer;
    }

    /**
     * Gets the previous serializer snapshot.
     *
     * @return The previous serializer snapshot, or null if registered serializer was for a new
     *     state, not a restored one.
     */
    @Nullable
    public final TypeSerializerSnapshot<T> getPreviousSerializerSnapshot() {
        return previousSerializerSnapshot;
    }

    /**
     * For restored state, register a new serializer that potentially has a new serialization
     * schema.
     *
     * <p>Users are allowed to register serializers for state only once. Therefore, this method is
     * irrelevant if this provider was created with a serializer instance, since a state serializer
     * had been registered already.
     *
     * <p>For the case where this provider was created from a serializer snapshot, then this method
     * should be called at most once. The new serializer will be checked for its schema
     * compatibility with the previous serializer's schema, and returned to the caller. The caller
     * is responsible for checking the result and react appropriately to it, as follows:
     *
     * <ul>
     *   <li>{@link TypeSerializerSchemaCompatibility#isCompatibleAsIs()}: nothing needs to be done.
     *       {@link #currentSchemaSerializer()} now returns the newly registered serializer.
     *   <li>{@link TypeSerializerSchemaCompatibility#isCompatibleAfterMigration()}: state needs to
     *       be migrated before the serializer returned by {@link #currentSchemaSerializer()} can be
     *       used. The migration should be performed by reading the state with {@link
     *       #previousSchemaSerializer()}, and then writing it again with {@link
     *       #currentSchemaSerializer()}.
     *   <li>{@link TypeSerializerSchemaCompatibility#isIncompatible()}: the registered serializer
     *       is incompatible. {@link #currentSchemaSerializer()} can no longer return a serializer
     *       for the state, and therefore this provider shouldn't be used anymore.
     * </ul>
     *
     * @return the schema compatibility of the new registered serializer, with respect to the
     *     previous serializer.
     */
    @Nonnull
    public abstract TypeSerializerSchemaCompatibility<T> registerNewSerializerForRestoredState(
            TypeSerializer<T> newSerializer);

    /**
     * For restored state, set the state's previous serializer's snapshot.
     *
     * <p>Users are allowed to set the previous serializer's snapshot once. Therefore, this method
     * is irrelevant if this provider was created with a serializer snapshot, since the serializer
     * snapshot had been set already.
     *
     * <p>For the case where this provider was created from a serializer instance, then this method
     * should be called at most once. The initially registered state serializer will be checked for
     * its schema compatibility with the previous serializer's schema, and returned to the caller.
     * The caller is responsible for checking the result and react appropriately to it, as follows:
     *
     * <ul>
     *   <li>{@link TypeSerializerSchemaCompatibility#isCompatibleAsIs()}: nothing needs to be done.
     *       {@link #currentSchemaSerializer()} remains to return the initially registered
     *       serializer.
     *   <li>{@link TypeSerializerSchemaCompatibility#isCompatibleAfterMigration()}: state needs to
     *       be migrated before the serializer returned by {@link #currentSchemaSerializer()} can be
     *       used. The migration should be performed by reading the state with {@link
     *       #previousSchemaSerializer()}, and then writing it again with {@link
     *       #currentSchemaSerializer()}.
     *   <li>{@link TypeSerializerSchemaCompatibility#isIncompatible()}: the registered serializer
     *       is incompatible. {@link #currentSchemaSerializer()} can no longer return a serializer
     *       for the state, and therefore this provider shouldn't be used anymore.
     * </ul>
     *
     * @param previousSerializerSnapshot the state's previous serializer's snapshot
     * @return the schema compatibility of the initially registered serializer, with respect to the
     *     previous serializer.
     */
    @Nonnull
    public abstract TypeSerializerSchemaCompatibility<T>
            setPreviousSerializerSnapshotForRestoredState(
                    TypeSerializerSnapshot<T> previousSerializerSnapshot);

    /**
     * Invalidates access to the current schema serializer. This lets {@link
     * #currentSchemaSerializer()} fail when invoked.
     *
     * <p>Access to the current schema serializer should be invalidated by the methods {@link
     * #registerNewSerializerForRestoredState(TypeSerializer)} or {@link
     * #setPreviousSerializerSnapshotForRestoredState(TypeSerializerSnapshot)} once the registered
     * serializer is determined to be incompatible.
     */
    protected final void invalidateCurrentSchemaSerializerAccess() {
        this.isRegisteredWithIncompatibleSerializer = true;
    }

    /**
     * Implementation of the {@link StateSerializerProvider} for the case where a snapshot of the
     * previous serializer is obtained before a new state serializer is registered (hence, the
     * naming "lazily" registered).
     */
    private static class LazilyRegisteredStateSerializerProvider<T>
            extends StateSerializerProvider<T> {

        LazilyRegisteredStateSerializerProvider(
                TypeSerializerSnapshot<T> previousSerializerSnapshot) {
            super(Preconditions.checkNotNull(previousSerializerSnapshot));
        }

        @Nonnull
        @Override
        @SuppressWarnings("ConstantConditions")
        public TypeSerializerSchemaCompatibility<T> registerNewSerializerForRestoredState(
                TypeSerializer<T> newSerializer) {
            checkNotNull(newSerializer);
            if (registeredSerializer != null) {
                throw new UnsupportedOperationException(
                        "A serializer has already been registered for the state; re-registration is not allowed.");
            }

            TypeSerializerSchemaCompatibility<T> result =
                    previousSerializerSnapshot.resolveSchemaCompatibility(newSerializer);
            if (result.isIncompatible()) {
                invalidateCurrentSchemaSerializerAccess();
            }
            if (result.isCompatibleWithReconfiguredSerializer()) {
                this.registeredSerializer = result.getReconfiguredSerializer();
            } else {
                this.registeredSerializer = newSerializer;
            }
            return result;
        }

        @Nonnull
        @Override
        public TypeSerializerSchemaCompatibility<T> setPreviousSerializerSnapshotForRestoredState(
                TypeSerializerSnapshot<T> previousSerializerSnapshot) {
            throw new UnsupportedOperationException(
                    "The snapshot of the state's previous serializer has already been set; cannot reset.");
        }
    }

    /**
     * Implementation of the {@link StateSerializerProvider} for the case where a new state
     * serializer instance is registered first, before any snapshots of the previous state
     * serializer is obtained (hence, the naming "eagerly" registered).
     */
    private static class EagerlyRegisteredStateSerializerProvider<T>
            extends StateSerializerProvider<T> {

        EagerlyRegisteredStateSerializerProvider(TypeSerializer<T> registeredStateSerializer) {
            super(Preconditions.checkNotNull(registeredStateSerializer));
        }

        @Nonnull
        @Override
        public TypeSerializerSchemaCompatibility<T> registerNewSerializerForRestoredState(
                TypeSerializer<T> newSerializer) {
            throw new UnsupportedOperationException(
                    "A serializer has already been registered for the state; re-registration is not allowed.");
        }

        @Nonnull
        @Override
        public TypeSerializerSchemaCompatibility<T> setPreviousSerializerSnapshotForRestoredState(
                TypeSerializerSnapshot<T> previousSerializerSnapshot) {
            checkNotNull(previousSerializerSnapshot);
            if (this.previousSerializerSnapshot != null) {
                throw new UnsupportedOperationException(
                        "The snapshot of the state's previous serializer has already been set; cannot reset.");
            }

            this.previousSerializerSnapshot = previousSerializerSnapshot;

            TypeSerializerSchemaCompatibility<T> result =
                    previousSerializerSnapshot.resolveSchemaCompatibility(registeredSerializer);
            if (result.isIncompatible()) {
                invalidateCurrentSchemaSerializerAccess();
            }
            if (result.isCompatibleWithReconfiguredSerializer()) {
                this.registeredSerializer = result.getReconfiguredSerializer();
            }
            return result;
        }
    }
}
