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
 * @param <T> the type of the state.
 */
@Internal
public abstract class StateSerializerProvider<T> {

	/**
	 * The registered serializer for the state.
	 *
	 * <p>In the case that this provider was created from a restored serializer snapshot via
	 * {@link #fromRestoredState(TypeSerializerSnapshot)}, but a new serializer was never registered
	 * for the state (i.e., this is the case if a restored state was never accessed), this would be {@code null}.
	 */
	@Nullable
	TypeSerializer<T> registeredSerializer;

	/**
	 * Creates a {@link StateSerializerProvider} for restored state from the previous serializer's snapshot.
	 *
	 * <p>Once a new serializer is registered for the state, it should be provided via
	 * the {@link #registerNewSerializerForRestoredState(TypeSerializer)} method.
	 *
	 * @param stateSerializerSnapshot the previous serializer's snapshot.
	 * @param <T> the type of the state.
	 *
	 * @return a new {@link StateSerializerProvider} for restored state.
	 */
	public static <T> StateSerializerProvider<T> fromRestoredState(TypeSerializerSnapshot<T> stateSerializerSnapshot) {
		return new RestoredStateSerializerProvider<>(stateSerializerSnapshot);
	}

	/**
	 * Creates a {@link StateSerializerProvider} for new state from the registered state serializer.
	 *
	 * @param registeredStateSerializer the new state's registered serializer.
	 * @param <T> the type of the state.
	 *
	 * @return a new {@link StateSerializerProvider} for new state.
	 */
	public static <T> StateSerializerProvider<T> fromNewState(TypeSerializer<T> registeredStateSerializer) {
		return new NewStateSerializerProvider<>(registeredStateSerializer);
	}

	private StateSerializerProvider(@Nullable TypeSerializer<T> stateSerializer) {
		this.registeredSerializer = stateSerializer;
	}

	/**
	 * Gets the serializer that recognizes the current serialization schema of the state.
	 * This is the serializer that should be used for regular state serialization and
	 * deserialization after state has been restored.
	 *
	 * <p>If this provider was created from a restored state's serializer snapshot, while a
	 * new serializer (with a new schema) was not registered for the state (i.e., because
	 * the state was never accessed after it was restored), then the schema of state remains
	 * identical. Therefore, in this case, it is guaranteed that the serializer returned by
	 * this method is the same as the one returned by {@link #previousSchemaSerializer()}.
	 *
	 * <p>If this provider was created from new state, then this always returns the
	 * serializer that the new state was registered with.
	 *
	 * @return a serializer that reads and writes in the current schema of the state.
	 */
	@Nonnull
	public abstract TypeSerializer<T> currentSchemaSerializer();

	/**
	 * Gets the serializer that recognizes the previous serialization schema of the state.
	 * This is the serializer that should be used for restoring the state, i.e. when the state
	 * is still in the previous serialization schema.
	 *
	 * <p>This method can only be used if this provider was created from a restored state's serializer
	 * snapshot. If this provider was created from new state, then this method is
	 * irrelevant, since there doesn't exist any previous version of the state schema.
	 *
	 * @return a serializer that reads and writes in the previous schema of the state.
	 */
	@Nonnull
	public abstract TypeSerializer<T> previousSchemaSerializer();

	/**
	 * For restored state, register a new serializer that potentially has a new serialization schema.
	 *
	 * <p>Users are allowed to register serializers for state only once. Therefore, this method
	 * is irrelevant if this provider was created from new state, since a state serializer had
	 * been registered already.
	 *
	 * <p>For the case where this provider was created from restored state, then this method should
	 * be called at most once. The new serializer will be checked for its schema compatibility with the
	 * previous serializer's schema, and returned to the caller. The caller is responsible for
	 * checking the result and react appropriately to it, as follows:
	 * <ul>
	 *     <li>{@link TypeSerializerSchemaCompatibility#isCompatibleAsIs()}: nothing needs to be done.
	 *     {@link #currentSchemaSerializer()} now returns the newly registered serializer.</li>
	 *     <li>{@link TypeSerializerSchemaCompatibility#isCompatibleAfterMigration()} ()}: state needs to be
	 *     migrated before the serializer returned by {@link #currentSchemaSerializer()} can be used.
	 *     The migration should be performed by reading the state with {@link #previousSchemaSerializer()},
	 *     and then writing it again with {@link #currentSchemaSerializer()}.</li>
	 *     <li>{@link TypeSerializerSchemaCompatibility#isIncompatible()}: the registered serializer is
	 *     incompatible. {@link #currentSchemaSerializer()} can no longer return a serializer for
	 *     the state, and therefore this provider shouldn't be used anymore.</li>
	 * </ul>
	 *
	 * @return the schema compatibility of the new registered serializer, with respect to the previous serializer.
	 */
	@Nonnull
	public abstract TypeSerializerSchemaCompatibility<T> registerNewSerializerForRestoredState(TypeSerializer<T> newSerializer);

	/**
	 * Implementation of the {@link StateSerializerProvider} for the restored state case.
	 */
	private static class RestoredStateSerializerProvider<T> extends StateSerializerProvider<T> {

		/**
		 * The snapshot of the previous serializer of the state.
		 */
		@Nonnull
		private final TypeSerializerSnapshot<T> previousSerializerSnapshot;

		private boolean isRegisteredWithIncompatibleSerializer = false;

		RestoredStateSerializerProvider(TypeSerializerSnapshot<T> previousSerializerSnapshot) {
			super(null);
			this.previousSerializerSnapshot = Preconditions.checkNotNull(previousSerializerSnapshot);
		}

		/**
		 * The restore serializer, lazily created only when the restore serializer is accessed.
		 *
		 * <p>NOTE: It is important to only create this lazily, so that off-heap
		 * state do not fail eagerly when restoring state that has a
		 * {@link UnloadableDummyTypeSerializer} as the previous serializer. This should
		 * be relevant only for restores from Flink versions prior to 1.7.x.
		 */
		@Nullable
		private TypeSerializer<T> cachedRestoredSerializer;

		@Override
		@Nonnull
		public TypeSerializer<T> currentSchemaSerializer() {
			if (registeredSerializer != null) {
				checkState(
					!isRegisteredWithIncompatibleSerializer,
					"Unable to provide a serializer with the current schema, because the restored state was " +
						"registered with a new serializer that has incompatible schema.");

					return registeredSerializer;
			}

			// if we are not yet registered with a new serializer,
			// we can just use the restore serializer to read / write the state.
			return previousSchemaSerializer();
		}

		@Nonnull
		public TypeSerializerSchemaCompatibility<T> registerNewSerializerForRestoredState(TypeSerializer<T> newSerializer) {
			checkNotNull(newSerializer);
			if (registeredSerializer != null) {
				throw new UnsupportedOperationException("A serializer has already been registered for the state; re-registration is not allowed.");
			}

			TypeSerializerSchemaCompatibility<T> result = previousSerializerSnapshot.resolveSchemaCompatibility(newSerializer);
			if (result.isIncompatible()) {
				this.isRegisteredWithIncompatibleSerializer = true;
			}
			this.registeredSerializer = newSerializer;
			return result;
		}

		@Nonnull
		public final TypeSerializer<T> previousSchemaSerializer() {
			if (cachedRestoredSerializer != null) {
				return cachedRestoredSerializer;
			}

			this.cachedRestoredSerializer = previousSerializerSnapshot.restoreSerializer();
			return cachedRestoredSerializer;
		}
	}

	/**
	 * Implementation of the {@link StateSerializerProvider} for the new state case.
	 */
	private static class NewStateSerializerProvider<T> extends StateSerializerProvider<T> {

		NewStateSerializerProvider(TypeSerializer<T> registeredStateSerializer) {
			super(Preconditions.checkNotNull(registeredStateSerializer));
		}

		@Override
		@Nonnull
		@SuppressWarnings("ConstantConditions")
		public TypeSerializer<T> currentSchemaSerializer() {
			return registeredSerializer;
		}

		@Override
		@Nonnull
		public TypeSerializerSchemaCompatibility<T> registerNewSerializerForRestoredState(TypeSerializer<T> newSerializer) {
			throw new UnsupportedOperationException("A serializer has already been registered for the state; re-registration is not allowed.");
		}

		@Override
		@Nonnull
		public TypeSerializer<T> previousSchemaSerializer() {
			throw new UnsupportedOperationException("This is a NewStateSerializerProvider; you cannot get a restore serializer because there was no restored state.");
		}
	}
}
