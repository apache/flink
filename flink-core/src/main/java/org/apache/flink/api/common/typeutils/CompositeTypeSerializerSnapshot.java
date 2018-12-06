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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeutils.base.GenericArraySerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.java.typeutils.runtime.EitherSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

/**
 * A {@link CompositeTypeSerializerSnapshot} is a convenient serializer snapshot class that can be used by
 * simple serializers which 1) delegates its serialization to multiple nested serializers, and 2) may contain
 * some extra static information that needs to be persisted as part of its snapshot.
 *
 * <p>Examples for this would be the {@link ListSerializer}, {@link MapSerializer}, {@link EitherSerializer}, etc.,
 * in which case the serializer, called the "outer" serializer in this context, has only some nested serializers that
 * needs to be persisted as its snapshot, and nothing else that needs to be persisted as the "outer" snapshot.
 * An example which has non-empty outer snapshots would be the {@link GenericArraySerializer}, which beyond the
 * nested component serializer, also contains a class of the component type that needs to be persisted.
 *
 * <p>Serializers that do have some outer snapshot needs to make sure to implement the methods
 * {@link #writeOuterSnapshot(DataOutputView)}, {@link #readOuterSnapshot(int, DataInputView, ClassLoader)}, and
 * {@link #isOuterSnapshotCompatible(TypeSerializer)} when using this class as the base for its serializer snapshot
 * class. By default, the base implementations of these methods are empty, i.e. this class assumes that
 * subclasses do not have any outer snapshot that needs to be persisted.
 *
 * @param <T> The data type that the originating serializer of this snapshot serializes.
 * @param <S> The type of the originating serializer.
 */
@PublicEvolving
public abstract class CompositeTypeSerializerSnapshot<T, S extends TypeSerializer> implements TypeSerializerSnapshot<T> {

	protected NestedSerializersSnapshotDelegate nestedSerializerSnapshots;

	private final Class<S> correspondingSerializerClass;

	/**
	 * Constructor to be used for read instantiation.
	 *
	 * @param correspondingSerializerClass the expected class of the new serializer.
	 */
	public CompositeTypeSerializerSnapshot(Class<S> correspondingSerializerClass) {
		this.correspondingSerializerClass = Preconditions.checkNotNull(correspondingSerializerClass);
	}

	/**
	 * Constructor to be used for writing the snapshot.
	 *
	 * @param serializerInstance an instance of the originating serializer of this snapshot.
	 */
	@SuppressWarnings("unchecked")
	public CompositeTypeSerializerSnapshot(S serializerInstance) {
		Preconditions.checkNotNull(serializerInstance);
		this.nestedSerializerSnapshots = new NestedSerializersSnapshotDelegate(getNestedSerializers(serializerInstance));
		this.correspondingSerializerClass = (Class<S>) serializerInstance.getClass();
	}

	@Override
	public final void writeSnapshot(DataOutputView out) throws IOException {
		writeOuterSnapshot(out);
		nestedSerializerSnapshots.writeNestedSerializerSnapshots(out);
	}

	@Override
	public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader) throws IOException {
		readOuterSnapshot(readVersion, in, userCodeClassLoader);
		this.nestedSerializerSnapshots = NestedSerializersSnapshotDelegate.readNestedSerializerSnapshots(in, userCodeClassLoader);
	}

	@Override
	public final TypeSerializerSchemaCompatibility<T> resolveSchemaCompatibility(TypeSerializer<T> newSerializer) {
		if (newSerializer.getClass() != correspondingSerializerClass) {
			return TypeSerializerSchemaCompatibility.incompatible();
		}

		S castedNewSerializer = correspondingSerializerClass.cast(newSerializer);

		// check that outer configuration is compatible; if not, short circuit result
		if (!isOuterSnapshotCompatible(castedNewSerializer)) {
			return TypeSerializerSchemaCompatibility.incompatible();
		}

		// since outer configuration is compatible, the final compatibility result depends only on the nested serializers
		return constructFinalSchemaCompatibilityResult(
			getNestedSerializers(castedNewSerializer),
			nestedSerializerSnapshots.getNestedSerializerSnapshots());
	}

	@Override
	public final TypeSerializer<T> restoreSerializer() {
		@SuppressWarnings("unchecked")
		TypeSerializer<T> serializer = (TypeSerializer<T>)
			createOuterSerializerWithNestedSerializers(nestedSerializerSnapshots.getRestoredNestedSerializers());

		return serializer;
	}

	// ------------------------------------------------------------------------------------------
	//  Outer serializer access methods
	// ------------------------------------------------------------------------------------------

	/**
	 * Gets the nested serializers from the outer serializer.
	 *
	 * @param outerSerializer the outer serializer.
	 *
	 * @return the nested serializers.
	 */
	protected abstract TypeSerializer<?>[] getNestedSerializers(S outerSerializer);

	/**
	 * Creates an instance of the outer serializer with a given array of its nested serializers.
	 *
	 * @param nestedSerializers array of nested serializers to create the outer serializer with.
	 *
	 * @return an instance of the outer serializer.
	 */
	protected abstract S createOuterSerializerWithNestedSerializers(TypeSerializer<?>[] nestedSerializers);

	// ------------------------------------------------------------------------------------------
	//  Outer snapshot methods; need to be overridden if outer snapshot is not empty,
	//  or in other words, the outer serializer has extra configuration beyond its nested serializers.
	// ------------------------------------------------------------------------------------------

	/**
	 * Writes the outer snapshot, i.e. any information beyond the nested serializers of the outer serializer.
	 *
	 * <p>The base implementation of this methods writes nothing, i.e. it assumes that the outer serializer
	 * only has nested serializers and no extra information. Otherwise, if the outer serializer contains
	 * some extra information that needs to be persisted as part of the serializer snapshot, this
	 * must be overridden. Note that this method and the corresponding methods
	 * {@link #readOuterSnapshot(int, DataInputView, ClassLoader)}, {@link #isOuterSnapshotCompatible(TypeSerializer)}
	 * needs to be implemented.
	 *
	 * @param out the {@link DataOutputView} to write the outer snapshot to.
	 */
	protected void writeOuterSnapshot(DataOutputView out) throws IOException {}

	/**
	 * Reads the outer snapshot, i.e. any information beyond the nested serializers of the outer serializer.
	 *
	 * <p>The base implementation of this methods reads nothing, i.e. it assumes that the outer serializer
	 * only has nested serializers and no extra information. Otherwise, if the outer serializer contains
	 * some extra information that has been persisted as part of the serializer snapshot, this
	 * must be overridden. Note that this method and the corresponding methods
	 * {@link #writeOuterSnapshot(DataOutputView)}, {@link #isOuterSnapshotCompatible(TypeSerializer)}
	 * needs to be implemented.
	 *
	 * @param readVersion the read version of the snapshot.
	 * @param in the {@link DataInputView} to read the outer snapshot from.
	 * @param userCodeClassLoader the user code class loader.
	 */
	protected void readOuterSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader) throws IOException {}

	/**
	 * Checks whether the outer snapshot is compatible with a given new serializer.
	 *
	 * <p>The base implementation of this method just returns {@code true}, i.e. it assumes that the outer serializer
	 * only has nested serializers and no extra information, and therefore the result of the check must always
	 * be true. Otherwise, if the outer serializer contains
	 * some extra information that has been persisted as part of the serializer snapshot, this
	 * must be overridden. Note that this method and the corresponding methods
	 * {@link #writeOuterSnapshot(DataOutputView)}, {@link #readOuterSnapshot(int, DataInputView, ClassLoader)}
	 * needs to be implemented.
	 *
	 * @param newSerializer the new serializer, which contains the new outer information to check against.
	 *
	 * @return a flag indicating whether or not the new serializer's outer information is compatible with the one
	 *         written in this snapshot.
	 */
	protected boolean isOuterSnapshotCompatible(S newSerializer) {
		return true;
	}

	// ------------------------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------------------------

	private TypeSerializerSchemaCompatibility<T> constructFinalSchemaCompatibilityResult(
			TypeSerializer<?>[] newNestedSerializers,
			TypeSerializerSnapshot<?>[] nestedSerializerSnapshots) {

		Preconditions.checkArgument(newNestedSerializers.length == nestedSerializerSnapshots.length,
			"Different number of new serializers and existing serializer snapshots.");

		TypeSerializer<?>[] reconfiguredNestedSerializers = new TypeSerializer[newNestedSerializers.length];

		// check nested serializers for compatibility
		boolean nestedSerializerRequiresMigration = false;
		boolean hasReconfiguredNestedSerializers = false;
		for (int i = 0; i < nestedSerializerSnapshots.length; i++) {
			TypeSerializerSchemaCompatibility<?> compatibility =
				resolveCompatibility(newNestedSerializers[i], nestedSerializerSnapshots[i]);

			// if any one of the new nested serializers is incompatible, we can just short circuit the result
			if (compatibility.isIncompatible()) {
				return TypeSerializerSchemaCompatibility.incompatible();
			}

			if (compatibility.isCompatibleAfterMigration()) {
				nestedSerializerRequiresMigration = true;
			} else if (compatibility.isCompatibleWithReconfiguredSerializer()) {
				hasReconfiguredNestedSerializers = true;
				reconfiguredNestedSerializers[i] = compatibility.getReconfiguredSerializer();
			} else if (compatibility.isCompatibleAsIs()) {
				reconfiguredNestedSerializers[i] = newNestedSerializers[i];
			} else {
				throw new IllegalStateException("Undefined compatibility type.");
			}
		}

		if (nestedSerializerRequiresMigration) {
			return TypeSerializerSchemaCompatibility.compatibleAfterMigration();
		}

		if (hasReconfiguredNestedSerializers) {
			@SuppressWarnings("unchecked")
			TypeSerializer<T> reconfiguredCompositeSerializer = createOuterSerializerWithNestedSerializers(reconfiguredNestedSerializers);
			return TypeSerializerSchemaCompatibility.compatibleWithReconfiguredSerializer(reconfiguredCompositeSerializer);
		}

		// ends up here if everything is compatible as is
		return TypeSerializerSchemaCompatibility.compatibleAsIs();
	}

	@SuppressWarnings("unchecked")
	private static <E> TypeSerializerSchemaCompatibility<E> resolveCompatibility(
		TypeSerializer<?> serializer,
		TypeSerializerSnapshot<?> snapshot) {

		TypeSerializer<E> typedSerializer = (TypeSerializer<E>) serializer;
		TypeSerializerSnapshot<E> typedSnapshot = (TypeSerializerSnapshot<E>) snapshot;

		return typedSnapshot.resolveSchemaCompatibility(typedSerializer);
	}
}
