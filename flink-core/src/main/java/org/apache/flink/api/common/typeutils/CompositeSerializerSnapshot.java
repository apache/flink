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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * A CompositeSerializerSnapshot represents the snapshots of multiple serializers that are used
 * by an outer serializer. Examples would be tuples, where the outer serializer is the tuple
 * format serializer, an the CompositeSerializerSnapshot holds the serializers for the
 * different tuple fields.
 *
 * <p>The serializers and their snapshots are always passed in the order in which they are
 * first given to the constructor.
 *
 * <p>The CompositeSerializerSnapshot has its own versioning internally, it does not couple its
 * versioning to the versioning of the TypeSerializerSnapshot that builds on top of this class.
 * That way, the CompositeSerializerSnapshot and enclosing TypeSerializerSnapshot the can evolve
 * their formats independently.
 */
@Internal
public abstract class CompositeSerializerSnapshot<T, S extends TypeSerializer<T>> implements TypeSerializerSnapshot<T> {

	/** Magic number for integrity checks during deserialization. */
	private static final int MAGIC_NUMBER = 1333245;

	/** Current version of the new serialization format. */
	private static final int VERSION = 1;

	/** The snapshots of the serializers in the composition, in order. */
	private TypeSerializerSnapshot<?>[] nestedSnapshots;

	/**
	 * Constructor for read instantiation.
	 */
	protected CompositeSerializerSnapshot() {}

	/**
	 * Constructor for writing snapshot.
	 */
	protected CompositeSerializerSnapshot(TypeSerializer<?>... serializers) {
		this.nestedSnapshots = TypeSerializerUtils.snapshotBackwardsCompatible(serializers);
	}

	// ------------------------------------------------------------------------
	//  Methods to bridge between outer serializer and composition
	//  of nested serializers
	// ------------------------------------------------------------------------

	protected abstract TypeSerializer<T> createSerializer(TypeSerializer<?>... nestedSerializers);

	protected abstract TypeSerializer<?>[] getNestedSerializersFromSerializer(S serializer);

	protected abstract TypeSerializerSchemaCompatibility<T, S> outerCompatibility(S serializer);

	protected abstract Class<?> outerSerializerType();

	// ------------------------------------------------------------------------
	//  Serialization
	// ------------------------------------------------------------------------

	/**
	 * Writes the composite snapshot of all the contained serializers.
	 */
	public final void writeProductSnapshots(DataOutputView out) throws IOException {
		out.writeInt(MAGIC_NUMBER);
		out.writeInt(VERSION);

		out.writeInt(nestedSnapshots.length);
		for (TypeSerializerSnapshot<?> snap : nestedSnapshots) {
			TypeSerializerSnapshot.writeVersionedSnapshot(out, snap);
		}
	}

	/**
	 * Reads the composite snapshot of all the contained serializers.
	 */
	public final void readProductSnapshots(DataInputView in, ClassLoader cl) throws IOException {
		final int magicNumber = in.readInt();
		if (magicNumber != MAGIC_NUMBER) {
			throw new IOException(String.format("Corrupt data, magic number mismatch. Expected %8x, found %8x",
					MAGIC_NUMBER, magicNumber));
		}

		final int version = in.readInt();
		if (version != VERSION) {
			throw new IOException("Unrecognized version: " + version);
		}

		final int numSnapshots = in.readInt();
		nestedSnapshots = new TypeSerializerSnapshot<?>[numSnapshots];

		for (int i = 0; i < numSnapshots; i++) {
			nestedSnapshots[i] = TypeSerializerSnapshot.readVersionedSnapshot(in, cl);
		}
	}

	public final void legacyReadProductSnapshots(DataInputView in, ClassLoader cl) throws IOException {
		@SuppressWarnings("deprecation")
		final List<Tuple2<TypeSerializer<?>, TypeSerializerSnapshot<?>>> serializersAndSnapshots =
				TypeSerializerSerializationUtil.readSerializersAndConfigsWithResilience(in, cl);

		nestedSnapshots = serializersAndSnapshots.stream()
				.map(t -> t.f1)
				.toArray(TypeSerializerSnapshot<?>[]::new);
	}

	// ------------------------------------------------------------------------
	//  Type Serializer Snapshot
	// ------------------------------------------------------------------------

	@Override
	public TypeSerializer<T> restoreSerializer() {
		TypeSerializer<?>[] nestedSerializers = snapshotsToRestoreSerializers(nestedSnapshots);
		return createSerializer(nestedSerializers);
	}

	public <NS extends TypeSerializer<T>> TypeSerializerSchemaCompatibility<T, NS>
	resolveSchemaCompatibility(NS newSerializer) {

		// class compatibility
		if (!outerSerializerType().isInstance(newSerializer)) {
			return TypeSerializerSchemaCompatibility.incompatible();
		}

		// compatibility of the outer serializer's format

		@SuppressWarnings("unchecked")
		final S castedSerializer = (S) newSerializer;
		final TypeSerializerSchemaCompatibility<T, S> outerCompatibility = outerCompatibility(castedSerializer);

		if (outerCompatibility.isIncompatible()) {
			return TypeSerializerSchemaCompatibility.incompatible();
		}

		// check nested serializers for compatibility

		final TypeSerializer<?>[] nestedSerializers = getNestedSerializersFromSerializer(castedSerializer);
		checkState(nestedSerializers.length == nestedSnapshots.length);

		boolean nestedSerializerRequiresMigration = false;
		for (int i = 0; i < nestedSnapshots.length; i++) {
			TypeSerializerSchemaCompatibility<?, ?> compatibility =
					resolveCompatibility(nestedSerializers[i], nestedSnapshots[i]);

			if (compatibility.isIncompatible()) {
				return TypeSerializerSchemaCompatibility.incompatible();
			}
			if (compatibility.isCompatibleAfterMigration()) {
				nestedSerializerRequiresMigration = true;
			}
		}

		return (nestedSerializerRequiresMigration || !outerCompatibility.isCompatibleAsIs()) ?
				TypeSerializerSchemaCompatibility.compatibleAfterMigration() :
				TypeSerializerSchemaCompatibility.compatibleAsIs();
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	/**
	 * Utility method to conjure up a new scope for the generic parameters.
	 */
	@SuppressWarnings("unchecked")
	private static <E, X extends TypeSerializer<E>> TypeSerializerSchemaCompatibility<E, X> resolveCompatibility(
			TypeSerializer<?> serializer,
			TypeSerializerSnapshot<?> snapshot) {

		X typedSerializer = (X) serializer;
		TypeSerializerSnapshot<E> typedSnapshot = (TypeSerializerSnapshot<E>) snapshot;

		return typedSnapshot.resolveSchemaCompatibility(typedSerializer);
	}

	private static TypeSerializer<?>[] snapshotsToRestoreSerializers(TypeSerializerSnapshot<?>... snapshots) {
		return Arrays.stream(snapshots)
				.map(TypeSerializerSnapshot::restoreSerializer)
				.toArray(TypeSerializer[]::new);
	}
}
