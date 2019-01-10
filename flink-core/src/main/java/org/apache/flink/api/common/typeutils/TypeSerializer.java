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
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.io.Serializable;

/**
 * This interface describes the methods that are required for a data type to be handled by the Flink
 * runtime. Specifically, this interface contains the serialization and copying methods.
 *
 * <p>The methods in this class are assumed to be stateless, such that it is effectively thread safe. Stateful
 * implementations of the methods may lead to unpredictable side effects and will compromise both stability and
 * correctness of the program.
 *
 * <p><b>Upgrading TypeSerializers to the new TypeSerializerSnapshot model</b>
 *
 * <p>This section is relevant if you implemented a TypeSerializer in Flink versions up to 1.6 and want
 * to adapt that implementation to the new interfaces that support proper state schema evolution. Please
 * follow these steps:
 *
 * <ul>
 *     <li>Change the type serializer's config snapshot to implement {@link TypeSerializerSnapshot}, rather
 *     than extending {@code TypeSerializerConfigSnapshot} (as previously).
 *     <li>Move the compatibility check from the {@link TypeSerializer#ensureCompatibility(TypeSerializerConfigSnapshot)}
 *     method to the {@link TypeSerializerSnapshot#resolveSchemaCompatibility(TypeSerializer)} method.
 * </ul>
 *
 * <p><b>Maintaining Backwards Compatibility</b>
 *
 * <p>If you want your serializer to be able to restore checkpoints from Flink 1.6 and before, add the steps
 * below in addition to the steps above.
 *
 * <ul>
 *     <li>Retain the old serializer snapshot class (extending {@code TypeSerializerConfigSnapshot}) under
 *     the same name and give the updated serializer snapshot class (the one extending {@code TypeSerializerSnapshot})
 *     a new name.
 *     <li>Keep the {@link TypeSerializer#ensureCompatibility(TypeSerializerConfigSnapshot)} on the TypeSerializer
 *     as well.
 * </ul>
 *
 * @param <T> The data type that the serializer serializes.
 */
@PublicEvolving
public abstract class TypeSerializer<T> implements Serializable {
	
	private static final long serialVersionUID = 1L;

	// --------------------------------------------------------------------------------------------
	// General information about the type and the serializer
	// --------------------------------------------------------------------------------------------

	/**
	 * Gets whether the type is an immutable type.
	 * 
	 * @return True, if the type is immutable.
	 */
	public abstract boolean isImmutableType();
	
	/**
	 * Creates a deep copy of this serializer if it is necessary, i.e. if it is stateful. This
	 * can return itself if the serializer is not stateful.
	 *
	 * We need this because Serializers might be used in several threads. Stateless serializers
	 * are inherently thread-safe while stateful serializers might not be thread-safe.
	 */
	public abstract TypeSerializer<T> duplicate();

	// --------------------------------------------------------------------------------------------
	// Instantiation & Cloning
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Creates a new instance of the data type.
	 * 
	 * @return A new instance of the data type.
	 */
	public abstract T createInstance();

	/**
	 * Creates a deep copy of the given element in a new element.
	 * 
	 * @param from The element reuse be copied.
	 * @return A deep copy of the element.
	 */
	public abstract T copy(T from);
	
	/**
	 * Creates a copy from the given element.
	 * The method makes an attempt to store the copy in the given reuse element, if the type is mutable.
	 * This is, however, not guaranteed.
	 * 
	 * @param from The element to be copied.
	 * @param reuse The element to be reused. May or may not be used.
	 * @return A deep copy of the element.
	 */
	public abstract T copy(T from, T reuse);
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Gets the length of the data type, if it is a fix length data type.
	 * 
	 * @return The length of the data type, or <code>-1</code> for variable length data types.
	 */
	public abstract int getLength();
	
	// --------------------------------------------------------------------------------------------

	/**
	 * Serializes the given record to the given target output view.
	 * 
	 * @param record The record to serialize.
	 * @param target The output view to write the serialized data to.
	 * 
	 * @throws IOException Thrown, if the serialization encountered an I/O related error. Typically raised by the
	 *                     output view, which may have an underlying I/O channel to which it delegates.
	 */
	public abstract void serialize(T record, DataOutputView target) throws IOException;

	/**
	 * De-serializes a record from the given source input view.
	 * 
	 * @param source The input view from which to read the data.
	 * @return The deserialized element.
	 * 
	 * @throws IOException Thrown, if the de-serialization encountered an I/O related error. Typically raised by the
	 *                     input view, which may have an underlying I/O channel from which it reads.
	 */
	public abstract T deserialize(DataInputView source) throws IOException;
	
	/**
	 * De-serializes a record from the given source input view into the given reuse record instance if mutable.
	 * 
	 * @param reuse The record instance into which to de-serialize the data.
	 * @param source The input view from which to read the data.
	 * @return The deserialized element.
	 * 
	 * @throws IOException Thrown, if the de-serialization encountered an I/O related error. Typically raised by the
	 *                     input view, which may have an underlying I/O channel from which it reads.
	 */
	public abstract T deserialize(T reuse, DataInputView source) throws IOException;
	
	/**
	 * Copies exactly one record from the source input view to the target output view. Whether this operation
	 * works on binary data or partially de-serializes the record to determine its length (such as for records
	 * of variable length) is up to the implementer. Binary copies are typically faster. A copy of a record containing
	 * two integer numbers (8 bytes total) is most efficiently implemented as
	 * {@code target.write(source, 8);}.
	 *  
	 * @param source The input view from which to read the record.
	 * @param target The target output view to which to write the record.
	 * 
	 * @throws IOException Thrown if any of the two views raises an exception.
	 */
	public abstract void copy(DataInputView source, DataOutputView target) throws IOException;

	public abstract boolean equals(Object obj);

	/**
	 * Returns true if the given object can be equaled with this object. If not, it returns false.
	 *
	 * @param obj Object which wants to take part in the equality relation
	 * @return true if obj can be equaled with this, otherwise false
	 */
	public abstract boolean canEqual(Object obj);

	public abstract int hashCode();

	// --------------------------------------------------------------------------------------------
	// Serializer configuration snapshot for checkpoints/savepoints
	// --------------------------------------------------------------------------------------------

	/**
	 * Snapshots the configuration of this TypeSerializer. This method is only relevant if the serializer is
	 * used to state stored in checkpoints/savepoints.
	 *
	 * <p>The snapshot of the TypeSerializer is supposed to contain all information that affects the serialization
	 * format of the serializer. The snapshot serves two purposes: First, to reproduce the serializer when the
	 * checkpoint/savepoint is restored, and second, to check whether the serialization format is compatible
	 * with the serializer used in the restored program.
	 *
	 * <p><b>IMPORTANT:</b> TypeSerializerSnapshots changed after Flink 1.6. Serializers implemented against
	 * Flink versions up to 1.6 should still work, but adjust to new model to enable state evolution and be
	 * future-proof.
	 * See the class-level comments, section "Upgrading TypeSerializers to the new TypeSerializerSnapshot model"
	 * for details.
	 *
	 * @see TypeSerializerSnapshot#resolveSchemaCompatibility(TypeSerializer)
	 *
	 * @return snapshot of the serializer's current configuration (cannot be {@code null}).
	 */
	public abstract TypeSerializerSnapshot<T> snapshotConfiguration();

	// --------------------------------------------------------------------------------------------
	//  Deprecated methods for backwards compatibility
	// --------------------------------------------------------------------------------------------

	/**
	 * This method is deprecated. It used to resolved compatibility of the serializer with serializer
	 * config snapshots in checkpoints. The responsibility for this has moved to
	 * {@link TypeSerializerSnapshot#resolveSchemaCompatibility(TypeSerializer)}.
	 *
	 * <p>New serializers should not override this method any more! Serializers implemented against Flink
	 * versions up to 1.6 should still work, but should adjust to new model to enable state evolution and
	 * be future-proof. See the class-level comments, section <i>"Upgrading TypeSerializers to the new
	 * TypeSerializerSnapshot model"</i> for details.
	 *
	 * @deprecated Replaced by {@link TypeSerializerSnapshot#resolveSchemaCompatibility(TypeSerializer)}.
	 */
	@Deprecated
	public CompatibilityResult<T> ensureCompatibility(TypeSerializerConfigSnapshot<?> configSnapshot) {
		throw new UnsupportedOperationException(
				"This method is not supported any more - please evolve your TypeSerializer the following way:\n\n" +
				"  - If you have a serializer whose 'ensureCompatibility()' method delegates to another\n" +
				"    serializer's 'ensureCompatibility()', please use" +
						"'CompatibilityUtil.resolveCompatibilityResult(snapshot, this)' instead.\n\n" +
				"  - If you updated your serializer (removed overriding the 'ensureCompatibility()' method),\n" +
				"    please also update the corresponding config snapshot to not extend 'TypeSerializerConfigSnapshot'" +
						"any more.\n\n");
	}
}
