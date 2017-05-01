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
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.io.Serializable;

/**
 * This interface describes the methods that are required for a data type to be handled by the Flink
 * runtime. Specifically, this interface contains the serialization and copying methods.
 * <p>
 * The methods in this class are assumed to be stateless, such that it is effectively thread safe. Stateful
 * implementations of the methods may lead to unpredictable side effects and will compromise both stability and
 * correctness of the program.
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

	public boolean canRestoreFrom(TypeSerializer<?> other) {
		return equals(other);
	}

	// --------------------------------------------------------------------------------------------
	// Serializer configuration snapshotting & reconfiguring
	// --------------------------------------------------------------------------------------------

	/**
	 * Create a snapshot of the serializer's current configuration to be stored along with the managed state it is
	 * registered to (if any - this method is only relevant if this serializer is registered for serialization of
	 * managed state).
	 *
	 * <p>The configuration snapshot should contain information about the serializer's parameter settings and its
	 * serialization format. When a new serializer is registered to serialize the same managed state that this
	 * serializer was registered to, the returned configuration snapshot can be used to reconfigure the new serializer
	 * so that it may be compatible (i.e., the new serializer is capable of reading data written by this serializer).
	 *
	 * <p>Implementations can also return the singleton {@link ForwardCompatibleSerializationFormatConfig#INSTANCE}
	 * configuration if they guarantee forwards compatibility. For example, implementations that use serialization
	 * frameworks with built-in serialization compatibility, such as <a href=>Thrift</a> or <a href=> Protobuf</a>,
	 * is suitable for this usage pattern. By returning the {@link ForwardCompatibleSerializationFormatConfig#INSTANCE},
	 * this informs Flink that when managed state serialized using this serializer is restored, there is no need to
	 * check compatibility of the new serializer for the same state. In other words, new serializers are always assumed
	 * to be fully compatible for the serialized state.
	 *
	 * @see TypeSerializerConfigSnapshot
	 * @see ForwardCompatibleSerializationFormatConfig
	 *
	 * @return snapshot of the serializer's current configuration.
	 */
	public abstract TypeSerializerConfigSnapshot snapshotConfiguration();

	/**
	 * Reconfigure this serializer with the configuration snapshot of a preceding serializer that was registered
	 * for serialization of the same managed state (if any - this method is only relevant if this serializer is
	 * registered for serialization of managed state).
	 *
	 * <p>Implementations need to return the result of the reconfiguration. The result can be one of the following:
	 * <ul>
	 *     <li>{@link ReconfigureResult#COMPATIBLE}: this serializer has been reconfigured to be compatible with its
	 *     predecessor, and the serialization schema remains the same.</li>
	 *
	 *     <li>{@link ReconfigureResult#COMPATIBLE_NEW_SCHEMA}: this serializer is reconfigured to be compatible with
	 *     its predecessor, but has a new serialization schema.</li>
	 *
	 *     <li>{@link ReconfigureResult#INCOMPATIBLE}: this serializer cannot resolve the configuration of its
	 *     predecessor, and therefore is not compatible for data written by the preceding serializer.</li>
	 * </ul>
	 *
	 * <p>This method is guaranteed to only be invoked if the preceding serializer's configuration snapshot is not the
	 * singleton {@link ForwardCompatibleSerializationFormatConfig#INSTANCE} configuration. In such cases, Flink always
	 * assume that the reconfiguration result is {@link ReconfigureResult#COMPATIBLE}.
	 *
	 * @see ReconfigureResult
	 *
	 * @param configSnapshot configuration snapshot of a preceding serializer for the same managed state
	 *
	 * @return the result of the reconfiguration.
	 */
	protected abstract ReconfigureResult reconfigure(TypeSerializerConfigSnapshot configSnapshot);

	/**
	 * Reconfigure this serializer with the configuration snapshot of a preceding serializer that was registered
	 * for serialization of the same managed state (if any - this method is only relevant if this serializer is
	 * registered for serialization of managed state).
	 *
	 * <p>This method is not part of the public user-facing API, and cannot be overriden. External operations
	 * providing a configuration snapshot of preceding serializer can only do so through this method.
	 *
	 * <p>This method always assumes that the reconfiguration result is {@link ReconfigureResult#COMPATIBLE} if
	 * the provided configuration snapshot is the singleton {@link ForwardCompatibleSerializationFormatConfig#INSTANCE}.
	 * Otherwise, the configuration snapshot is provided to the actual
	 * {@link #reconfigure(TypeSerializerConfigSnapshot)} implementation.
	 *
	 * @param configSnapshot configuration snapshot of a preceding serializer for the same managed state
	 *
	 * @return the result of the reconfiguration.
	 */
	@Internal
	public final ReconfigureResult reconfigureWith(TypeSerializerConfigSnapshot configSnapshot) {
		// reference equality is viable here, because the forward compatible
		// marker config will always be explicitly restored with the singleton instance
		if (configSnapshot != ForwardCompatibleSerializationFormatConfig.INSTANCE) {
			return reconfigure(configSnapshot);
		} else {
			return ReconfigureResult.COMPATIBLE;
		}
	}
}
