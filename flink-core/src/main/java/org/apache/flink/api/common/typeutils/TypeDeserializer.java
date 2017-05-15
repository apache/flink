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

import org.apache.flink.core.memory.DataInputView;

import java.io.IOException;

/**
 * This interface describes the methods that are required for a data type to be read by the Flink runtime.
 * Specifically, this interface contains the deserialization methods. In contrast, the {@link TypeSerializer}
 * interface contains the complete set of methods for both serialization and deserialization.
 *
 * <p>The methods in this class are assumed to be stateless, such that it is effectively thread safe. Stateful
 * implementations of the methods may lead to unpredictable side effects and will compromise both stability and
 * correctness of the program.
 *
 * @param <T> The data type that the deserializer deserializes.
 */
public interface TypeDeserializer<T> {

	/**
	 * Creates a deep copy of this deserializer if it is necessary, i.e. if it is stateful. This
	 * can return itself if the serializer is not stateful.
	 *
	 * We need this because deserializers might be used in several threads. Stateless deserializers
	 * are inherently thread-safe while stateful deserializers might not be thread-safe.
	 */
	TypeSerializer<T> duplicate();

	/**
	 * De-serializes a record from the given source input view.
	 *
	 * @param source The input view from which to read the data.
	 * @return The deserialized element.
	 *
	 * @throws IOException Thrown, if the de-serialization encountered an I/O related error. Typically raised by the
	 *                     input view, which may have an underlying I/O channel from which it reads.
	 */
	T deserialize(DataInputView source) throws IOException;

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
	T deserialize(T reuse, DataInputView source) throws IOException;

	/**
	 * Gets the length of the data type, if it is a fix length data type.
	 *
	 * @return The length of the data type, or <code>-1</code> for variable length data types.
	 */
	int getLength();

	/**
	 * Returns true if the given object can be equaled with this object. If not, it returns false.
	 *
	 * @param obj Object which wants to take part in the equality relation
	 * @return true if obj can be equaled with this, otherwise false
	 */
	boolean canEqual(Object obj);

	boolean equals(Object obj);

	int hashCode();
}
