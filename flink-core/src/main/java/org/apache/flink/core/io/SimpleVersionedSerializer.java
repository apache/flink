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

package org.apache.flink.core.io;

import org.apache.flink.annotation.Internal;

import java.io.IOException;

/**
 * A simple serializer interface for versioned serialization.
 *
 * <p>The serializer has a version (returned by {@link #getVersion()}) which can be attached
 * to the serialized data. When the serializer evolves, the version can be used to identify
 * with which prior version the data was serialized.
 *
 * <pre>{@code
 * MyType someObject = ...;
 * SimpleVersionedSerializer<MyType> serializer = ...;
 *
 * byte[] serializedData = serializer.serialize(someObject);
 * int version = serializer.getVersion();
 *
 * MyType deserialized = serializer.deserialize(version, serializedData);
 *
 * byte[] someOldData = ...;
 * int oldVersion = ...;
 * MyType deserializedOldObject = serializer.deserialize(oldVersion, someOldData);
 *
 * }</pre>
 *
 * @param <E> The data type serialized / deserialized by this serializer.
 */
@Internal
public interface SimpleVersionedSerializer<E> extends Versioned {

	/**
	 * Gets the version with which this serializer serializes.
	 *
	 * @return The version of the serialization schema.
	 */
	@Override
	int getVersion();

	/**
	 * Serializes the given object. The serialization is assumed to correspond to the
	 * current serialization version (as returned by {@link #getVersion()}.
	 *
	 * @param obj The object to serialize.
	 * @return The serialized data (bytes).
	 *
	 * @throws IOException Thrown, if the serialization fails.
	 */
	byte[] serialize(E obj) throws IOException;

	/**
	 * De-serializes the given data (bytes) which was serialized with the scheme of the
	 * indicated version.
	 *
	 * @param version The version in which the data was serialized
	 * @param serialized The serialized data
	 * @return The deserialized object
	 *
	 * @throws IOException Thrown, if the deserialization fails.
	 */
	E deserialize(int version, byte[] serialized) throws IOException;
}
