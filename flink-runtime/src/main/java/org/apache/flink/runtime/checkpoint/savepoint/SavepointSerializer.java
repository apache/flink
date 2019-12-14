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

package org.apache.flink.runtime.checkpoint.savepoint;

import org.apache.flink.runtime.checkpoint.Checkpoints;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Serializer for {@link Savepoint} instances.
 *
 * <p>This serializer is used to read/write a savepoint via {@link Checkpoints}.
 *
 * <p>Version-specific serializers are accessed via the {@link SavepointSerializers} helper.
 *
 * @param <T> Savepoint type to serialize.
 */
public interface SavepointSerializer<T extends Savepoint> {

	/**
	 * Serializes a savepoint to an output stream.
	 *
	 * @param savepoint Savepoint to serialize
	 * @param dos        Output stream to serialize the savepoint to
	 * @throws IOException Serialization failures are forwarded
	 */
	void serialize(T savepoint, DataOutputStream dos) throws IOException;

	/**
	 * Deserializes a savepoint from an input stream.
	 *
	 * @param dis Input stream to deserialize savepoint from
	 * @param  userCodeClassLoader the user code class loader
	 * @return The deserialized savepoint
	 * @throws IOException Serialization failures are forwarded
	 */
	T deserialize(DataInputStream dis, ClassLoader userCodeClassLoader) throws IOException;

}
