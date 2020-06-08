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

package org.apache.flink.runtime.checkpoint.metadata;

import org.apache.flink.core.io.Versioned;

import java.io.DataInputStream;
import java.io.IOException;

/**
 * Deserializer for checkpoint metadata. Different deserializers exist to deserialize from different
 * format versions.
 *
 * <p>Version-specific serializers are accessed via the {@link MetadataSerializers} helper.
 */
public interface MetadataSerializer extends Versioned {

	/**
	 * Deserializes a savepoint from an input stream.
	 *
	 * @param dis Input stream to deserialize savepoint from
	 * @param  userCodeClassLoader the user code class loader
	 * @param externalPointer the external pointer of the given checkpoint
	 * @return The deserialized savepoint
	 * @throws IOException Serialization failures are forwarded
	 */
	CheckpointMetadata deserialize(DataInputStream dis, ClassLoader userCodeClassLoader, String externalPointer) throws IOException;
}
