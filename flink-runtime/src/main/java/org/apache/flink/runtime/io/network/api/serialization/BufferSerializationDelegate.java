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
 * WITHOUStreamRecord<?>WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.io.network.api.serialization;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.DataInputView;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * The serialization delegate exposes an {@link ByteBuffer} as a {@link IOReadableWritable} for serialization.
 * This interface is implemented to encapsulate buffer transformations such as compression and encryption.
 */
interface BufferSerializationDelegate extends IOReadableWritable {

	/**
	 * Set the input buffer before serialization.
	 * @param buffer the input buffer.
	 */
	void setBuffer(ByteBuffer buffer);

	/**
	 * Clear to release all its resources.
	 */
	void clear();

	/**
	 * Don't implement this method since this interface is on serialization side.
	 */
	default void read(DataInputView in) throws IOException {
		throw new IllegalStateException("Deserialization method called on BufferSerializationDelegate.");
	}
}
