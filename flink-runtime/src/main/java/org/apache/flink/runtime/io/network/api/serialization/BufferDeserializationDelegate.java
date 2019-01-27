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
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.io.network.buffer.Buffer;

import java.io.IOException;

/**
 * The deserialization delegate is used during deserialization to read a {@link Buffer} as if it implements
 * {@link IOReadableWritable}. This interface is implemented to encapsulate buffer transformations
 * such as decompression and decryption.
 */
interface BufferDeserializationDelegate extends IOReadableWritable {

	/**
	 * Reset before deserialization. Since its output buffer always belongs to
	 * this {@link BufferDeserializationDelegate}, make sure to reset before another deserialization.
	 */
	void reset();

	/**
	 * Get the output buffer after deserialization.
	 * @return {@link Buffer} contains the result.
	 */
	Buffer getBuffer();

	/**
	 * Clear before release all its resources.
	 */
	void clear();

	/**
	 * Don't implement this method since this interface is on deserialization side.
	 */
	default void write(DataOutputView out) throws IOException {
		throw new IllegalStateException("Serialization method called on BufferDeserializationDelegate.");
	}
}
