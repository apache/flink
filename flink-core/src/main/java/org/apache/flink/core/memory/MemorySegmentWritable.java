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

package org.apache.flink.core.memory;

import org.apache.flink.annotation.Internal;

import java.io.IOException;

/**
 * Provides the interface for write(Segment).
 */
@Internal
public interface MemorySegmentWritable {

	/**
	 * Writes {@code len} bytes from memory segment {@code segment} starting at offset {@code off}, in order,
	 * to the output.
	 *
	 * @param segment memory segment to copy the bytes from.
	 * @param off the start offset in the memory segment.
	 * @param len The number of bytes to copy.
	 * @throws IOException if an I/O error occurs.
	 */
	void write(MemorySegment segment, int off, int len) throws IOException;
}
