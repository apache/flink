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

package org.apache.flink.runtime.state.heap.space;

/**
 * Bucket is a logic space in the chunk. Each Chunk has a
 * {@link BucketAllocator} to avoid space fragments.
 */
public interface BucketAllocator {

	/**
	 * Allocate a space in a the chunk.
	 *
	 * @param size size of space to allocate.
	 * @return offset of allocated space in the chunk.
	 */
	int allocate(int size);

	/**
	 * Free a space with the given offset in the chunk.
	 *
	 * @param offset offset of space in the chunk.
	 */
	void free(int offset);
}
