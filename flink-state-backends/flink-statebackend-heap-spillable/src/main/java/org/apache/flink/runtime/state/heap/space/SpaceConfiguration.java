/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.flink.runtime.state.heap.space;

import org.apache.flink.util.Preconditions;

/**
 * Space-related configurations.
 */
class SpaceConfiguration {

	/** Size of chunk. */
	private final int chunkSize;

	/** Type of space. */
	private final ChunkAllocator.SpaceType spaceType;

	/** Whether to preallocate space. */
	private final boolean preAllocate;

	SpaceConfiguration(
		int chunkSize,
		boolean preAllocate,
		ChunkAllocator.SpaceType spaceType) {
		Preconditions.checkArgument(chunkSize > 0,
			"Chunk size should be positive, but the actual is " + chunkSize);
		Preconditions.checkArgument((chunkSize & chunkSize - 1) == 0,
			"Chunk size should be a power of 2, but the actual is " + chunkSize);
		this.chunkSize = chunkSize;
		this.preAllocate = preAllocate;
		this.spaceType = spaceType;
	}

	int getChunkSize() {
		return chunkSize;
	}

	boolean isPreAllocate() {
		return preAllocate;
	}

	ChunkAllocator.SpaceType getSpaceType() {
		return spaceType;
	}
}
