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
 * Some constants used for space.
 */
public class SpaceConstants {

	private SpaceConstants() {
	}

	/** This indicates there is no space left. */
	static final int NO_SPACE = -1;

	/** Size of bucket. */
	static final int BUCKET_SIZE = 1024 * 1024;

	/** Number of bits for four bytes. */
	public static final int FOUR_BYTES_BITS = 32;

	/** Mask for four bytes. */
	public static final long FOUR_BYTES_MARK = 0xFFFFFFFFL;
}
