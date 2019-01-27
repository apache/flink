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

package org.apache.flink.table.runtime.util;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentSource;

import java.util.Arrays;
import java.util.List;

/**
 * MemorySegment pool.
 */
public interface MemorySegmentPool extends MemorySegmentSource {

	int pageSize();

	void returnAll(List<MemorySegment> memory);

	default void returnAll(MemorySegment[] memory) {
		returnAll(Arrays.asList(memory));
	}

	void clear();

	default int remainBuffers() {
		throw new UnsupportedOperationException();
	}
}
