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

import java.util.List;

/**
 * MemorySegment pool to hold pages in memory.
 */
public interface MemorySegmentPool extends MemorySegmentSource {

	/**
	 * Get the page size of each page this pool holds.
	 *
	 * @return the page size
	 */
	int pageSize();

	/**
	 * Return all pages back into this pool.
	 *
	 * @param memory the pages which want to be returned.
	 */
	void returnAll(List<MemorySegment> memory);

}

