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

package org.apache.flink.state.api.output.partitioner;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;

/**
 * A partitioner that selects the target channel based on the key group index.
 */
@Internal
public class KeyGroupRangePartitioner implements Partitioner<Integer> {

	private static final long serialVersionUID = 1L;

	private final int maxParallelism;

	public KeyGroupRangePartitioner(int maxParallelism) {
		this.maxParallelism = maxParallelism;
	}

	@Override
	public int partition(Integer key, int numPartitions) {
		return KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(
			maxParallelism,
			numPartitions,
			KeyGroupRangeAssignment.computeKeyGroupForKeyHash(key, maxParallelism));
	}
}
