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

package org.apache.flink.api.common.state;

import org.apache.flink.annotation.Internal;

import java.io.Serializable;

/**
 * Assigns a key to a key group index. A key group is the smallest unit of partitioned state
 * which is assigned to an operator. An operator can be assigned multiple key groups.
 *
 * @param <K> Type of the key
 */
@Internal
public interface KeyGroupAssigner<K> extends Serializable {
	/**
	 * Calculates the key group index for the given key.
	 *
	 * @param key Key to be used
	 * @return Key group index for the given key
	 */
	int getKeyGroupIndex(K key);

	/**
	 * Setups the key group assigner with the maximum parallelism (= number of key groups).
	 *
	 * @param maxParallelism Maximum parallelism (= number of key groups)
	 */
	void setup(int maxParallelism);
}
