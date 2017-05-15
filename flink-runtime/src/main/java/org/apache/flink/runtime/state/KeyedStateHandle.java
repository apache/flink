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

package org.apache.flink.runtime.state;

/**
 * Base for the handles of the checkpointed states in keyed streams. When
 * recovering from failures, the handle will be passed to all tasks whose key
 * group ranges overlap with it.
 */
public interface KeyedStateHandle extends CompositeStateHandle {

	/**
	 * Returns the range of the key groups contained in the state.
	 */
	KeyGroupRange getKeyGroupRange();

	/**
	 * Returns a state over a range that is the intersection between this
	 * handle's key-group range and the provided key-group range.
	 *
	 * @param keyGroupRange The key group range to intersect with
	 */
	KeyedStateHandle getIntersection(KeyGroupRange keyGroupRange);
}
