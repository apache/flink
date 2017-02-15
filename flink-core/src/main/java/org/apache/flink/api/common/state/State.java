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

import org.apache.flink.annotation.PublicEvolving;

/**
 * Base interface for all types of <i>keyed state</i> must implement. Sub-classes of this
 * interface define the specific type and behavior of the state, for example
 * {@link ValueState} or {@link ListState}.
 *
 * <p>Keyed  state is only accessible by functions applied on a {@code KeyedDataStream}, which
 * is the result of a {@code DataStream.keyBy()} operation.
 * 
 * <p>The key is automatically supplied by the system, so the function always sees the value mapped to the
 * key of the current element. That way, the system can handle stream and state partitioning
 * consistently together, making state accesses local and allowing the system to transparently
 * re-shard the state when changing the parallelism of the operation that works with the state. 
 */
@PublicEvolving
public interface State {

	/**
	 * Removes the value mapped under the current key.
	 */
	void clear();
}
