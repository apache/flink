/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.annotation.PublicEvolving;

/**
 * This interface provides a context in which operators can initialize by registering to managed state (i.e. state that
 * is managed by state backends) or iterating over streams of state partitions written as raw state in a previous
 * snapshot.
 *
 * <p>
 * Similar to the managed state from {@link ManagedInitializationContext} and in general,  raw operator state is
 * available to all operators, while raw keyed state is only available for operators after keyBy.
 *
 * <p>
 * For the purpose of initialization, the context signals if all state is empty (new operator) or if any state was
 * restored from a previous execution of this operator.
 *
 */
@PublicEvolving
public interface StateInitializationContext extends FunctionInitializationContext {

	/**
	 * Returns an iterable to obtain input streams for previously stored operator state partitions that are assigned to
	 * this operator.
	 */
	Iterable<StatePartitionStreamProvider> getRawOperatorStateInputs();

	/**
	 * Returns an iterable to obtain input streams for previously stored keyed state partitions that are assigned to
	 * this operator.
	 */
	Iterable<KeyGroupStatePartitionStreamProvider> getRawKeyedStateInputs();

}
