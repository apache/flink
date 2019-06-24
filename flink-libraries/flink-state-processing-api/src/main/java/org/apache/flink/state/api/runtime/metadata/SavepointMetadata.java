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

package org.apache.flink.state.api.runtime.metadata;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.checkpoint.MasterState;
import org.apache.flink.runtime.checkpoint.OperatorState;

import java.io.Serializable;
import java.util.Collection;

/**
 * Returns metadata about a savepoint.
 */
@Internal
public interface SavepointMetadata extends Serializable {

	/**
	 * @return The max parallelism for the savepoint.
	 */
	int maxParallelism();

	/**
	 * @return Masters states for the savepoint.
	 */
	Collection<MasterState> getMasterStates();

	/**
	 * @return Operator states for the savepoint.
	 */
	Collection<OperatorState> getOperatorStates();
}
