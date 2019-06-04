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

package org.apache.flink.state.api;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.runtime.state.StateBackend;

/**
 * A {@link Savepoint} is a collection of operator states that can be used to supply initial state
 * when starting a {@link org.apache.flink.streaming.api.datastream.DataStream} job.
 */
@PublicEvolving
public final class Savepoint {

	private Savepoint() {}

	/**
	 * Loads an existing savepoint. Useful if you want to query, modify, or extend
	 * the state of an existing application.
	 *
	 * @param env The execution enviornment used to transform the savepoint.
	 * @param path The path to an existing savepoint on disk.
	 * @param stateBackend The state backend of the savepoint used for keyed state.
	 */
	public static ExistingSavepoint load(ExecutionEnvironment env, String path, StateBackend stateBackend) {
		return new ExistingSavepoint(env, path, stateBackend);
	}
}
