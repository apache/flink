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
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.state.api.runtime.metadata.NewSavepointMetadata;
import org.apache.flink.state.api.runtime.metadata.SavepointMetadata;

import java.util.HashMap;
import java.util.Map;

/**
 * A new savepoint.
 */
@PublicEvolving
public class NewSavepoint extends WritableSavepoint<NewSavepoint> {
	private final StateBackend stateBackend;

	private final int maxParallelism;

	private final Map<String, BootstrapTransformation> transformations;

	NewSavepoint(StateBackend stateBackend, int maxParallelism) {
		this.stateBackend = stateBackend;
		this.maxParallelism = maxParallelism;
		this.transformations = new HashMap<>();
	}

	@Override
	public NewSavepoint removeOperator(String uid) {
		transformations.remove(uid);
		return this;
	}

	@Override
	public <T> NewSavepoint withOperator(String uid, BootstrapTransformation<T> transformation) {
		if (transformations.containsKey(uid)) {
			throw new IllegalArgumentException("Duplicate uid " + uid + ". All uid's must be unique");
		}

		transformations.put(uid, transformation);
		return this;
	}

	@Override
	public void write(String path) {
		Path savepointPath = new Path(path);
		SavepointMetadata provider = new NewSavepointMetadata(maxParallelism);

		write(savepointPath, transformations, stateBackend, provider, null);
	}
}
