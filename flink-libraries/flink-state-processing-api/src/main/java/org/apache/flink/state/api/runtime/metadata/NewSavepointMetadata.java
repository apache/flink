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

import org.apache.flink.runtime.checkpoint.MasterState;
import org.apache.flink.runtime.checkpoint.OperatorState;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

/**
 * Returns metadata for a new savepoint.
 */
public class NewSavepointMetadata implements SavepointMetadata {

	private final int maxParallelism;

	public NewSavepointMetadata(int maxParallelism) {
		this.maxParallelism = maxParallelism;
	}

	@Override
	public int maxParallelism() {
		return maxParallelism;
	}

	@Override
	public Collection<MasterState> getMasterStates() {
		return Collections.emptyList();
	}

	@Override
	public Collection<OperatorState> getOperatorStates() {
		return Collections.emptyList();
	}

	@Override
	public OperatorState getOperatorState(String uid) throws IOException{
		throw new IOException("Savepoint does not contain state with operator uid " + uid);
	}
}
