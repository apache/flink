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

package org.apache.flink.state.api.runtime;

import org.apache.flink.runtime.state.LocalRecoveryDirectoryProvider;

import java.io.File;

/**
 * A minimally implemented {@link LocalRecoveryDirectoryProvider} that provides the functionality
 * required to run the {@code state-processor-api}.
 */
final class SavepointLocalRecoveryProvider implements LocalRecoveryDirectoryProvider {

	private static final long serialVersionUID = 9205635927790634162L;

	private static final String MSG = "This method should never be called";

	@Override
	public File allocationBaseDirectory(long checkpointId) {
		throw new UnsupportedOperationException(MSG);
	}

	@Override
	public File subtaskBaseDirectory(long checkpointId) {
		throw new UnsupportedOperationException(MSG);
	}

	@Override
	public File subtaskSpecificCheckpointDirectory(long checkpointId) {
		throw new UnsupportedOperationException(MSG);
	}

	@Override
	public File selectAllocationBaseDirectory(int idx) {
		throw new UnsupportedOperationException(MSG);
	}

	@Override
	public File selectSubtaskBaseDirectory(int idx) {
		throw new UnsupportedOperationException(MSG);
	}

	@Override
	public int allocationBaseDirsCount() {
		throw new UnsupportedOperationException(MSG);
	}
}

