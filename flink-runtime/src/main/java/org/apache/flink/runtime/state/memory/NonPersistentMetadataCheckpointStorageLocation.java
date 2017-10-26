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

package org.apache.flink.runtime.state.memory;

import org.apache.flink.runtime.state.CheckpointStorageLocation;
import org.apache.flink.runtime.state.CheckpointStreamFactory.CheckpointStateOutputStream;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory.MemoryCheckpointOutputStream;

import java.io.IOException;

/**
 * A checkpoint storage location for the {@link MemoryStateBackend} in case no durable persistence
 * for metadata has been configured.
 */
public class NonPersistentMetadataCheckpointStorageLocation implements CheckpointStorageLocation {

	/** The external pointer returned for checkpoints that are not externally addressable. */
	public static final String EXTERNAL_POINTER = "<checkpoint-not-externally-addressable>";

	/** The maximum serialized state size for the checkpoint metadata. */
	private static final int MAX_METADATA_STATE_SIZE = Integer.MAX_VALUE;

	@Override
	public CheckpointStateOutputStream createMetadataOutputStream() throws IOException {
		return new MemoryCheckpointOutputStream(MAX_METADATA_STATE_SIZE);
	}

	@Override
	public String markCheckpointAsFinished() {
		return EXTERNAL_POINTER;
	}

	@Override
	public void disposeOnFailure() {}

	@Override
	public String getLocationAsPointer() {
		return PersistentMetadataCheckpointStorageLocation.LOCATION_POINTER;
	}
}
