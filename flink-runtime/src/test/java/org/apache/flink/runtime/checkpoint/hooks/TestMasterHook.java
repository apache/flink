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

package org.apache.flink.runtime.checkpoint.hooks;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinatorTestingUtils;
import org.apache.flink.runtime.checkpoint.MasterTriggerRestoreHook;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link MasterTriggerRestoreHook} implementation for testing.
 */
public class TestMasterHook implements MasterTriggerRestoreHook<String> {

	private static final String DEFAULT_STATE = "default";

	private final String id;

	private int restoreCount = 0;

	private boolean failOnRestore = false;

	private TestMasterHook(String id) {
		this.id = checkNotNull(id);
	}

	public static TestMasterHook fromId(String id) {
		return new TestMasterHook(id);
	}

	@Override
	public String getIdentifier() {
		return id;
	}

	@Override
	public CompletableFuture<String> triggerCheckpoint(
			final long checkpointId,
			final long timestamp,
			final Executor executor) {

		return CompletableFuture.completedFuture(DEFAULT_STATE);
	}

	@Override
	public void restoreCheckpoint(final long checkpointId, @Nullable final String checkpointData) throws Exception {
		restoreCount++;
		if (failOnRestore) {
			throw new Exception("Failing mast hook state restore on purpose.");
		}
	}

	@Override
	public SimpleVersionedSerializer<String> createCheckpointDataSerializer() {
		return new CheckpointCoordinatorTestingUtils.StringSerializer();
	}

	public int getRestoreCount() {
		return restoreCount;
	}

	public void enableFailOnRestore() {
		this.failOnRestore = true;
	}

	public void disableFailOnRestore() {
		this.failOnRestore = false;
	}
}
