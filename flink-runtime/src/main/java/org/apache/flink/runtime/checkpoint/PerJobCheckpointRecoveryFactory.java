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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Simple {@link CheckpointRecoveryFactory} which creates and keeps separate
 * {@link CompletedCheckpointStore} and {@link CheckpointIDCounter} for each {@link JobID}.
 */
public class PerJobCheckpointRecoveryFactory implements CheckpointRecoveryFactory {
	private final Function<Integer, CompletedCheckpointStore> completedCheckpointStorePerJobFactory;
	private final Supplier<CheckpointIDCounter> checkpointIDCounterPerJobFactory;
	private final Map<JobID, CompletedCheckpointStore> store;
	private final Map<JobID, CheckpointIDCounter> counter;

	public PerJobCheckpointRecoveryFactory(
			Function<Integer, CompletedCheckpointStore> completedCheckpointStorePerJobFactory,
			Supplier<CheckpointIDCounter> checkpointIDCounterPerJobFactory) {
		this.completedCheckpointStorePerJobFactory = completedCheckpointStorePerJobFactory;
		this.checkpointIDCounterPerJobFactory = checkpointIDCounterPerJobFactory;
		this.store = new HashMap<>();
		this.counter = new HashMap<>();
	}

	@Override
	public CompletedCheckpointStore createCheckpointStore(
			JobID jobId,
			int maxNumberOfCheckpointsToRetain,
			ClassLoader userClassLoader) {
		return store.computeIfAbsent(jobId, jId ->
			completedCheckpointStorePerJobFactory.apply(maxNumberOfCheckpointsToRetain));
	}

	@Override
	public CheckpointIDCounter createCheckpointIDCounter(JobID jobId) {
		return counter.computeIfAbsent(jobId, jId -> checkpointIDCounterPerJobFactory.get());
	}

	@VisibleForTesting
	public static CheckpointRecoveryFactory useSameServicesForAllJobs(
			CompletedCheckpointStore store,
			CheckpointIDCounter counter) {
		return new PerJobCheckpointRecoveryFactory(n -> store, () -> counter);
	}
}
