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

package org.apache.flink.runtime.resourcemanager.active;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceIDRetrievable;
import org.apache.flink.util.Preconditions;

import java.util.Collection;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Testing implementation of {@link ResourceEventHandler}.
 */
public class TestingResourceEventHandler<WorkerType extends ResourceIDRetrievable> implements ResourceEventHandler<WorkerType> {

	private final Consumer<Collection<WorkerType>> onPreviousAttemptWorkersRecoveredConsumer;
	private final BiConsumer<ResourceID, String> onWorkerTerminatedConsumer;
	private final Consumer<Throwable> onErrorConsumer;

	private TestingResourceEventHandler(
			Consumer<Collection<WorkerType>> onPreviousAttemptWorkersRecoveredConsumer,
			BiConsumer<ResourceID, String> onWorkerTerminatedConsumer,
			Consumer<Throwable> onErrorConsumer) {
		this.onPreviousAttemptWorkersRecoveredConsumer = onPreviousAttemptWorkersRecoveredConsumer;
		this.onWorkerTerminatedConsumer = onWorkerTerminatedConsumer;
		this.onErrorConsumer = onErrorConsumer;
	}

	@Override
	public void onPreviousAttemptWorkersRecovered(Collection<WorkerType> recoveredWorkers) {
		onPreviousAttemptWorkersRecoveredConsumer.accept(recoveredWorkers);
	}

	@Override
	public void onWorkerTerminated(ResourceID resourceId, String diagnostics) {
		onWorkerTerminatedConsumer.accept(resourceId, diagnostics);
	}

	@Override
	public void onError(Throwable exception) {
		onErrorConsumer.accept(exception);
	}

	public static <WorkerType extends ResourceIDRetrievable> Builder<WorkerType> builder() {
		return new Builder<>();
	}

	/**
	 * Builder class for {@link TestingResourceEventHandler}.
	 */
	public static class Builder<WorkerType extends ResourceIDRetrievable> {
		private Consumer<Collection<WorkerType>> onPreviousAttemptWorkersRecoveredConsumer = (ignore) -> {};
		private BiConsumer<ResourceID, String> onWorkerTerminatedConsumer = (ignore1, ignore2) -> {};
		private Consumer<Throwable> onErrorConsumer = (ignore) -> {};

		private Builder() {}

		public Builder<WorkerType> setOnPreviousAttemptWorkersRecoveredConsumer(
				Consumer<Collection<WorkerType>> onPreviousAttemptWorkersRecoveredConsumer) {
			this.onPreviousAttemptWorkersRecoveredConsumer = Preconditions.checkNotNull(onPreviousAttemptWorkersRecoveredConsumer);
			return this;
		}

		public Builder<WorkerType> setOnWorkerTerminatedConsumer(BiConsumer<ResourceID, String> onWorkerTerminatedConsumer) {
			this.onWorkerTerminatedConsumer = Preconditions.checkNotNull(onWorkerTerminatedConsumer);
			return this;
		}

		public Builder<WorkerType> setOnErrorConsumer(Consumer<Throwable> onErrorConsumer) {
			this.onErrorConsumer = Preconditions.checkNotNull(onErrorConsumer);
			return this;
		}

		public TestingResourceEventHandler<WorkerType> build() {
			return new TestingResourceEventHandler<>(
					onPreviousAttemptWorkersRecoveredConsumer,
					onWorkerTerminatedConsumer,
					onErrorConsumer);
		}
	}
}
