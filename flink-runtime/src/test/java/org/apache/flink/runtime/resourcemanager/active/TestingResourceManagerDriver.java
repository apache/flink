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

import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessSpec;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.BiConsumerWithException;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Testing implementation of {@link ResourceManagerDriver}.
 */
public class TestingResourceManagerDriver implements ResourceManagerDriver<ResourceID> {

	private final BiConsumerWithException<ResourceEventHandler<ResourceID>, ScheduledExecutor, Exception> initializeConsumer;
	private final Supplier<CompletableFuture<Void>> terminateSupplier;
	private final BiConsumerWithException<ApplicationStatus, String, Exception> deregisterApplicationConsumer;
	private final Function<TaskExecutorProcessSpec, CompletableFuture<ResourceID>> requestResourceFunction;
	private final Consumer<ResourceID> releaseResourceConsumer;

	private TestingResourceManagerDriver(
			final BiConsumerWithException<ResourceEventHandler<ResourceID>, ScheduledExecutor, Exception> initializeConsumer,
			final Supplier<CompletableFuture<Void>> terminateSupplier,
			final BiConsumerWithException<ApplicationStatus, String, Exception> deregisterApplicationConsumer,
			final Function<TaskExecutorProcessSpec, CompletableFuture<ResourceID>> requestResourceFunction,
			final Consumer<ResourceID> releaseResourceConsumer) {
		this.initializeConsumer = Preconditions.checkNotNull(initializeConsumer);
		this.terminateSupplier = Preconditions.checkNotNull(terminateSupplier);
		this.deregisterApplicationConsumer = Preconditions.checkNotNull(deregisterApplicationConsumer);
		this.requestResourceFunction = Preconditions.checkNotNull(requestResourceFunction);
		this.releaseResourceConsumer = Preconditions.checkNotNull(releaseResourceConsumer);
	}

	@Override
	public void initialize(ResourceEventHandler<ResourceID> resourceEventHandler, ScheduledExecutor mainThreadExecutor) throws Exception {
		initializeConsumer.accept(resourceEventHandler, mainThreadExecutor);
	}

	@Override
	public CompletableFuture<Void> terminate() {
		return terminateSupplier.get();
	}

	@Override
	public void deregisterApplication(ApplicationStatus finalStatus, @Nullable String optionalDiagnostics) throws Exception {
		deregisterApplicationConsumer.accept(finalStatus, optionalDiagnostics);
	}

	@Override
	public CompletableFuture<ResourceID> requestResource(TaskExecutorProcessSpec taskExecutorProcessSpec) {
		return requestResourceFunction.apply(taskExecutorProcessSpec);
	}

	@Override
	public void releaseResource(ResourceID worker) {
		releaseResourceConsumer.accept(worker);
	}

	public static class Builder {
		private BiConsumerWithException<ResourceEventHandler<ResourceID>, ScheduledExecutor, Exception> initializeConsumer =
				(ignore1, ignore2) -> {};

		private Supplier<CompletableFuture<Void>> terminateSupplier =
				() -> CompletableFuture.completedFuture(null);

		private BiConsumerWithException<ApplicationStatus, String, Exception> deregisterApplicationConsumer =
				(ignore1, ignore2) -> {};

		private Function<TaskExecutorProcessSpec, CompletableFuture<ResourceID>> requestResourceFunction =
				(ignore) -> CompletableFuture.completedFuture(ResourceID.generate());

		private Consumer<ResourceID> releaseResourceConsumer =
				(ignore) -> {};

		public Builder setInitializeConsumer(BiConsumerWithException<ResourceEventHandler<ResourceID>, ScheduledExecutor, Exception> initializeConsumer) {
			this.initializeConsumer = Preconditions.checkNotNull(initializeConsumer);
			return this;
		}

		public Builder setTerminateSupplier(Supplier<CompletableFuture<Void>> terminateSupplier) {
			this.terminateSupplier = Preconditions.checkNotNull(terminateSupplier);
			return this;
		}

		public Builder setDeregisterApplicationConsumer(BiConsumerWithException<ApplicationStatus, String, Exception> deregisterApplicationConsumer) {
			this.deregisterApplicationConsumer = Preconditions.checkNotNull(deregisterApplicationConsumer);
			return this;
		}

		public Builder setRequestResourceFunction(Function<TaskExecutorProcessSpec, CompletableFuture<ResourceID>> requestResourceFunction) {
			this.requestResourceFunction = Preconditions.checkNotNull(requestResourceFunction);
			return this;
		}

		public Builder setReleaseResourceConsumer(Consumer<ResourceID> releaseResourceConsumer) {
			this.releaseResourceConsumer = Preconditions.checkNotNull(releaseResourceConsumer);
			return this;
		}

		public TestingResourceManagerDriver build() {
			return new TestingResourceManagerDriver(
					initializeConsumer,
					terminateSupplier,
					deregisterApplicationConsumer,
					requestResourceFunction,
					releaseResourceConsumer);
		}
	}
}
