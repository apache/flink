/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.dispatcher.runner;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.webmonitor.TestingDispatcherGateway;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;

class TestingDispatcherGatewayService implements AbstractDispatcherLeaderProcess.DispatcherGatewayService {

	private final Object lock = new Object();

	private final Supplier<CompletableFuture<Void>> terminationFutureSupplier;

	private final Function<JobID, CompletableFuture<Void>> onRemovedJobGraphFunction;

	private final DispatcherGateway dispatcherGateway;

	private final CompletableFuture<ApplicationStatus> shutDownFuture;

	private CompletableFuture<Void> terminationFuture;

	private TestingDispatcherGatewayService(
			Supplier<CompletableFuture<Void>> terminationFutureSupplier,
			Function<JobID, CompletableFuture<Void>> onRemovedJobGraphFunction,
			DispatcherGateway dispatcherGateway,
			CompletableFuture<ApplicationStatus> shutDownFuture) {
		this.terminationFutureSupplier = terminationFutureSupplier;
		this.onRemovedJobGraphFunction = onRemovedJobGraphFunction;
		this.dispatcherGateway = dispatcherGateway;
		this.shutDownFuture = shutDownFuture;
	}

	@Override
	public DispatcherGateway getGateway() {
		return dispatcherGateway;
	}

	@Override
	public CompletableFuture<Void> onRemovedJobGraph(JobID jobId) {
		return onRemovedJobGraphFunction.apply(jobId);
	}

	@Override
	public CompletableFuture<ApplicationStatus> getShutDownFuture() {
		return shutDownFuture;
	}

	@Override
	public CompletableFuture<Void> closeAsync() {
		synchronized (lock) {
			if (terminationFuture == null) {
				terminationFuture = terminationFutureSupplier.get();
			}

			return terminationFuture;
		}
	}

	public static Builder newBuilder() {
		return new Builder();
	}

	public static class Builder {

		private Supplier<CompletableFuture<Void>> terminationFutureSupplier = FutureUtils::completedVoidFuture;

		private Function<JobID, CompletableFuture<Void>> onRemovedJobGraphFunction = ignored -> FutureUtils.completedVoidFuture();

		private DispatcherGateway dispatcherGateway = new TestingDispatcherGateway.Builder().build();

		private CompletableFuture<ApplicationStatus> shutDownFuture = new CompletableFuture<>();

		private Builder() {}

		public Builder setTerminationFutureSupplier(Supplier<CompletableFuture<Void>> terminationFutureSupplier) {
			this.terminationFutureSupplier = terminationFutureSupplier;
			return this;
		}

		public Builder setDispatcherGateway(DispatcherGateway dispatcherGateway) {
			this.dispatcherGateway = dispatcherGateway;
			return this;
		}

		public Builder setOnRemovedJobGraphFunction(Function<JobID, CompletableFuture<Void>> onRemovedJobGraphFunction) {
			this.onRemovedJobGraphFunction = onRemovedJobGraphFunction;
			return this;
		}

		public Builder setShutDownFuture(CompletableFuture<ApplicationStatus> shutDownFuture) {
			this.shutDownFuture = shutDownFuture;
			return this;
		}

		public TestingDispatcherGatewayService build() {
			return new TestingDispatcherGatewayService(terminationFutureSupplier, onRemovedJobGraphFunction, dispatcherGateway, shutDownFuture);
		}
	}
}
