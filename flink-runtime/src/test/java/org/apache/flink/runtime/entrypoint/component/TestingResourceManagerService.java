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

package org.apache.flink.runtime.entrypoint.component;

import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;

import java.util.concurrent.CompletableFuture;

/**
 * Testing implementation of {@link DispatcherResourceManagerComponent.ResourceManagerService}.
 */
public class TestingResourceManagerService implements DispatcherResourceManagerComponent.ResourceManagerService {
	private final ResourceManagerGateway resourceManagerGateway;

	private final CompletableFuture<Void> terminationFuture;
	private final boolean completeTerminationFutureOnClose;

	private TestingResourceManagerService(
			ResourceManagerGateway resourceManagerGateway,
			CompletableFuture<Void> terminationFuture,
			boolean completeTerminationFutureOnClose) {
		this.resourceManagerGateway = resourceManagerGateway;
		this.terminationFuture = terminationFuture;
		this.completeTerminationFutureOnClose = completeTerminationFutureOnClose;
	}

	@Override
	public ResourceManagerGateway getGateway() {
		return resourceManagerGateway;
	}

	@Override
	public CompletableFuture<Void> getTerminationFuture() {
		return terminationFuture;
	}

	@Override
	public CompletableFuture<Void> closeAsync() {
		if (completeTerminationFutureOnClose) {
			terminationFuture.complete(null);
		}
		return getTerminationFuture();
	}

	public static Builder newBuilder() {
		return new Builder();
	}

	/**
	 * Builder for {@link TestingResourceManagerService}.
	 */
	public static final class Builder {
		private ResourceManagerGateway resourceManagerGateway = new TestingResourceManagerGateway();
		private CompletableFuture<Void> terminationFuture = new CompletableFuture<>();
		private boolean completeTerminationFutureOnClose = true;

		public Builder setResourceManagerGateway(ResourceManagerGateway resourceManagerGateway) {
			this.resourceManagerGateway = resourceManagerGateway;
			return this;
		}

		public Builder setTerminationFuture(CompletableFuture<Void> terminationFuture) {
			this.terminationFuture = terminationFuture;
			return this;
		}

		public Builder withManualTerminationFutureCompletion() {
			completeTerminationFutureOnClose = false;
			return this;
		}

		public TestingResourceManagerService build() {
			return new TestingResourceManagerService(resourceManagerGateway, terminationFuture, completeTerminationFutureOnClose);
		}
	}
}
