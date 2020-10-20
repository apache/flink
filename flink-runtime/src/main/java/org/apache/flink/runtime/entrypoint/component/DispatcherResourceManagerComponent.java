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

package org.apache.flink.runtime.entrypoint.component;

import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.dispatcher.Dispatcher;
import org.apache.flink.runtime.dispatcher.runner.DispatcherRunner;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.resourcemanager.ResourceManager;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.webmonitor.WebMonitorEndpoint;
import org.apache.flink.util.AutoCloseableAsync;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Component which starts a {@link Dispatcher}, {@link ResourceManager} and {@link WebMonitorEndpoint}
 * in the same process.
 */
public class DispatcherResourceManagerComponent implements AutoCloseableAsync {

	private static final Logger LOG = LoggerFactory.getLogger(DispatcherResourceManagerComponent.class);

	@Nonnull
	private final DispatcherRunner dispatcherRunner;

	@Nonnull
	private final ResourceManagerService resourceManagerService;

	@Nonnull
	private final LeaderRetrievalService dispatcherLeaderRetrievalService;

	@Nonnull
	private final LeaderRetrievalService resourceManagerRetrievalService;

	@Nonnull
	private final AutoCloseableAsync webMonitorEndpoint;

	private final CompletableFuture<Void> terminationFuture;

	private final CompletableFuture<ApplicationStatus> shutDownFuture;

	private final AtomicBoolean isRunning = new AtomicBoolean(true);

	private final FatalErrorHandler fatalErrorHandler;

	DispatcherResourceManagerComponent(
			@Nonnull DispatcherRunner dispatcherRunner,
			@Nonnull ResourceManagerService resourceManagerService,
			@Nonnull LeaderRetrievalService dispatcherLeaderRetrievalService,
			@Nonnull LeaderRetrievalService resourceManagerRetrievalService,
			@Nonnull AutoCloseableAsync webMonitorEndpoint,
			@Nonnull FatalErrorHandler fatalErrorHandler) {
		this.dispatcherRunner = dispatcherRunner;
		this.resourceManagerService = resourceManagerService;
		this.dispatcherLeaderRetrievalService = dispatcherLeaderRetrievalService;
		this.resourceManagerRetrievalService = resourceManagerRetrievalService;
		this.webMonitorEndpoint = webMonitorEndpoint;
		this.fatalErrorHandler = fatalErrorHandler;
		this.terminationFuture = new CompletableFuture<>();
		this.shutDownFuture = new CompletableFuture<>();

		registerShutDownFuture();
		handleUnexpectedResourceManagerTermination();
	}

	private void handleUnexpectedResourceManagerTermination() {
		resourceManagerService.getTerminationFuture().whenComplete(
				(ignored, throwable) -> {
					if (isRunning.get()) {
						fatalErrorHandler.onFatalError(new FlinkException("Unexpected termination of ResourceManager.", throwable));
					}
				});
	}

	private void registerShutDownFuture() {
		FutureUtils.forward(dispatcherRunner.getShutDownFuture(), shutDownFuture);
	}

	public final CompletableFuture<ApplicationStatus> getShutDownFuture() {
		return shutDownFuture;
	}

	/**
	 * Deregister the Flink application from the resource management system by signalling
	 * the {@link ResourceManager}.
	 *
	 * @param applicationStatus to terminate the application with
	 * @param diagnostics additional information about the shut down, can be {@code null}
	 * @return Future which is completed once the shut down
	 */
	public CompletableFuture<Void> deregisterApplicationAndClose(
			final ApplicationStatus applicationStatus,
			final @Nullable String diagnostics) {

		if (isRunning.compareAndSet(true, false)) {
			final CompletableFuture<Void> closeWebMonitorAndDeregisterAppFuture =
				FutureUtils.composeAfterwards(webMonitorEndpoint.closeAsync(), () -> deregisterApplication(applicationStatus, diagnostics));

			return FutureUtils.composeAfterwards(closeWebMonitorAndDeregisterAppFuture, this::closeAsyncInternal);
		} else {
			return terminationFuture;
		}
	}

	private CompletableFuture<Void> deregisterApplication(
			final ApplicationStatus applicationStatus,
			final @Nullable String diagnostics) {

		final ResourceManagerGateway selfGateway = resourceManagerService.getGateway();
		return selfGateway.deregisterApplication(applicationStatus, diagnostics).thenApply(ack -> null);
	}

	private CompletableFuture<Void> closeAsyncInternal() {
		LOG.info("Closing components.");

		Exception exception = null;

		final Collection<CompletableFuture<Void>> terminationFutures = new ArrayList<>(3);

		try {
			dispatcherLeaderRetrievalService.stop();
		} catch (Exception e) {
			exception = ExceptionUtils.firstOrSuppressed(e, exception);
		}

		try {
			resourceManagerRetrievalService.stop();
		} catch (Exception e) {
			exception = ExceptionUtils.firstOrSuppressed(e, exception);
		}

		terminationFutures.add(dispatcherRunner.closeAsync());

		terminationFutures.add(resourceManagerService.closeAsync());

		if (exception != null) {
			terminationFutures.add(FutureUtils.completedExceptionally(exception));
		}

		final CompletableFuture<Void> componentTerminationFuture = FutureUtils.completeAll(terminationFutures);

		componentTerminationFuture.whenComplete((aVoid, throwable) -> {
			if (throwable != null) {
				terminationFuture.completeExceptionally(throwable);
			} else {
				terminationFuture.complete(aVoid);
			}
		});

		return terminationFuture;
	}

	@Override
	public CompletableFuture<Void> closeAsync() {
		return deregisterApplicationAndClose(ApplicationStatus.CANCELED, "DispatcherResourceManagerComponent has been closed.");
	}

	/**
	 * Service which gives access to a {@link ResourceManagerGateway}.
	 */
	interface ResourceManagerService extends AutoCloseableAsync {

		ResourceManagerGateway getGateway();

		CompletableFuture<Void> getTerminationFuture();
	}
}
