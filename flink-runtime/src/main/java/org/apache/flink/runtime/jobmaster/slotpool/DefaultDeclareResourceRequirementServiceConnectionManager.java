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

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.runtime.concurrent.ExponentialBackoffRetryStrategy;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.slots.ResourceRequirements;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;

/**
 * Default implementation of {@link DeclareResourceRequirementServiceConnectionManager}.
 *
 * <p>This connection manager is responsible for sending new
 * resource requirements to the connected service. In case of faults it continues
 * retrying to send the latest resource requirements to the service with
 * an exponential backoff strategy.
 */
class DefaultDeclareResourceRequirementServiceConnectionManager
		extends AbstractServiceConnectionManager<DeclareResourceRequirementServiceConnectionManager.DeclareResourceRequirementsService>
		implements DeclareResourceRequirementServiceConnectionManager {

	private static final Logger LOG = LoggerFactory.getLogger(DefaultDeclareResourceRequirementServiceConnectionManager.class);

	private final ScheduledExecutor scheduledExecutor;

	@Nullable
	@GuardedBy("lock")
	private ResourceRequirements currentResourceRequirements;

	private DefaultDeclareResourceRequirementServiceConnectionManager(ScheduledExecutor scheduledExecutor) {
		this.scheduledExecutor = scheduledExecutor;
	}

	@Override
	public void declareResourceRequirements(ResourceRequirements resourceRequirements) {
		synchronized (lock) {
			checkNotClosed();
			if (isConnected()) {
				currentResourceRequirements = resourceRequirements;

				triggerResourceRequirementsSubmission(Duration.ofMillis(1L), Duration.ofMillis(10000L), currentResourceRequirements);
			}
		}
	}

	@GuardedBy("lock")
	private void triggerResourceRequirementsSubmission(
			Duration sleepOnError,
			Duration maxSleepOnError,
			ResourceRequirements resourceRequirementsToSend) {

		FutureUtils.retryWithDelay(
				() -> sendResourceRequirements(resourceRequirementsToSend),
				new ExponentialBackoffRetryStrategy(Integer.MAX_VALUE, sleepOnError, maxSleepOnError),
				throwable -> !(throwable instanceof CancellationException),
				scheduledExecutor);
	}

	private CompletableFuture<Acknowledge> sendResourceRequirements(ResourceRequirements resourceRequirementsToSend) {
		synchronized (lock) {
			if (isConnected()) {
				if (resourceRequirementsToSend == currentResourceRequirements) {
					return service.declareResourceRequirements(resourceRequirementsToSend);
				} else {
					LOG.debug("Newer resource requirements found. Stop sending old requirements.");
					return FutureUtils.completedExceptionally(new CancellationException());
				}
			} else {
				LOG.debug("Stop sending resource requirements to ResourceManager because it is not connected.");
				return FutureUtils.completedExceptionally(new CancellationException());
			}
		}
	}

	public static DeclareResourceRequirementServiceConnectionManager create(ScheduledExecutor scheduledExecutor) {
		return new DefaultDeclareResourceRequirementServiceConnectionManager(scheduledExecutor);
	}
}
