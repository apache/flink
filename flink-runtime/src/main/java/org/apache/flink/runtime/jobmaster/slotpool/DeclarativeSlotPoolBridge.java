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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.AllocatedSlotInfo;
import org.apache.flink.runtime.jobmaster.AllocatedSlotReport;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.runtime.slots.ResourceRequirements;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.clock.Clock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * {@link SlotPool} implementation which uses the {@link DeclarativeSlotPool} to allocate slots.
 */
public class DeclarativeSlotPoolBridge implements SlotPool {

	private static final Logger LOG = LoggerFactory.getLogger(DeclarativeSlotPoolBridge.class);

	private final JobID jobId;

	private final Map<SlotRequestId, PendingRequest> pendingRequests;
	private final Map<SlotRequestId, AllocationID> fulfilledRequests;
	private final DeclarativeSlotPool declarativeSlotPool;
	private final Set<ResourceID> registeredTaskManagers;

	@Nullable
	private ComponentMainThreadExecutor componentMainThreadExecutor;

	@Nullable
	private String jobManagerAddress;

	@Nullable
	private JobMasterId jobMasterId;

	private DeclareResourceRequirementServiceConnectionManager declareResourceRequirementServiceConnectionManager;

	private final Clock clock;
	private final Time rpcTimeout;
	private final Time idleSlotTimeout;
	private final Time batchSlotTimeout;
	private boolean isBatchSlotRequestTimeoutCheckDisabled;

	public DeclarativeSlotPoolBridge(
			JobID jobId,
			DeclarativeSlotPoolFactory declarativeSlotPoolFactory,
			Clock clock,
			Time rpcTimeout,
			Time idleSlotTimeout,
			Time batchSlotTimeout) {
		this.jobId = Preconditions.checkNotNull(jobId);
		this.clock = Preconditions.checkNotNull(clock);
		this.rpcTimeout = Preconditions.checkNotNull(rpcTimeout);
		this.idleSlotTimeout = Preconditions.checkNotNull(idleSlotTimeout);
		this.batchSlotTimeout = Preconditions.checkNotNull(batchSlotTimeout);
		this.isBatchSlotRequestTimeoutCheckDisabled = false;

		this.pendingRequests = new LinkedHashMap<>();
		this.fulfilledRequests = new HashMap<>();
		this.registeredTaskManagers = new HashSet<>();
		this.declareResourceRequirementServiceConnectionManager = NoOpDeclareResourceRequirementServiceConnectionManager.INSTANCE;
		this.declarativeSlotPool = declarativeSlotPoolFactory.create(
				this::declareResourceRequirements,
				this::newSlotsAreAvailable,
				idleSlotTimeout,
				rpcTimeout);
	}

	@Override
	public void start(JobMasterId jobMasterId, String newJobManagerAddress, ComponentMainThreadExecutor jmMainThreadScheduledExecutor) throws Exception {
		this.componentMainThreadExecutor = Preconditions.checkNotNull(jmMainThreadScheduledExecutor);
		this.jobManagerAddress = Preconditions.checkNotNull(newJobManagerAddress);
		this.jobMasterId = Preconditions.checkNotNull(jobMasterId);
		this.declareResourceRequirementServiceConnectionManager = DefaultDeclareResourceRequirementServiceConnectionManager.create(componentMainThreadExecutor);

		componentMainThreadExecutor.schedule(this::checkIdleSlotTimeout, idleSlotTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);
		componentMainThreadExecutor.schedule(this::checkBatchSlotTimeout, batchSlotTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);
	}

	@Override
	public void suspend() {
		assertRunningInMainThread();
		LOG.info("Suspending slot pool.");

		cancelPendingRequests(request -> true, new FlinkException("Suspending slot pool."));
		clearState();
	}

	@Override
	public void close() {
		LOG.info("Closing slot pool.");
		final FlinkException cause = new FlinkException("Closing slot pool");
		cancelPendingRequests(request -> true, cause);
		releaseAllTaskManagers(new FlinkException("Closing slot pool."));
		clearState();
	}

	private void cancelPendingRequests(Predicate<PendingRequest> requestPredicate, FlinkException cancelCause) {
		ResourceCounter decreasedResourceRequirements = ResourceCounter.empty();

		// need a copy since failing a request could trigger another request to be issued
		final Iterable<PendingRequest> pendingRequestsToFail = new ArrayList<>(pendingRequests.values());
		pendingRequests.clear();

		for (PendingRequest pendingRequest : pendingRequestsToFail) {
			if (requestPredicate.test(pendingRequest)) {
				pendingRequest.failRequest(cancelCause);
				decreasedResourceRequirements = decreasedResourceRequirements.add(pendingRequest.getResourceProfile(), 1);
			} else {
				pendingRequests.put(pendingRequest.getSlotRequestId(), pendingRequest);
			}
		}

		if (!decreasedResourceRequirements.isEmpty()) {
			declarativeSlotPool.decreaseResourceRequirementsBy(decreasedResourceRequirements);
		}
	}

	private void clearState() {
		declareResourceRequirementServiceConnectionManager.close();
		declareResourceRequirementServiceConnectionManager = NoOpDeclareResourceRequirementServiceConnectionManager.INSTANCE;
		registeredTaskManagers.clear();
		jobManagerAddress = null;
		jobMasterId = null;
	}

	@Override
	public void connectToResourceManager(ResourceManagerGateway resourceManagerGateway) {
		assertRunningInMainThread();
		Preconditions.checkNotNull(resourceManagerGateway);

		declareResourceRequirementServiceConnectionManager.connect(resourceRequirements -> resourceManagerGateway.declareRequiredResources(jobMasterId, resourceRequirements, rpcTimeout));
		declareResourceRequirements(declarativeSlotPool.getResourceRequirements());
	}

	@Override
	public void disconnectResourceManager() {
		assertRunningInMainThread();
		this.declareResourceRequirementServiceConnectionManager.disconnect();
	}

	@Override
	public boolean registerTaskManager(ResourceID resourceID) {
		assertRunningInMainThread();

		LOG.debug("Register new TaskExecutor {}.", resourceID);
		return registeredTaskManagers.add(resourceID);
	}

	@Override
	public boolean releaseTaskManager(ResourceID resourceId, Exception cause) {
		assertRunningInMainThread();

		if (registeredTaskManagers.remove(resourceId)) {
			internalReleaseTaskManager(resourceId, cause);
			return true;
		} else {
			return false;
		}
	}

	private void releaseAllTaskManagers(FlinkException cause) {
		for (ResourceID registeredTaskManager : registeredTaskManagers) {
			internalReleaseTaskManager(registeredTaskManager, cause);
		}

		registeredTaskManagers.clear();
	}

	private void internalReleaseTaskManager(ResourceID resourceId, Exception cause) {
		ResourceCounter previouslyFulfilledRequirement = declarativeSlotPool.releaseSlots(resourceId, cause);
		declarativeSlotPool.decreaseResourceRequirementsBy(previouslyFulfilledRequirement);
	}

	@Override
	public Collection<SlotOffer> offerSlots(TaskManagerLocation taskManagerLocation, TaskManagerGateway taskManagerGateway, Collection<SlotOffer> offers) {
		assertRunningInMainThread();
		Preconditions.checkNotNull(taskManagerGateway);
		Preconditions.checkNotNull(offers);

		if (!registeredTaskManagers.contains(taskManagerLocation.getResourceID())) {
			return Collections.emptyList();
		}

		return declarativeSlotPool.offerSlots(offers, taskManagerLocation, taskManagerGateway, clock.relativeTimeMillis());
	}

	@VisibleForTesting
	void newSlotsAreAvailable(Collection<? extends PhysicalSlot> newSlots) {
		final Collection<PendingRequestSlotMatching> matchingsToFulfill = new ArrayList<>();

		for (PhysicalSlot newSlot : newSlots) {
			final Optional<PendingRequest> matchingPendingRequest = findMatchingPendingRequest(newSlot);

			matchingPendingRequest.ifPresent(pendingRequest -> {
				Preconditions.checkNotNull(pendingRequests.remove(pendingRequest.getSlotRequestId()), "Cannot fulfill a non existing pending slot request.");
				reserveFreeSlot(pendingRequest.getSlotRequestId(), newSlot.getAllocationId(), pendingRequest.resourceProfile);

				matchingsToFulfill.add(PendingRequestSlotMatching.createFor(pendingRequest, newSlot));
			});
		}

		// we have to first reserve all matching slots before fulfilling the requests
		// otherwise it can happen that the SchedulerImpl reserves one of the new slots
		// for a request which has been triggered by fulfilling a pending request
		for (PendingRequestSlotMatching pendingRequestSlotMatching : matchingsToFulfill) {
			pendingRequestSlotMatching.fulfillPendingRequest();
		}
	}

	private void reserveFreeSlot(SlotRequestId slotRequestId, AllocationID allocationId, ResourceProfile resourceProfile) {
		LOG.debug("Reserve slot {} for slot request id {}", allocationId, slotRequestId);
		declarativeSlotPool.reserveFreeSlot(allocationId, resourceProfile);
		fulfilledRequests.put(slotRequestId, allocationId);
	}

	private Optional<PendingRequest> findMatchingPendingRequest(PhysicalSlot slot) {
		final ResourceProfile resourceProfile = slot.getResourceProfile();

		for (PendingRequest pendingRequest : pendingRequests.values()) {
			if (resourceProfile.isMatching(pendingRequest.getResourceProfile())) {
				LOG.debug("Matched slot {} to pending request {}.", slot, pendingRequest);
				return Optional.of(pendingRequest);
			}
		}
		LOG.debug("Could not match slot {} to any pending request.", slot);

		return Optional.empty();
	}

	@Override
	public Optional<PhysicalSlot> allocateAvailableSlot(@Nonnull SlotRequestId slotRequestId, @Nonnull AllocationID allocationID, @Nonnull ResourceProfile requirementProfile) {
		assertRunningInMainThread();
		Preconditions.checkNotNull(requirementProfile, "The requiredSlotProfile must not be null.");

		LOG.debug("Reserving free slot {} for slot request id {} and profile {}.", allocationID, slotRequestId, requirementProfile);

		return Optional.of(reserveFreeSlotForResource(slotRequestId, allocationID, requirementProfile));
	}

	private PhysicalSlot reserveFreeSlotForResource(SlotRequestId slotRequestId, AllocationID allocationId, ResourceProfile requiredSlotProfile) {
		declarativeSlotPool.increaseResourceRequirementsBy(ResourceCounter.withResource(requiredSlotProfile, 1));
		final PhysicalSlot physicalSlot = declarativeSlotPool.reserveFreeSlot(allocationId, requiredSlotProfile);
		fulfilledRequests.put(slotRequestId, allocationId);

		return physicalSlot;
	}

	@Override
	@Nonnull
	public CompletableFuture<PhysicalSlot> requestNewAllocatedSlot(@Nonnull SlotRequestId slotRequestId, @Nonnull ResourceProfile resourceProfile, @Nullable Time timeout) {
		assertRunningInMainThread();

		LOG.debug("Request new allocated slot with slot request id {} and resource profile {}", slotRequestId, resourceProfile);

		final PendingRequest pendingRequest = PendingRequest.createNormalRequest(slotRequestId, resourceProfile);

		return internalRequestNewSlot(pendingRequest, timeout);
	}

	@Override
	@Nonnull
	public CompletableFuture<PhysicalSlot> requestNewAllocatedBatchSlot(@Nonnull SlotRequestId slotRequestId, @Nonnull ResourceProfile resourceProfile) {
		assertRunningInMainThread();

		LOG.debug("Request new allocated batch slot with slot request id {} and resource profile {}", slotRequestId, resourceProfile);

		final PendingRequest pendingRequest = PendingRequest.createBatchRequest(slotRequestId, resourceProfile);

		return internalRequestNewSlot(pendingRequest, null);
	}

	private CompletableFuture<PhysicalSlot> internalRequestNewSlot(PendingRequest pendingRequest, @Nullable Time timeout) {
		internalRequestNewAllocatedSlot(pendingRequest);

		if (timeout == null) {
			return pendingRequest.getSlotFuture();
		} else {
			return FutureUtils
				.orTimeout(
					pendingRequest.getSlotFuture(),
					timeout.toMilliseconds(),
					TimeUnit.MILLISECONDS,
					componentMainThreadExecutor)
				.whenComplete((physicalSlot, throwable) -> {
					if (throwable instanceof TimeoutException) {
						timeoutPendingSlotRequest(pendingRequest.getSlotRequestId());
					}
				});
		}
	}

	private void timeoutPendingSlotRequest(SlotRequestId slotRequestId) {
		releaseSlot(slotRequestId, new TimeoutException("Pending slot request timed out in slot pool."));
	}

	private void internalRequestNewAllocatedSlot(PendingRequest pendingRequest) {
		pendingRequests.put(pendingRequest.getSlotRequestId(), pendingRequest);

		declarativeSlotPool.increaseResourceRequirementsBy(ResourceCounter.withResource(pendingRequest.getResourceProfile(), 1));
	}

	private void declareResourceRequirements(Collection<ResourceRequirement> resourceRequirements) {
		assertRunningInMainThread();

		LOG.debug("Declare new resource requirements for job {}: {}.", jobId, resourceRequirements);

		declareResourceRequirementServiceConnectionManager.declareResourceRequirements(ResourceRequirements.create(jobId, jobManagerAddress, resourceRequirements));
	}

	@Override
	public Optional<ResourceID> failAllocation(AllocationID allocationID, Exception cause) {
		throw new UnsupportedOperationException("Please call failAllocation(ResourceID, AllocationID, Exception)");
	}

	@Override
	public Optional<ResourceID> failAllocation(@Nullable ResourceID resourceId, AllocationID allocationID, Exception cause) {
		assertRunningInMainThread();

		Preconditions.checkNotNull(allocationID);
		Preconditions.checkNotNull(resourceId, "This slot pool only supports failAllocation calls coming from the TaskExecutor.");

		ResourceCounter previouslyFulfilledRequirements = declarativeSlotPool.releaseSlot(allocationID, cause);
		if (!previouslyFulfilledRequirements.isEmpty()) {
			declarativeSlotPool.decreaseResourceRequirementsBy(previouslyFulfilledRequirements);
		}

		if (declarativeSlotPool.containsSlots(resourceId)) {
			return Optional.empty();
		} else {
			return Optional.of(resourceId);
		}
	}

	@Override
	public void releaseSlot(@Nonnull SlotRequestId slotRequestId, @Nullable Throwable cause) {
		LOG.debug("Release slot with slot request id {}", slotRequestId);
		assertRunningInMainThread();

		final PendingRequest pendingRequest = pendingRequests.remove(slotRequestId);

		if (pendingRequest != null) {
			declarativeSlotPool.decreaseResourceRequirementsBy(ResourceCounter.withResource(pendingRequest.getResourceProfile(), 1));
			pendingRequest.failRequest(new FlinkException(
					String.format("Pending slot request with %s has been released.", pendingRequest.getSlotRequestId()),
					cause));
		} else {
			final AllocationID allocationId = fulfilledRequests.remove(slotRequestId);

			if (allocationId != null) {
				ResourceCounter previouslyFulfilledRequirement = declarativeSlotPool.freeReservedSlot(allocationId, cause, clock.relativeTimeMillis());
				if (!previouslyFulfilledRequirement.isEmpty()) {
					declarativeSlotPool.decreaseResourceRequirementsBy(previouslyFulfilledRequirement);
				}
			} else {
				LOG.debug("Could not find slot which has fulfilled slot request {}. Ignoring the release operation.", slotRequestId);
			}
		}
	}

	@Override
	public void notifyNotEnoughResourcesAvailable(Collection<ResourceRequirement> acquiredResources) {
		assertRunningInMainThread();

		failPendingRequests();
	}

	private void failPendingRequests() {
		if (!pendingRequests.isEmpty()) {
			final NoResourceAvailableException cause = new NoResourceAvailableException("Could not acquire the minimum required resources.");

			cancelPendingRequests(
				request -> !isBatchSlotRequestTimeoutCheckDisabled || !request.isBatchRequest(), cause
			);
		}
	}

	@Override
	public AllocatedSlotReport createAllocatedSlotReport(ResourceID taskManagerId) {
		assertRunningInMainThread();
		Preconditions.checkNotNull(taskManagerId);

		final Collection<? extends SlotInfo> allocatedSlotsInformation = declarativeSlotPool.getAllSlotsInformation();

		final Collection<AllocatedSlotInfo> allocatedSlotInfos = new ArrayList<>();

		for (SlotInfo slotInfo : allocatedSlotsInformation) {
			if (slotInfo.getTaskManagerLocation().getResourceID().equals(taskManagerId)) {
				allocatedSlotInfos.add(new AllocatedSlotInfo(
						slotInfo.getPhysicalSlotNumber(),
						slotInfo.getAllocationId()));
			}
		}

		return new AllocatedSlotReport(jobId, allocatedSlotInfos);
	}

	@Override
	public Collection<SlotInfo> getAllocatedSlotsInformation() {
		assertRunningInMainThread();

		final Collection<? extends SlotInfo> allSlotsInformation = declarativeSlotPool.getAllSlotsInformation();
		final Set<AllocationID> freeSlots = declarativeSlotPool.getFreeSlotsInformation().stream()
				.map(SlotInfoWithUtilization::getAllocationId)
				.collect(Collectors.toSet());

		return allSlotsInformation.stream()
				.filter(slotInfo -> !freeSlots.contains(slotInfo.getAllocationId()))
				.collect(Collectors.toList());
	}

	@Override
	@Nonnull
	public Collection<SlotInfoWithUtilization> getAvailableSlotsInformation() {
		assertRunningInMainThread();

		return declarativeSlotPool.getFreeSlotsInformation();
	}

	@Override
	public void disableBatchSlotRequestTimeoutCheck() {
		isBatchSlotRequestTimeoutCheckDisabled = true;
	}

	private void assertRunningInMainThread() {
		if (componentMainThreadExecutor != null) {
			componentMainThreadExecutor.assertRunningInMainThread();
		} else {
			throw new IllegalStateException("The FutureSlotPool has not been started yet.");
		}
	}

	private void checkIdleSlotTimeout() {
		declarativeSlotPool.releaseIdleSlots(clock.relativeTimeMillis());

		if (componentMainThreadExecutor != null) {
			componentMainThreadExecutor.schedule(this::checkIdleSlotTimeout, idleSlotTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);
		}
	}

	private void checkBatchSlotTimeout() {
		if (isBatchSlotRequestTimeoutCheckDisabled) {
			return;
		}

		final Collection<PendingRequest> pendingBatchRequests = getPendingBatchRequests();

		if (!pendingBatchRequests.isEmpty()) {
			final Set<ResourceProfile> allResourceProfiles = getResourceProfilesFromAllSlots();

			final Map<Boolean, List<PendingRequest>> fulfillableAndUnfulfillableRequests = pendingBatchRequests
					.stream()
					.collect(Collectors.partitioningBy(canBeFulfilledWithAnySlot(allResourceProfiles)));

			final List<PendingRequest> fulfillableRequests = fulfillableAndUnfulfillableRequests.get(true);
			final List<PendingRequest> unfulfillableRequests = fulfillableAndUnfulfillableRequests.get(false);

			final long currentTimestamp = clock.relativeTimeMillis();

			for (PendingRequest fulfillableRequest : fulfillableRequests) {
				fulfillableRequest.markFulfillable();
			}

			for (PendingRequest unfulfillableRequest : unfulfillableRequests) {
				unfulfillableRequest.markUnfulfillable(currentTimestamp);

				if (unfulfillableRequest.getUnfulfillableSince() + batchSlotTimeout.toMilliseconds() <= currentTimestamp) {
					timeoutPendingSlotRequest(unfulfillableRequest.getSlotRequestId());
				}
			}
		}

		if (componentMainThreadExecutor != null) {
			componentMainThreadExecutor.schedule(this::checkBatchSlotTimeout, batchSlotTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);
		}
	}

	private Set<ResourceProfile> getResourceProfilesFromAllSlots() {
		return Stream
				.concat(
						getAvailableSlotsInformation().stream(),
						getAllocatedSlotsInformation().stream())
				.map(SlotInfo::getResourceProfile)
				.collect(Collectors.toSet());
	}

	private Collection<PendingRequest> getPendingBatchRequests() {
		return pendingRequests.values().stream()
				.filter(PendingRequest::isBatchRequest)
				.collect(Collectors.toList());
	}

	private static Predicate<PendingRequest> canBeFulfilledWithAnySlot(Set<ResourceProfile> allocatedResourceProfiles) {
		return pendingRequest -> {
			for (ResourceProfile allocatedResourceProfile : allocatedResourceProfiles) {
				if (allocatedResourceProfile.isMatching(pendingRequest.getResourceProfile())) {
					return true;
				}
			}

			return false;
		};
	}

	private static final class PendingRequest {

		private final SlotRequestId slotRequestId;

		private final ResourceProfile resourceProfile;

		private final CompletableFuture<PhysicalSlot> slotFuture;

		private final boolean isBatchRequest;

		private long unfulfillableSince;

		private PendingRequest(SlotRequestId slotRequestId, ResourceProfile resourceProfile, boolean isBatchRequest) {
			this.slotRequestId = slotRequestId;
			this.resourceProfile = resourceProfile;
			this.isBatchRequest = isBatchRequest;
			this.slotFuture = new CompletableFuture<>();
			this.unfulfillableSince = Long.MAX_VALUE;
		}

		static PendingRequest createBatchRequest(SlotRequestId slotRequestId, ResourceProfile resourceProfile) {
			return new PendingRequest(
					slotRequestId,
					resourceProfile,
					true);
		}

		static PendingRequest createNormalRequest(SlotRequestId slotRequestId, ResourceProfile resourceProfile) {
			return new PendingRequest(
					slotRequestId,
					resourceProfile,
					false);
		}

		SlotRequestId getSlotRequestId() {
			return slotRequestId;
		}

		ResourceProfile getResourceProfile() {
			return resourceProfile;
		}

		CompletableFuture<PhysicalSlot> getSlotFuture() {
			return slotFuture;
		}

		void failRequest(Exception cause) {
			slotFuture.completeExceptionally(cause);
		}

		public boolean isBatchRequest() {
			return isBatchRequest;
		}

		public void markFulfillable() {
			this.unfulfillableSince = Long.MAX_VALUE;
		}

		public void markUnfulfillable(long currentTimestamp) {
			this.unfulfillableSince = currentTimestamp;
		}

		public long getUnfulfillableSince() {
			return unfulfillableSince;
		}

		public boolean fulfill(PhysicalSlot slot) {
			return slotFuture.complete(slot);
		}

		@Override
		public String toString() {
			return "PendingRequest{" +
					"slotRequestId=" + slotRequestId +
					", resourceProfile=" + resourceProfile +
					", isBatchRequest=" + isBatchRequest +
					", unfulfillableSince=" + unfulfillableSince +
					'}';
		}
	}

	private static final class PendingRequestSlotMatching {
		private final PendingRequest pendingRequest;
		private final PhysicalSlot matchedSlot;

		private PendingRequestSlotMatching(PendingRequest pendingRequest, PhysicalSlot matchedSlot) {
			this.pendingRequest = pendingRequest;
			this.matchedSlot = matchedSlot;
		}

		public static PendingRequestSlotMatching createFor(PendingRequest pendingRequest, PhysicalSlot newSlot) {
			return new PendingRequestSlotMatching(pendingRequest, newSlot);
		}

		public void fulfillPendingRequest() {
			Preconditions.checkState(pendingRequest.fulfill(matchedSlot), "Pending requests must be fulfillable.");
		}
	}
}
