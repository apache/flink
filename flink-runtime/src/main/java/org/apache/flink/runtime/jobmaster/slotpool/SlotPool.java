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

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationConstraint;
import org.apache.flink.runtime.jobmanager.scheduler.Locality;
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;
import org.apache.flink.runtime.jobmanager.scheduler.ScheduledUnit;
import org.apache.flink.runtime.jobmanager.slots.SlotAndLocality;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotContext;
import org.apache.flink.runtime.jobmaster.SlotOwner;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.SlotRequest;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.util.clock.Clock;
import org.apache.flink.runtime.util.clock.SystemClock;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The slot pool serves slot request issued by {@link ExecutionGraph}. It will will attempt to acquire new slots
 * from the ResourceManager when it cannot serve a slot request. If no ResourceManager is currently available,
 * or it gets a decline from the ResourceManager, or a request times out, it fails the slot request. The slot pool also
 * holds all the slots that were offered to it and accepted, and can thus provides registered free slots even if the
 * ResourceManager is down. The slots will only be released when they are useless, e.g. when the job is fully running
 * but we still have some free slots.
 *
 * <p>All the allocation or the slot offering will be identified by self generated AllocationID, we will use it to
 * eliminate ambiguities.
 *
 * TODO : Make pending requests location preference aware
 * TODO : Make pass location preferences to ResourceManager when sending a slot request
 */
public class SlotPool extends RpcEndpoint implements SlotPoolGateway, AllocatedSlotActions {

	/** The log for the pool - shared also with the internal classes. */
	static final Logger LOG = LoggerFactory.getLogger(SlotPool.class);

	// ------------------------------------------------------------------------

	private static final Time DEFAULT_SLOT_REQUEST_TIMEOUT = Time.minutes(5);

	private static final Time DEFAULT_RM_ALLOCATION_TIMEOUT = Time.minutes(10);

	private static final Time DEFAULT_RM_REQUEST_TIMEOUT = Time.seconds(10);

	// ------------------------------------------------------------------------

	private final JobID jobId;

	private final ProviderAndOwner providerAndOwner;

	/** All registered TaskManagers, slots will be accepted and used only if the resource is registered. */
	private final HashSet<ResourceID> registeredTaskManagers;

	/** The book-keeping of all allocated slots. */
	private final AllocatedSlots allocatedSlots;

	/** The book-keeping of all available slots. */
	private final AvailableSlots availableSlots;

	/** All pending requests waiting for slots. */
	private final DualKeyMap<SlotRequestId, AllocationID, PendingRequest> pendingRequests;

	/** The requests that are waiting for the resource manager to be connected. */
	private final HashMap<SlotRequestId, PendingRequest> waitingForResourceManager;

	/** Timeout for request calls to the ResourceManager. */
	private final Time resourceManagerRequestsTimeout;

	/** Timeout for allocation round trips (RM -> launch TM -> offer slot). */
	private final Time resourceManagerAllocationTimeout;

	private final Clock clock;

	/** Managers for the different slot sharing groups. */
	protected final Map<SlotSharingGroupId, SlotSharingManager> slotSharingManagers;

	/** the fencing token of the job manager. */
	private JobMasterId jobMasterId;

	/** The gateway to communicate with resource manager. */
	private ResourceManagerGateway resourceManagerGateway;

	private String jobManagerAddress;

	// ------------------------------------------------------------------------

	public SlotPool(RpcService rpcService, JobID jobId) {
		this(rpcService, jobId, SystemClock.getInstance(),
				DEFAULT_SLOT_REQUEST_TIMEOUT, DEFAULT_RM_ALLOCATION_TIMEOUT, DEFAULT_RM_REQUEST_TIMEOUT);
	}

	public SlotPool(
			RpcService rpcService,
			JobID jobId,
			Clock clock,
			Time slotRequestTimeout,
			Time resourceManagerAllocationTimeout,
			Time resourceManagerRequestTimeout) {

		super(rpcService);

		this.jobId = checkNotNull(jobId);
		this.clock = checkNotNull(clock);
		this.resourceManagerRequestsTimeout = checkNotNull(resourceManagerRequestTimeout);
		this.resourceManagerAllocationTimeout = checkNotNull(resourceManagerAllocationTimeout);

		this.registeredTaskManagers = new HashSet<>();
		this.allocatedSlots = new AllocatedSlots();
		this.availableSlots = new AvailableSlots();
		this.pendingRequests = new DualKeyMap<>(16);
		this.waitingForResourceManager = new HashMap<>(16);

		this.providerAndOwner = new ProviderAndOwner(getSelfGateway(SlotPoolGateway.class), slotRequestTimeout);

		this.slotSharingManagers = new HashMap<>(4);

		this.jobMasterId = null;
		this.resourceManagerGateway = null;
		this.jobManagerAddress = null;
	}

	// ------------------------------------------------------------------------
	//  Starting and Stopping
	// ------------------------------------------------------------------------

	@Override
	public void start() {
		throw new UnsupportedOperationException("Should never call start() without leader ID");
	}

	/**
	 * Start the slot pool to accept RPC calls.
	 *
	 * @param jobMasterId The necessary leader id for running the job.
	 * @param newJobManagerAddress for the slot requests which are sent to the resource manager
	 */
	public void start(JobMasterId jobMasterId, String newJobManagerAddress) throws Exception {
		this.jobMasterId = checkNotNull(jobMasterId);
		this.jobManagerAddress = checkNotNull(newJobManagerAddress);

		// TODO - start should not throw an exception
		try {
			super.start();
		} catch (Exception e) {
			throw new RuntimeException("This should never happen", e);
		}
	}

	/**
	 * Suspends this pool, meaning it has lost its authority to accept and distribute slots.
	 */
	@Override
	public void suspend() {
		validateRunsInMainThread();

		// suspend this RPC endpoint
		stop();

		// do not accept any requests
		jobMasterId = null;
		resourceManagerGateway = null;

		// Clear (but not release!) the available slots. The TaskManagers should re-register them
		// at the new leader JobManager/SlotPool
		availableSlots.clear();
		allocatedSlots.clear();
		pendingRequests.clear();
	}

	// ------------------------------------------------------------------------
	//  Getting PoolOwner and PoolProvider
	// ------------------------------------------------------------------------

	/**
	 * Gets the slot owner implementation for this pool.
	 *
	 * <p>This method does not mutate state and can be called directly (no RPC indirection)
	 *
	 * @return The slot owner implementation for this pool.
	 */
	public SlotOwner getSlotOwner() {
		return providerAndOwner;
	}

	/**
	 * Gets the slot provider implementation for this pool.
	 *
	 * <p>This method does not mutate state and can be called directly (no RPC indirection)
	 *
	 * @return The slot provider implementation for this pool.
	 */
	public SlotProvider getSlotProvider() {
		return providerAndOwner;
	}

	// ------------------------------------------------------------------------
	//  Resource Manager Connection
	// ------------------------------------------------------------------------

	@Override
	public void connectToResourceManager(ResourceManagerGateway resourceManagerGateway) {
		this.resourceManagerGateway = checkNotNull(resourceManagerGateway);

		// work on all slots waiting for this connection
		for (PendingRequest pendingRequest : waitingForResourceManager.values()) {
			requestSlotFromResourceManager(resourceManagerGateway, pendingRequest);
		}

		// all sent off
		waitingForResourceManager.clear();
	}

	@Override
	public void disconnectResourceManager() {
		this.resourceManagerGateway = null;
	}

	// ------------------------------------------------------------------------
	//  Slot Allocation
	// ------------------------------------------------------------------------

	@Override
	public CompletableFuture<LogicalSlot> allocateSlot(
			SlotRequestId slotRequestId,
			ScheduledUnit scheduledUnit,
			ResourceProfile resourceProfile,
			Collection<TaskManagerLocation> locationPreferences,
			boolean allowQueuedScheduling,
			Time timeout) {

		return internalAllocateSlot(
			slotRequestId,
			scheduledUnit,
			resourceProfile,
			locationPreferences,
			allowQueuedScheduling);
	}

	private CompletableFuture<LogicalSlot> internalAllocateSlot(
			SlotRequestId slotRequestId,
			ScheduledUnit task,
			ResourceProfile resourceProfile,
			Collection<TaskManagerLocation> locationPreferences,
			boolean allowQueuedScheduling) {

		final SlotSharingGroupId slotSharingGroupId = task.getSlotSharingGroupId();

		if (slotSharingGroupId != null) {
			// allocate slot with slot sharing
			final SlotSharingManager multiTaskSlotManager = slotSharingManagers.computeIfAbsent(
				slotSharingGroupId,
				id -> new SlotSharingManager(
					id,
					this,
					providerAndOwner));

			final SlotSharingManager.MultiTaskSlotLocality multiTaskSlotLocality;

			try {
				if (task.getCoLocationConstraint() != null) {
					multiTaskSlotLocality = allocateCoLocatedMultiTaskSlot(
						task.getCoLocationConstraint(),
						multiTaskSlotManager,
						resourceProfile,
						locationPreferences,
						allowQueuedScheduling);
				} else {
					multiTaskSlotLocality = allocateMultiTaskSlot(
						task.getJobVertexId(), multiTaskSlotManager,
						resourceProfile,
						locationPreferences,
						allowQueuedScheduling);
				}
			} catch (NoResourceAvailableException noResourceException) {
				return FutureUtils.completedExceptionally(noResourceException);
			}

			// sanity check
			Preconditions.checkState(!multiTaskSlotLocality.getMultiTaskSlot().contains(task.getJobVertexId()));

			final SlotSharingManager.SingleTaskSlot leaf = multiTaskSlotLocality.getMultiTaskSlot().allocateSingleTaskSlot(
				slotRequestId,
				task.getJobVertexId(),
				multiTaskSlotLocality.getLocality());

			return leaf.getLogicalSlotFuture();
		} else {
			// request an allocated slot to assign a single logical slot to
			CompletableFuture<SlotAndLocality> slotAndLocalityFuture = requestAllocatedSlot(
				slotRequestId,
				resourceProfile,
				locationPreferences,
				allowQueuedScheduling);

			return slotAndLocalityFuture.thenApply(
				(SlotAndLocality slotAndLocality) -> {
					final AllocatedSlot allocatedSlot = slotAndLocality.getSlot();

					final SingleLogicalSlot singleTaskSlot = new SingleLogicalSlot(
						slotRequestId,
						allocatedSlot,
						null,
						slotAndLocality.getLocality(),
						providerAndOwner);

					if (allocatedSlot.tryAssignPayload(singleTaskSlot)) {
						return singleTaskSlot;
					} else {
						final FlinkException flinkException = new FlinkException("Could not assign payload to allocated slot " + allocatedSlot.getAllocationId() + '.');
						releaseSlot(slotRequestId, null, flinkException);
						throw new CompletionException(flinkException);
					}
				});
		}
	}

	/**
	 * Allocates a co-located {@link SlotSharingManager.MultiTaskSlot} for the given {@link CoLocationConstraint}.
	 *
	 * <p>If allowQueuedScheduling is true, then the returned {@link SlotSharingManager.MultiTaskSlot} can be
	 * uncompleted.
	 *
	 * @param coLocationConstraint for which to allocate a {@link SlotSharingManager.MultiTaskSlot}
	 * @param multiTaskSlotManager responsible for the slot sharing group for which to allocate the slot
	 * @param resourceProfile specifying the requirements for the requested slot
	 * @param locationPreferences containing preferred TaskExecutors on which to allocate the slot
	 * @param allowQueuedScheduling true if queued scheduling (the returned task slot must not be completed yet) is allowed, otherwise false
	 * @return A {@link SlotSharingManager.MultiTaskSlotLocality} which contains the allocated{@link SlotSharingManager.MultiTaskSlot}
	 * 		and its locality wrt the given location preferences
	 * @throws NoResourceAvailableException if no task slot could be allocated
	 */
	private SlotSharingManager.MultiTaskSlotLocality allocateCoLocatedMultiTaskSlot(
			CoLocationConstraint coLocationConstraint,
			SlotSharingManager multiTaskSlotManager,
			ResourceProfile resourceProfile,
			Collection<TaskManagerLocation> locationPreferences,
			boolean allowQueuedScheduling) throws NoResourceAvailableException {
		final SlotRequestId coLocationSlotRequestId = coLocationConstraint.getSlotRequestId();

		if (coLocationSlotRequestId != null) {
			// we have a slot assigned --> try to retrieve it
			final SlotSharingManager.TaskSlot taskSlot = multiTaskSlotManager.getTaskSlot(coLocationSlotRequestId);

			if (taskSlot != null) {
				Preconditions.checkState(taskSlot instanceof SlotSharingManager.MultiTaskSlot);
				return SlotSharingManager.MultiTaskSlotLocality.of(((SlotSharingManager.MultiTaskSlot) taskSlot), Locality.LOCAL);
			} else {
				// the slot may have been cancelled in the mean time
				coLocationConstraint.setSlotRequestId(null);
			}
		}

		final Collection<TaskManagerLocation> actualLocationPreferences;

		if (coLocationConstraint.isAssigned()) {
			actualLocationPreferences = Collections.singleton(coLocationConstraint.getLocation());
		} else {
			actualLocationPreferences = locationPreferences;
		}

		// get a new multi task slot
		final SlotSharingManager.MultiTaskSlotLocality multiTaskSlotLocality = allocateMultiTaskSlot(
			coLocationConstraint.getGroupId(), multiTaskSlotManager,
			resourceProfile,
			actualLocationPreferences,
			allowQueuedScheduling);

		// check whether we fulfill the co-location constraint
		if (coLocationConstraint.isAssigned() && multiTaskSlotLocality.getLocality() != Locality.LOCAL) {
			multiTaskSlotLocality.getMultiTaskSlot().release(
				new FlinkException("Multi task slot is not local and, thus, does not fulfill the co-location constraint."));

			throw new NoResourceAvailableException("Could not allocate a local multi task slot for the " +
				"co location constraint " + coLocationConstraint + '.');
		}

		final SlotRequestId slotRequestId = new SlotRequestId();
		final SlotSharingManager.MultiTaskSlot coLocationSlot = multiTaskSlotLocality.getMultiTaskSlot().allocateMultiTaskSlot(
			slotRequestId,
			coLocationConstraint.getGroupId());

		// mark the requested slot as co-located slot for other co-located tasks
		coLocationConstraint.setSlotRequestId(slotRequestId);

		// lock the co-location constraint once we have obtained the allocated slot
		coLocationSlot.getSlotContextFuture().whenComplete(
			(SlotContext slotContext, Throwable throwable) -> {
				if (throwable == null) {
					// check whether we are still assigned to the co-location constraint
					if (Objects.equals(coLocationConstraint.getSlotRequestId(), slotRequestId)) {
						coLocationConstraint.lockLocation(slotContext.getTaskManagerLocation());
					} else {
						log.debug("Failed to lock colocation constraint {} because assigned slot " +
							"request {} differs from fulfilled slot request {}.",
							coLocationConstraint.getGroupId(),
							coLocationConstraint.getSlotRequestId(),
							slotRequestId);
					}
				} else {
					log.debug("Failed to lock colocation constraint {} because the slot " +
						"allocation for slot request {} failed.",
						coLocationConstraint.getGroupId(),
						coLocationConstraint.getSlotRequestId(),
						throwable);
				}
			});

		return SlotSharingManager.MultiTaskSlotLocality.of(coLocationSlot, multiTaskSlotLocality.getLocality());
	}

	/**
	 * Allocates a {@link SlotSharingManager.MultiTaskSlot} for the given groupId which is in the
	 * slot sharing group for which the given {@link SlotSharingManager} is responsible.
	 *
	 * <p>If allowQueuedScheduling is true, then the method can return an uncompleted {@link SlotSharingManager.MultiTaskSlot}.
	 *
	 * @param groupId for which to allocate a new {@link SlotSharingManager.MultiTaskSlot}
	 * @param slotSharingManager responsible for the slot sharing group for which to allocate the slot
	 * @param resourceProfile specifying the requirements for the requested slot
	 * @param locationPreferences containing preferred TaskExecutors on which to allocate the slot
	 * @param allowQueuedScheduling true if queued scheduling (the returned task slot must not be completed yet) is allowed, otherwise false
	 * @return A {@link SlotSharingManager.MultiTaskSlotLocality} which contains the allocated {@link SlotSharingManager.MultiTaskSlot}
	 * 		and its locality wrt the given location preferences
	 * @throws NoResourceAvailableException if no task slot could be allocated
	 */
	private SlotSharingManager.MultiTaskSlotLocality allocateMultiTaskSlot(
			AbstractID groupId,
			SlotSharingManager slotSharingManager,
			ResourceProfile resourceProfile,
			Collection<TaskManagerLocation> locationPreferences,
			boolean allowQueuedScheduling) throws NoResourceAvailableException {

		// check first whether we have a resolved root slot which we can use
		SlotSharingManager.MultiTaskSlotLocality multiTaskSlotLocality = slotSharingManager.getResolvedRootSlot(
			groupId,
			locationPreferences);

		if (multiTaskSlotLocality != null && multiTaskSlotLocality.getLocality() == Locality.LOCAL) {
			return multiTaskSlotLocality;
		}

		final SlotRequestId allocatedSlotRequestId = new SlotRequestId();
		final SlotRequestId multiTaskSlotRequestId = new SlotRequestId();

		// check whether we have an allocated slot available which we can use to create a new multi task slot in
		final SlotAndLocality polledSlotAndLocality = pollAndAllocateSlot(allocatedSlotRequestId, resourceProfile, locationPreferences);

		if (polledSlotAndLocality != null && (polledSlotAndLocality.getLocality() == Locality.LOCAL || multiTaskSlotLocality == null)) {

			final AllocatedSlot allocatedSlot = polledSlotAndLocality.getSlot();
			final SlotSharingManager.MultiTaskSlot multiTaskSlot = slotSharingManager.createRootSlot(
				multiTaskSlotRequestId,
				CompletableFuture.completedFuture(polledSlotAndLocality.getSlot()),
				allocatedSlotRequestId);

			if (allocatedSlot.tryAssignPayload(multiTaskSlot)) {
				return SlotSharingManager.MultiTaskSlotLocality.of(multiTaskSlot, polledSlotAndLocality.getLocality());
			} else {
				multiTaskSlot.release(new FlinkException("Could not assign payload to allocated slot " +
					allocatedSlot.getAllocationId() + '.'));
			}
		}

		if (multiTaskSlotLocality != null) {
			// prefer slot sharing group slots over unused slots
			if (polledSlotAndLocality != null) {
				releaseSlot(
					allocatedSlotRequestId,
					null,
					new FlinkException("Locality constraint is not better fulfilled by allocated slot."));
			}
			return multiTaskSlotLocality;
		}

		if (allowQueuedScheduling) {
			// there is no slot immediately available --> check first for uncompleted slots at the slot sharing group
			SlotSharingManager.MultiTaskSlot multiTaskSlotFuture = slotSharingManager.getUnresolvedRootSlot(groupId);

			if (multiTaskSlotFuture == null) {
				// it seems as if we have to request a new slot from the resource manager, this is always the last resort!!!
				final CompletableFuture<AllocatedSlot> futureSlot = requestNewAllocatedSlot(allocatedSlotRequestId, resourceProfile);

				multiTaskSlotFuture = slotSharingManager.createRootSlot(
					multiTaskSlotRequestId,
					futureSlot,
					allocatedSlotRequestId);

				futureSlot.whenComplete(
					(AllocatedSlot allocatedSlot, Throwable throwable) -> {
						final SlotSharingManager.TaskSlot taskSlot = slotSharingManager.getTaskSlot(multiTaskSlotRequestId);

						if (taskSlot != null) {
							// still valid
							if (!(taskSlot instanceof SlotSharingManager.MultiTaskSlot) || throwable != null) {
								taskSlot.release(throwable);
							} else {
								if (!allocatedSlot.tryAssignPayload(((SlotSharingManager.MultiTaskSlot) taskSlot))) {
									taskSlot.release(new FlinkException("Could not assign payload to allocated slot " +
										allocatedSlot.getAllocationId() + '.'));
								}
							}
						} else {
							releaseSlot(
								allocatedSlotRequestId,
								null,
								new FlinkException("Could not find task slot with " + multiTaskSlotRequestId + '.'));
						}
					});
			}

			return SlotSharingManager.MultiTaskSlotLocality.of(multiTaskSlotFuture, Locality.UNKNOWN);

		} else {
			throw new NoResourceAvailableException("Could not allocate a shared slot for " + groupId + '.');
		}
	}

	/**
	 * Allocates an allocated slot first by polling from the available slots and then requesting a new
	 * slot from the ResourceManager if no fitting slot could be found.
	 *
	 * @param slotRequestId identifying the slot allocation request
	 * @param resourceProfile which the allocated slot should fulfill
	 * @param locationPreferences for the allocated slot
	 * @param allowQueuedScheduling true if the slot allocation can be completed in the future
	 * @return Future containing the allocated simple slot
	 */
	private CompletableFuture<SlotAndLocality> requestAllocatedSlot(
			SlotRequestId slotRequestId,
			ResourceProfile resourceProfile,
			Collection<TaskManagerLocation> locationPreferences,
			boolean allowQueuedScheduling) {

		final CompletableFuture<SlotAndLocality> allocatedSlotLocalityFuture;

		// (1) do we have a slot available already?
		SlotAndLocality slotFromPool = pollAndAllocateSlot(slotRequestId, resourceProfile, locationPreferences);

		if (slotFromPool != null) {
			allocatedSlotLocalityFuture = CompletableFuture.completedFuture(slotFromPool);
		} else if (allowQueuedScheduling) {
			// we have to request a new allocated slot
			CompletableFuture<AllocatedSlot> allocatedSlotFuture = requestNewAllocatedSlot(
				slotRequestId,
				resourceProfile);

			allocatedSlotLocalityFuture = allocatedSlotFuture.thenApply((AllocatedSlot allocatedSlot) -> new SlotAndLocality(allocatedSlot, Locality.UNKNOWN));
		} else {
			allocatedSlotLocalityFuture = FutureUtils.completedExceptionally(new NoResourceAvailableException("Could not allocate a simple slot for " +
				slotRequestId + '.'));
		}

		return allocatedSlotLocalityFuture;
	}

	/**
	 * Requests a new slot with the given {@link ResourceProfile} from the ResourceManager. If there is
	 * currently not ResourceManager connected, then the request is stashed and send once a new
	 * ResourceManager is connected.
	 *
	 * @param slotRequestId identifying the requested slot
	 * @param resourceProfile which the requested slot should fulfill
	 * @return An {@link AllocatedSlot} future which is completed once the slot is offered to the {@link SlotPool}
	 */
	private CompletableFuture<AllocatedSlot> requestNewAllocatedSlot(
		SlotRequestId slotRequestId,
		ResourceProfile resourceProfile) {

		final PendingRequest pendingRequest = new PendingRequest(
			slotRequestId,
			resourceProfile);

		if (resourceManagerGateway == null) {
			stashRequestWaitingForResourceManager(pendingRequest);
		} else {
			requestSlotFromResourceManager(resourceManagerGateway, pendingRequest);
		}

		return pendingRequest.getAllocatedSlotFuture();
	}

	private void requestSlotFromResourceManager(
			final ResourceManagerGateway resourceManagerGateway,
			final PendingRequest pendingRequest) {

		Preconditions.checkNotNull(resourceManagerGateway);
		Preconditions.checkNotNull(pendingRequest);

		LOG.info("Requesting slot with profile {} from resource manager (request = {}).", pendingRequest.getResourceProfile(), pendingRequest.getSlotRequestId());

		final AllocationID allocationId = new AllocationID();

		pendingRequests.put(pendingRequest.getSlotRequestId(), allocationId, pendingRequest);

		pendingRequest.getAllocatedSlotFuture().whenComplete(
			(value, throwable) -> {
				if (throwable != null) {
					resourceManagerGateway.cancelSlotRequest(allocationId);
				}
			});

		CompletableFuture<Acknowledge> rmResponse = resourceManagerGateway.requestSlot(
			jobMasterId,
			new SlotRequest(jobId, allocationId, pendingRequest.getResourceProfile(), jobManagerAddress),
			resourceManagerRequestsTimeout);

		CompletableFuture<Void> slotRequestProcessingFuture = rmResponse.thenAcceptAsync(
			(Acknowledge value) -> {
				slotRequestToResourceManagerSuccess(pendingRequest.getSlotRequestId());
			},
			getMainThreadExecutor());

		// on failure, fail the request future
		slotRequestProcessingFuture.whenCompleteAsync(
			(Void v, Throwable failure) -> {
				if (failure != null) {
					slotRequestToResourceManagerFailed(pendingRequest.getSlotRequestId(), failure);
				}
			},
			getMainThreadExecutor());
	}

	private void slotRequestToResourceManagerSuccess(final SlotRequestId requestId) {
		// a request is pending from the ResourceManager to a (future) TaskManager
		// we only add the watcher here in case that request times out
		scheduleRunAsync(new Runnable() {
			@Override
			public void run() {
				checkTimeoutSlotAllocation(requestId);
			}
		}, resourceManagerAllocationTimeout);
	}

	private void slotRequestToResourceManagerFailed(SlotRequestId slotRequestID, Throwable failure) {
		PendingRequest request = pendingRequests.removeKeyA(slotRequestID);
		if (request != null) {
			request.getAllocatedSlotFuture().completeExceptionally(new NoResourceAvailableException(
					"No pooled slot available and request to ResourceManager for new slot failed", failure));
		} else {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Unregistered slot request {} failed.", slotRequestID, failure);
			}
		}
	}

	private void checkTimeoutSlotAllocation(SlotRequestId slotRequestID) {
		PendingRequest request = pendingRequests.removeKeyA(slotRequestID);
		if (request != null) {
			failPendingRequest(request, new TimeoutException("Slot allocation request " + slotRequestID + " timed out"));
		}
	}

	private void stashRequestWaitingForResourceManager(final PendingRequest pendingRequest) {

		LOG.info("Cannot serve slot request, no ResourceManager connected. " +
				"Adding as pending request {}",  pendingRequest.getSlotRequestId());

		waitingForResourceManager.put(pendingRequest.getSlotRequestId(), pendingRequest);

		scheduleRunAsync(new Runnable() {
			@Override
			public void run() {
				checkTimeoutRequestWaitingForResourceManager(pendingRequest.getSlotRequestId());
			}
		}, resourceManagerRequestsTimeout);
	}

	private void checkTimeoutRequestWaitingForResourceManager(SlotRequestId slotRequestId) {
		PendingRequest request = waitingForResourceManager.remove(slotRequestId);
		if (request != null) {
			failPendingRequest(
				request,
				new NoResourceAvailableException("No slot available and no connection to Resource Manager established."));
		}
	}

	// ------------------------------------------------------------------------
	//  Slot releasing & offering
	// ------------------------------------------------------------------------

	@Override
	public CompletableFuture<Acknowledge> releaseSlot(SlotRequestId slotRequestId, @Nullable SlotSharingGroupId slotSharingGroupId, Throwable cause) {

		if (slotSharingGroupId != null) {
			final SlotSharingManager multiTaskSlotManager = slotSharingManagers.get(slotSharingGroupId);

			if (multiTaskSlotManager != null) {
				final SlotSharingManager.TaskSlot taskSlot = multiTaskSlotManager.getTaskSlot(slotRequestId);

				if (taskSlot != null) {
					taskSlot.release(cause);
				} else {
					log.debug("Could not find slot {} in slot sharing group {}. Ignoring release slot request.", slotRequestId, slotSharingGroupId, cause);
				}
			} else {
				log.debug("Could not find slot sharing group {}. Ignoring release slot request.", slotSharingGroupId, cause);
			}
		} else {
			final PendingRequest pendingRequest = removePendingRequest(slotRequestId);

			if (pendingRequest != null) {
				failPendingRequest(pendingRequest, new FlinkException("Pending slot request with " + slotRequestId + " has been released."));
			} else {
				final AllocatedSlot allocatedSlot = allocatedSlots.remove(slotRequestId);

				if (allocatedSlot != null) {
					// sanity check
					if (allocatedSlot.releasePayload(cause)) {
						tryFulfillSlotRequestOrMakeAvailable(allocatedSlot);
					}
				} else {
					log.debug("There is no allocated slot with allocation id {}. Ignoring the release slot request.", slotRequestId, cause);
				}
			}
		}

		return CompletableFuture.completedFuture(Acknowledge.get());
	}

	/**
	 * Checks whether there exists a pending request with the given allocation id and removes it
	 * from the internal data structures.
	 *
	 * @param requestId identifying the pending request
	 * @return pending request if there is one, otherwise null
	 */
	@Nullable
	private PendingRequest removePendingRequest(SlotRequestId requestId) {
		PendingRequest result = waitingForResourceManager.remove(requestId);

		if (result != null) {
			// sanity check
			assert !pendingRequests.containsKeyA(requestId) : "A pending requests should only be part of either " +
				"the pendingRequests or waitingForResourceManager but not both.";

			return result;
		} else {
			return pendingRequests.removeKeyA(requestId);
		}
	}

	private void failPendingRequest(PendingRequest pendingRequest, Exception e) {
		Preconditions.checkNotNull(pendingRequest);
		Preconditions.checkNotNull(e);

		if (!pendingRequest.getAllocatedSlotFuture().isDone()) {
			LOG.info("Failing pending request {}.", pendingRequest.getSlotRequestId());
			pendingRequest.getAllocatedSlotFuture().completeExceptionally(e);
		}
	}

	@Nullable
	private SlotAndLocality pollAndAllocateSlot(
		SlotRequestId slotRequestId,
		ResourceProfile resourceProfile,
		Collection<TaskManagerLocation> locationPreferences) {
		SlotAndLocality slotFromPool = availableSlots.poll(resourceProfile, locationPreferences);

		if (slotFromPool != null) {
			allocatedSlots.add(slotRequestId, slotFromPool.getSlot());
		}

		return slotFromPool;
	}

	/**
	 * Tries to fulfill with the given allocated slot a pending slot request or add the
	 * allocated slot to the set of available slots if no matching request is available.
	 *
	 * @param allocatedSlot which shall be returned
	 */
	private void tryFulfillSlotRequestOrMakeAvailable(AllocatedSlot allocatedSlot) {
		Preconditions.checkState(!allocatedSlot.isUsed(), "Provided slot is still in use.");

		final PendingRequest pendingRequest = pollMatchingPendingRequest(allocatedSlot);

		if (pendingRequest != null) {
			LOG.debug("Fulfilling pending request [{}] early with returned slot [{}]",
				pendingRequest.getSlotRequestId(), allocatedSlot.getAllocationId());

			allocatedSlots.add(pendingRequest.getSlotRequestId(), allocatedSlot);
			pendingRequest.getAllocatedSlotFuture().complete(allocatedSlot);
		} else {
			LOG.debug("Adding returned slot [{}] to available slots", allocatedSlot.getAllocationId());
			availableSlots.add(allocatedSlot, clock.relativeTimeMillis());
		}
	}

	private PendingRequest pollMatchingPendingRequest(final AllocatedSlot slot) {
		final ResourceProfile slotResources = slot.getResourceProfile();

		// try the requests sent to the resource manager first
		for (PendingRequest request : pendingRequests.values()) {
			if (slotResources.isMatching(request.getResourceProfile())) {
				pendingRequests.removeKeyA(request.getSlotRequestId());
				return request;
			}
		}

		// try the requests waiting for a resource manager connection next
		for (PendingRequest request : waitingForResourceManager.values()) {
			if (slotResources.isMatching(request.getResourceProfile())) {
				waitingForResourceManager.remove(request.getSlotRequestId());
				return request;
			}
		}

		// no request pending, or no request matches
		return null;
	}

	@Override
	public CompletableFuture<Collection<SlotOffer>> offerSlots(
			TaskManagerLocation taskManagerLocation,
			TaskManagerGateway taskManagerGateway,
			Collection<SlotOffer> offers) {
		validateRunsInMainThread();

		List<CompletableFuture<Optional<SlotOffer>>> acceptedSlotOffers = offers.stream().map(
			offer -> {
				CompletableFuture<Optional<SlotOffer>> acceptedSlotOffer = offerSlot(
					taskManagerLocation,
					taskManagerGateway,
					offer)
					.thenApply(
						(acceptedSlot) -> {
							if (acceptedSlot) {
								return Optional.of(offer);
							} else {
								return Optional.empty();
							}
						});

				return acceptedSlotOffer;
			}
		).collect(Collectors.toList());

		CompletableFuture<Collection<Optional<SlotOffer>>> optionalSlotOffers = FutureUtils.combineAll(acceptedSlotOffers);

		CompletableFuture<Collection<SlotOffer>> resultingSlotOffers = optionalSlotOffers.thenApply(
			collection -> {
				Collection<SlotOffer> slotOffers = collection
					.stream()
					.flatMap(
						opt -> opt.map(Stream::of).orElseGet(Stream::empty))
					.collect(Collectors.toList());

				return slotOffers;
			});

		return resultingSlotOffers;
	}

	/**
	 * Slot offering by TaskExecutor with AllocationID. The AllocationID is originally generated by this pool and
	 * transfer through the ResourceManager to TaskManager. We use it to distinguish the different allocation
	 * we issued. Slot offering may be rejected if we find something mismatching or there is actually no pending
	 * request waiting for this slot (maybe fulfilled by some other returned slot).
	 *
	 * @param taskManagerLocation location from where the offer comes from
	 * @param taskManagerGateway TaskManager gateway
	 * @param slotOffer the offered slot
	 * @return True if we accept the offering
	 */
	@Override
	public CompletableFuture<Boolean> offerSlot(
			final TaskManagerLocation taskManagerLocation,
			final TaskManagerGateway taskManagerGateway,
			final SlotOffer slotOffer) {
		validateRunsInMainThread();

		// check if this TaskManager is valid
		final ResourceID resourceID = taskManagerLocation.getResourceID();
		final AllocationID allocationID = slotOffer.getAllocationId();

		if (!registeredTaskManagers.contains(resourceID)) {
			LOG.debug("Received outdated slot offering [{}] from unregistered TaskManager: {}",
					slotOffer.getAllocationId(), taskManagerLocation);
			return CompletableFuture.completedFuture(false);
		}

		// check whether we have already using this slot
		if (allocatedSlots.contains(allocationID) || availableSlots.contains(allocationID)) {
			LOG.debug("Received repeated offer for slot [{}]. Ignoring.", allocationID);

			// return true here so that the sender will get a positive acknowledgement to the retry
			// and mark the offering as a success
			return CompletableFuture.completedFuture(true);
		}

		final AllocatedSlot allocatedSlot = new AllocatedSlot(
			slotOffer.getAllocationId(),
			taskManagerLocation,
			slotOffer.getSlotIndex(),
			slotOffer.getResourceProfile(),
			taskManagerGateway);

		// check whether we have request waiting for this slot
		PendingRequest pendingRequest = pendingRequests.removeKeyB(allocationID);
		if (pendingRequest != null) {
			// we were waiting for this!
			allocatedSlots.add(pendingRequest.getSlotRequestId(), allocatedSlot);

			if (!pendingRequest.getAllocatedSlotFuture().complete(allocatedSlot)) {
				// we could not complete the pending slot future --> try to fulfill another pending request
				allocatedSlots.remove(pendingRequest.getSlotRequestId());
				tryFulfillSlotRequestOrMakeAvailable(allocatedSlot);
			}
		}
		else {
			// we were actually not waiting for this:
			//   - could be that this request had been fulfilled
			//   - we are receiving the slots from TaskManagers after becoming leaders
			tryFulfillSlotRequestOrMakeAvailable(allocatedSlot);
		}

		// we accepted the request in any case. slot will be released after it idled for
		// too long and timed out
		return CompletableFuture.completedFuture(true);
	}


	// TODO - periodic (every minute or so) catch slots that were lost (check all slots, if they have any task active)

	// TODO - release slots that were not used to the resource manager

	// ------------------------------------------------------------------------
	//  Error Handling
	// ------------------------------------------------------------------------

	/**
	 * Fail the specified allocation and release the corresponding slot if we have one.
	 * This may triggered by JobManager when some slot allocation failed with timeout.
	 * Or this could be triggered by TaskManager, when it finds out something went wrong with the slot,
	 * and decided to take it back.
	 *
	 * @param allocationID Represents the allocation which should be failed
	 * @param cause        The cause of the failure
	 */
	@Override
	public void failAllocation(final AllocationID allocationID, final Exception cause) {
		final PendingRequest pendingRequest = pendingRequests.removeKeyB(allocationID);
		if (pendingRequest != null) {
			// request was still pending
			failPendingRequest(pendingRequest, cause);
		}
		else if (availableSlots.tryRemove(allocationID)) {
			LOG.debug("Failed available slot [{}] with ", allocationID, cause);
		}
		else {
			AllocatedSlot allocatedSlot = allocatedSlots.remove(allocationID);
			if (allocatedSlot != null) {
				// release the slot.
				// since it is not in 'allocatedSlots' any more, it will be dropped o return'
				allocatedSlot.releasePayload(cause);
			}
			else {
				LOG.debug("Outdated request to fail slot [{}] with ", allocationID, cause);
			}
		}
		// TODO: add some unit tests when the previous two are ready, the allocation may failed at any phase
	}

	// ------------------------------------------------------------------------
	//  Resource
	// ------------------------------------------------------------------------

	/**
	 * Register TaskManager to this pool, only those slots come from registered TaskManager will be considered valid.
	 * Also it provides a way for us to keep "dead" or "abnormal" TaskManagers out of this pool.
	 *
	 * @param resourceID The id of the TaskManager
	 * @return Future acknowledge if th operation was successful
	 */
	@Override
	public CompletableFuture<Acknowledge> registerTaskManager(final ResourceID resourceID) {
		registeredTaskManagers.add(resourceID);

		return CompletableFuture.completedFuture(Acknowledge.get());
	}

	/**
	 * Unregister TaskManager from this pool, all the related slots will be released and tasks be canceled. Called
	 * when we find some TaskManager becomes "dead" or "abnormal", and we decide to not using slots from it anymore.
	 *
	 * @param resourceID The id of the TaskManager
	 */
	@Override
	public CompletableFuture<Acknowledge> releaseTaskManager(final ResourceID resourceID) {
		if (registeredTaskManagers.remove(resourceID)) {
			availableSlots.removeAllForTaskManager(resourceID);

			final Set<AllocatedSlot> allocatedSlotsForResource = allocatedSlots.removeSlotsForTaskManager(resourceID);

			for (AllocatedSlot allocatedSlot : allocatedSlotsForResource) {
				allocatedSlot.releasePayload(new FlinkException("TaskManager " + resourceID + " was released."));
			}
		}

		return CompletableFuture.completedFuture(Acknowledge.get());
	}

	// ------------------------------------------------------------------------
	//  Methods for tests
	// ------------------------------------------------------------------------

	@VisibleForTesting
	protected AllocatedSlots getAllocatedSlots() {
		return allocatedSlots;
	}

	@VisibleForTesting
	protected AvailableSlots getAvailableSlots() {
		return availableSlots;
	}

	@VisibleForTesting
	DualKeyMap<SlotRequestId, AllocationID, PendingRequest> getPendingRequests() {
		return pendingRequests;
	}

	@VisibleForTesting
	Map<SlotRequestId, PendingRequest> getWaitingForResourceManager() {
		return waitingForResourceManager;
	}

	// ------------------------------------------------------------------------
	//  Helper classes
	// ------------------------------------------------------------------------

	/**
	 * Organize allocated slots from different points of view.
	 */
	static class AllocatedSlots {

		/** All allocated slots organized by TaskManager's id. */
		private final Map<ResourceID, Set<AllocatedSlot>> allocatedSlotsByTaskManager;

		/** All allocated slots organized by AllocationID. */
		private final DualKeyMap<AllocationID, SlotRequestId, AllocatedSlot> allocatedSlotsById;

		AllocatedSlots() {
			this.allocatedSlotsByTaskManager = new HashMap<>(16);
			this.allocatedSlotsById = new DualKeyMap<>(16);
		}

		/**
		 * Adds a new slot to this collection.
		 *
		 * @param allocatedSlot The allocated slot
		 */
		void add(SlotRequestId slotRequestId, AllocatedSlot allocatedSlot) {
			allocatedSlotsById.put(allocatedSlot.getAllocationId(), slotRequestId, allocatedSlot);

			final ResourceID resourceID = allocatedSlot.getTaskManagerLocation().getResourceID();

			Set<AllocatedSlot> slotsForTaskManager = allocatedSlotsByTaskManager.computeIfAbsent(
				resourceID,
				resourceId -> new HashSet<>(4));

			slotsForTaskManager.add(allocatedSlot);
		}

		/**
		 * Get allocated slot with allocation id.
		 *
		 * @param allocationID The allocation id
		 * @return The allocated slot, null if we can't find a match
		 */
		AllocatedSlot get(final AllocationID allocationID) {
			return allocatedSlotsById.getKeyA(allocationID);
		}

		AllocatedSlot get(final SlotRequestId slotRequestId) {
			return allocatedSlotsById.getKeyB(slotRequestId);
		}

		/**
		 * Check whether we have allocated this slot.
		 *
		 * @param slotAllocationId The allocation id of the slot to check
		 * @return True if we contains this slot
		 */
		boolean contains(AllocationID slotAllocationId) {
			return allocatedSlotsById.containsKeyA(slotAllocationId);
		}

		/**
		 * Removes the allocated slot specified by the provided slot allocation id.
		 *
		 * @param allocationID identifying the allocated slot to remove
		 * @return The removed allocated slot or null.
		 */
		@Nullable
		AllocatedSlot remove(final AllocationID allocationID) {
			AllocatedSlot allocatedSlot = allocatedSlotsById.removeKeyA(allocationID);

			if (allocatedSlot != null) {
				removeAllocatedSlot(allocatedSlot);
			}

			return allocatedSlot;
		}

		/**
		 * Removes the allocated slot specified by the provided slot request id.
		 *
		 * @param slotRequestId identifying the allocated slot to remove
		 * @return The removed allocated slot or null.
		 */
		@Nullable
		AllocatedSlot remove(final SlotRequestId slotRequestId) {
			final AllocatedSlot allocatedSlot = allocatedSlotsById.removeKeyB(slotRequestId);

			if (allocatedSlot != null) {
				removeAllocatedSlot(allocatedSlot);
			}

			return allocatedSlot;
		}

		private void removeAllocatedSlot(final AllocatedSlot allocatedSlot) {
			Preconditions.checkNotNull(allocatedSlot);
			final ResourceID taskManagerId = allocatedSlot.getTaskManagerLocation().getResourceID();
			Set<AllocatedSlot> slotsForTM = allocatedSlotsByTaskManager.get(taskManagerId);

			slotsForTM.remove(allocatedSlot);

			if (slotsForTM.isEmpty()) {
				allocatedSlotsByTaskManager.remove(taskManagerId);
			}
		}

		/**
		 * Get all allocated slot from same TaskManager.
		 *
		 * @param resourceID The id of the TaskManager
		 * @return Set of slots which are allocated from the same TaskManager
		 */
		Set<AllocatedSlot> removeSlotsForTaskManager(final ResourceID resourceID) {
			Set<AllocatedSlot> slotsForTaskManager = allocatedSlotsByTaskManager.remove(resourceID);
			if (slotsForTaskManager != null) {
				for (AllocatedSlot allocatedSlot : slotsForTaskManager) {
					allocatedSlotsById.removeKeyA(allocatedSlot.getAllocationId());
				}
				return slotsForTaskManager;
			}
			else {
				return Collections.emptySet();
			}
		}

		void clear() {
			allocatedSlotsById.clear();
			allocatedSlotsByTaskManager.clear();
		}

		@VisibleForTesting
		boolean containResource(final ResourceID resourceID) {
			return allocatedSlotsByTaskManager.containsKey(resourceID);
		}

		@VisibleForTesting
		int size() {
			return allocatedSlotsById.size();
		}

		@VisibleForTesting
		Set<AllocatedSlot> getSlotsForTaskManager(ResourceID resourceId) {
			if (allocatedSlotsByTaskManager.containsKey(resourceId)) {
				return allocatedSlotsByTaskManager.get(resourceId);
			} else {
				return Collections.emptySet();
			}
		}
	}

	// ------------------------------------------------------------------------

	/**
	 * Organize all available slots from different points of view.
	 */
	protected static class AvailableSlots {

		/** All available slots organized by TaskManager. */
		private final HashMap<ResourceID, Set<AllocatedSlot>> availableSlotsByTaskManager;

		/** All available slots organized by host. */
		private final HashMap<String, Set<AllocatedSlot>> availableSlotsByHost;

		/** The available slots, with the time when they were inserted. */
		private final HashMap<AllocationID, SlotAndTimestamp> availableSlots;

		AvailableSlots() {
			this.availableSlotsByTaskManager = new HashMap<>();
			this.availableSlotsByHost = new HashMap<>();
			this.availableSlots = new HashMap<>();
		}

		/**
		 * Adds an available slot.
		 *
		 * @param slot The slot to add
		 */
		void add(final AllocatedSlot slot, final long timestamp) {
			checkNotNull(slot);

			SlotAndTimestamp previous = availableSlots.put(
					slot.getAllocationId(), new SlotAndTimestamp(slot, timestamp));

			if (previous == null) {
				final ResourceID resourceID = slot.getTaskManagerLocation().getResourceID();
				final String host = slot.getTaskManagerLocation().getFQDNHostname();

				Set<AllocatedSlot> slotsForTaskManager = availableSlotsByTaskManager.get(resourceID);
				if (slotsForTaskManager == null) {
					slotsForTaskManager = new HashSet<>();
					availableSlotsByTaskManager.put(resourceID, slotsForTaskManager);
				}
				slotsForTaskManager.add(slot);

				Set<AllocatedSlot> slotsForHost = availableSlotsByHost.get(host);
				if (slotsForHost == null) {
					slotsForHost = new HashSet<>();
					availableSlotsByHost.put(host, slotsForHost);
				}
				slotsForHost.add(slot);
			}
			else {
				throw new IllegalStateException("slot already contained");
			}
		}

		/**
		 * Check whether we have this slot.
		 */
		boolean contains(AllocationID slotId) {
			return availableSlots.containsKey(slotId);
		}

		/**
		 * Poll a slot which matches the required resource profile. The polling tries to satisfy the
		 * location preferences, by TaskManager and by host.
		 *
		 * @param resourceProfile      The required resource profile.
		 * @param locationPreferences  The location preferences, in order to be checked.
		 *
		 * @return Slot which matches the resource profile, null if we can't find a match
		 */
		SlotAndLocality poll(ResourceProfile resourceProfile, Collection<TaskManagerLocation> locationPreferences) {
			// fast path if no slots are available
			if (availableSlots.isEmpty()) {
				return null;
			}

			boolean hadLocationPreference = false;

			if (locationPreferences != null && !locationPreferences.isEmpty()) {

				// first search by TaskManager
				for (TaskManagerLocation location : locationPreferences) {
					hadLocationPreference = true;

					final Set<AllocatedSlot> onTaskManager = availableSlotsByTaskManager.get(location.getResourceID());
					if (onTaskManager != null) {
						for (AllocatedSlot candidate : onTaskManager) {
							if (candidate.getResourceProfile().isMatching(resourceProfile)) {
								remove(candidate.getAllocationId());
								return new SlotAndLocality(candidate, Locality.LOCAL);
							}
						}
					}
				}

				// now, search by host
				for (TaskManagerLocation location : locationPreferences) {
					final Set<AllocatedSlot> onHost = availableSlotsByHost.get(location.getFQDNHostname());
					if (onHost != null) {
						for (AllocatedSlot candidate : onHost) {
							if (candidate.getResourceProfile().isMatching(resourceProfile)) {
								remove(candidate.getAllocationId());
								return new SlotAndLocality(candidate, Locality.HOST_LOCAL);
							}
						}
					}
				}
			}

			// take any slot
			for (SlotAndTimestamp candidate : availableSlots.values()) {
				final AllocatedSlot slot = candidate.slot();

				if (slot.getResourceProfile().isMatching(resourceProfile)) {
					remove(slot.getAllocationId());
					return new SlotAndLocality(
							slot, hadLocationPreference ? Locality.NON_LOCAL : Locality.UNCONSTRAINED);
				}
			}

			// nothing available that matches
			return null;
		}

		/**
		 * Remove all available slots come from specified TaskManager.
		 *
		 * @param taskManager The id of the TaskManager
		 */
		void removeAllForTaskManager(final ResourceID taskManager) {
			// remove from the by-TaskManager view
			final Set<AllocatedSlot> slotsForTm = availableSlotsByTaskManager.remove(taskManager);

			if (slotsForTm != null && slotsForTm.size() > 0) {
				final String host = slotsForTm.iterator().next().getTaskManagerLocation().getFQDNHostname();
				final Set<AllocatedSlot> slotsForHost = availableSlotsByHost.get(host);

				// remove from the base set and the by-host view
				for (AllocatedSlot slot : slotsForTm) {
					availableSlots.remove(slot.getAllocationId());
					slotsForHost.remove(slot);
				}

				if (slotsForHost.isEmpty()) {
					availableSlotsByHost.remove(host);
				}
			}
		}

		boolean tryRemove(AllocationID slotId) {
			final SlotAndTimestamp sat = availableSlots.remove(slotId);
			if (sat != null) {
				final AllocatedSlot slot = sat.slot();
				final ResourceID resourceID = slot.getTaskManagerLocation().getResourceID();
				final String host = slot.getTaskManagerLocation().getFQDNHostname();

				final Set<AllocatedSlot> slotsForTm = availableSlotsByTaskManager.get(resourceID);
				final Set<AllocatedSlot> slotsForHost = availableSlotsByHost.get(host);

				slotsForTm.remove(slot);
				slotsForHost.remove(slot);

				if (slotsForTm.isEmpty()) {
					availableSlotsByTaskManager.remove(resourceID);
				}
				if (slotsForHost.isEmpty()) {
					availableSlotsByHost.remove(host);
				}

				return true;
			}
			else {
				return false;
			}
		}

		private void remove(AllocationID slotId) throws IllegalStateException {
			if (!tryRemove(slotId)) {
				throw new IllegalStateException("slot not contained");
			}
		}

		@VisibleForTesting
		boolean containsTaskManager(ResourceID resourceID) {
			return availableSlotsByTaskManager.containsKey(resourceID);
		}

		@VisibleForTesting
		public int size() {
			return availableSlots.size();
		}

		@VisibleForTesting
		void clear() {
			availableSlots.clear();
			availableSlotsByTaskManager.clear();
			availableSlotsByHost.clear();
		}
	}

	// ------------------------------------------------------------------------

	/**
	 * An implementation of the {@link SlotOwner} and {@link SlotProvider} interfaces
	 * that delegates methods as RPC calls to the SlotPool's RPC gateway.
	 */
	private static class ProviderAndOwner implements SlotOwner, SlotProvider {

		private final SlotPoolGateway gateway;

		private final Time timeout;

		ProviderAndOwner(SlotPoolGateway gateway, Time timeout) {
			this.gateway = gateway;
			this.timeout = timeout;
		}

		@Override
		public CompletableFuture<Boolean> returnAllocatedSlot(LogicalSlot slot) {
			return gateway
				.releaseSlot(
					slot.getSlotRequestId(),
					slot.getSlotSharingGroupId(),
					new FlinkException("Slot is being returned to the SlotPool."))
				.thenApply(
					(Acknowledge acknowledge) -> true);
		}

		@Override
		public CompletableFuture<LogicalSlot> allocateSlot(
				ScheduledUnit task,
				boolean allowQueued,
				Collection<TaskManagerLocation> preferredLocations) {

			final SlotRequestId requestId = new SlotRequestId();
			CompletableFuture<LogicalSlot> slotFuture = gateway.allocateSlot(
				requestId,
				task,
				ResourceProfile.UNKNOWN,
				preferredLocations,
				allowQueued,
				timeout);

			slotFuture.whenComplete(
				(LogicalSlot slot, Throwable failure) -> {
					if (failure != null) {
						gateway.releaseSlot(
							requestId,
							task.getSlotSharingGroupId(),
							failure);
					}
			});

			return slotFuture;
		}
	}

	// ------------------------------------------------------------------------

	/**
	 * A pending request for a slot.
	 */
	private static class PendingRequest {

		private final SlotRequestId slotRequestId;

		private final ResourceProfile resourceProfile;

		private final CompletableFuture<AllocatedSlot> allocatedSlotFuture;

		PendingRequest(
				SlotRequestId slotRequestId,
				ResourceProfile resourceProfile) {
			this.slotRequestId = Preconditions.checkNotNull(slotRequestId);
			this.resourceProfile = Preconditions.checkNotNull(resourceProfile);

			allocatedSlotFuture = new CompletableFuture<>();
		}

		public SlotRequestId getSlotRequestId() {
			return slotRequestId;
		}

		public CompletableFuture<AllocatedSlot> getAllocatedSlotFuture() {
			return allocatedSlotFuture;
		}

		public ResourceProfile getResourceProfile() {
			return resourceProfile;
		}
	}

	// ------------------------------------------------------------------------

	/**
	 * A slot, together with the timestamp when it was added.
	 */
	private static class SlotAndTimestamp {

		private final AllocatedSlot slot;

		private final long timestamp;

		SlotAndTimestamp(AllocatedSlot slot, long timestamp) {
			this.slot = slot;
			this.timestamp = timestamp;
		}

		public AllocatedSlot slot() {
			return slot;
		}

		public long timestamp() {
			return timestamp;
		}

		@Override
		public String toString() {
			return slot + " @ " + timestamp;
		}
	}
}
