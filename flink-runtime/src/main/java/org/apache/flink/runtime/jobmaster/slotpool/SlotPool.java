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
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationConstraint;
import org.apache.flink.runtime.jobmanager.scheduler.Locality;
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;
import org.apache.flink.runtime.jobmanager.scheduler.ScheduledUnit;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.AllocatedSlotInfo;
import org.apache.flink.runtime.jobmaster.AllocatedSlotReport;
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
import org.apache.flink.types.SerializableOptional;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
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
 * <p>TODO : Make pending requests location preference aware
 * TODO : Make pass location preferences to ResourceManager when sending a slot request
 */
public class SlotPool extends RpcEndpoint implements SlotPoolGateway, AllocatedSlotActions {

	/** The interval (in milliseconds) in which the SlotPool writes its slot distribution on debug level. */
	private static final int STATUS_LOG_INTERVAL_MS = 60_000;

	private final JobID jobId;

	private final SchedulingStrategy schedulingStrategy;

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

	/** Timeout for external request calls (e.g. to the ResourceManager or the TaskExecutor). */
	private final Time rpcTimeout;

	/** Timeout for releasing idle slots. */
	private final Time idleSlotTimeout;

	private final Clock clock;

	/** Managers for the different slot sharing groups. */
	protected final Map<SlotSharingGroupId, SlotSharingManager> slotSharingManagers;

	/** the fencing token of the job manager. */
	private JobMasterId jobMasterId;

	/** The gateway to communicate with resource manager. */
	private ResourceManagerGateway resourceManagerGateway;

	private String jobManagerAddress;

	// ------------------------------------------------------------------------

	@VisibleForTesting
	protected SlotPool(RpcService rpcService, JobID jobId, SchedulingStrategy schedulingStrategy) {
		this(
			rpcService,
			jobId,
			schedulingStrategy,
			SystemClock.getInstance(),
			AkkaUtils.getDefaultTimeout(),
			Time.milliseconds(JobManagerOptions.SLOT_IDLE_TIMEOUT.defaultValue()));
	}

	public SlotPool(
			RpcService rpcService,
			JobID jobId,
			SchedulingStrategy schedulingStrategy,
			Clock clock,
			Time rpcTimeout,
			Time idleSlotTimeout) {

		super(rpcService);

		this.jobId = checkNotNull(jobId);
		this.schedulingStrategy = checkNotNull(schedulingStrategy);
		this.clock = checkNotNull(clock);
		this.rpcTimeout = checkNotNull(rpcTimeout);
		this.idleSlotTimeout = checkNotNull(idleSlotTimeout);

		this.registeredTaskManagers = new HashSet<>(16);
		this.allocatedSlots = new AllocatedSlots();
		this.availableSlots = new AvailableSlots();
		this.pendingRequests = new DualKeyMap<>(16);
		this.waitingForResourceManager = new HashMap<>(16);

		this.providerAndOwner = new ProviderAndOwner(
			getSelfGateway(SlotPoolGateway.class),
			schedulingStrategy instanceof PreviousAllocationSchedulingStrategy);

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

		scheduleRunAsync(this::checkIdleSlot, idleSlotTimeout);

		if (log.isDebugEnabled()) {
			scheduleRunAsync(this::scheduledLogStatus, STATUS_LOG_INTERVAL_MS, TimeUnit.MILLISECONDS);
		}
	}

	@Override
	public CompletableFuture<Void> postStop() {
		log.info("Stopping SlotPool.");
		// cancel all pending allocations
		Set<AllocationID> allocationIds = pendingRequests.keySetB();

		for (AllocationID allocationId : allocationIds) {
			resourceManagerGateway.cancelSlotRequest(allocationId);
		}

		// release all registered slots by releasing the corresponding TaskExecutors
		for (ResourceID taskManagerResourceId : registeredTaskManagers) {
			final FlinkException cause = new FlinkException(
				"Releasing TaskManager " + taskManagerResourceId + ", because of stopping of SlotPool");
			releaseTaskManagerInternal(taskManagerResourceId, cause);
		}

		clear();

		return CompletableFuture.completedFuture(null);
	}

	/**
	 * Suspends this pool, meaning it has lost its authority to accept and distribute slots.
	 */
	@Override
	public void suspend() {
		log.info("Suspending SlotPool.");

		validateRunsInMainThread();

		// cancel all pending allocations --> we can request these slots
		// again after we regained the leadership
		Set<AllocationID> allocationIds = pendingRequests.keySetB();

		for (AllocationID allocationId : allocationIds) {
			resourceManagerGateway.cancelSlotRequest(allocationId);
		}

		// suspend this RPC endpoint
		stop();

		// do not accept any requests
		jobMasterId = null;
		resourceManagerGateway = null;

		// Clear (but not release!) the available slots. The TaskManagers should re-register them
		// at the new leader JobManager/SlotPool
		clear();
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
			ScheduledUnit task,
			SlotProfile slotProfile,
			boolean allowQueuedScheduling,
			Time allocationTimeout) {

		log.debug("Received slot request [{}] for task: {}", slotRequestId, task.getTaskToExecute());

		if (task.getSlotSharingGroupId() == null) {
			return allocateSingleSlot(slotRequestId, slotProfile, allowQueuedScheduling, allocationTimeout);
		} else {
			return allocateSharedSlot(slotRequestId, task, slotProfile, allowQueuedScheduling, allocationTimeout);
		}
	}

	private CompletableFuture<LogicalSlot> allocateSingleSlot(
		SlotRequestId slotRequestId,
		SlotProfile slotProfile,
		boolean allowQueuedScheduling,
		Time allocationTimeout) {
		// request an allocated slot to assign a single logical slot to
		CompletableFuture<SlotAndLocality> slotAndLocalityFuture = requestAllocatedSlot(
			slotRequestId,
			slotProfile,
			allowQueuedScheduling,
			allocationTimeout);

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
					final FlinkException flinkException =
						new FlinkException("Could not assign payload to allocated slot " + allocatedSlot.getAllocationId() + '.');
					releaseSingleSlot(slotRequestId, flinkException);
					throw new CompletionException(flinkException);
				}
			});
	}

	private CompletableFuture<LogicalSlot> allocateSharedSlot(
		SlotRequestId slotRequestId,
		ScheduledUnit task,
		SlotProfile slotProfile,
		boolean allowQueuedScheduling,
		Time allocationTimeout) {

		// allocate slot with slot sharing
		final SlotSharingManager multiTaskSlotManager = slotSharingManagers.computeIfAbsent(
			task.getSlotSharingGroupId(),
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
					slotProfile,
					allowQueuedScheduling,
					allocationTimeout);
			} else {
				multiTaskSlotLocality = allocateMultiTaskSlot(
					task.getJobVertexId(),
					multiTaskSlotManager,
					slotProfile,
					allowQueuedScheduling,
					allocationTimeout);
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
	}

	/**
	 * Allocates a co-located {@link SlotSharingManager.MultiTaskSlot} for the given {@link CoLocationConstraint}.
	 *
	 * <p>If allowQueuedScheduling is true, then the returned {@link SlotSharingManager.MultiTaskSlot} can be
	 * uncompleted.
	 *
	 * @param coLocationConstraint for which to allocate a {@link SlotSharingManager.MultiTaskSlot}
	 * @param multiTaskSlotManager responsible for the slot sharing group for which to allocate the slot
	 * @param slotProfile specifying the requirements for the requested slot
	 * @param allowQueuedScheduling true if queued scheduling (the returned task slot must not be completed yet) is allowed, otherwise false
	 * @param allocationTimeout timeout before the slot allocation times out
	 * @return A {@link SlotSharingManager.MultiTaskSlotLocality} which contains the allocated{@link SlotSharingManager.MultiTaskSlot}
	 * 		and its locality wrt the given location preferences
	 * @throws NoResourceAvailableException if no task slot could be allocated
	 */
	private SlotSharingManager.MultiTaskSlotLocality allocateCoLocatedMultiTaskSlot(
			CoLocationConstraint coLocationConstraint,
			SlotSharingManager multiTaskSlotManager,
			SlotProfile slotProfile,
			boolean allowQueuedScheduling,
			Time allocationTimeout) throws NoResourceAvailableException {
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

		if (coLocationConstraint.isAssigned()) {
			// refine the preferred locations of the slot profile
			slotProfile = new SlotProfile(
				slotProfile.getResourceProfile(),
				Collections.singleton(coLocationConstraint.getLocation()),
				slotProfile.getPreferredAllocations());
		}

		// get a new multi task slot
		final SlotSharingManager.MultiTaskSlotLocality multiTaskSlotLocality = allocateMultiTaskSlot(
			coLocationConstraint.getGroupId(),
			multiTaskSlotManager,
			slotProfile,
			allowQueuedScheduling,
			allocationTimeout);

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
	 * @param slotProfile slot profile that specifies the requirements for the slot
	 * @param allowQueuedScheduling true if queued scheduling (the returned task slot must not be completed yet) is allowed, otherwise false
	 * @param allocationTimeout timeout before the slot allocation times out
	 * @return A {@link SlotSharingManager.MultiTaskSlotLocality} which contains the allocated {@link SlotSharingManager.MultiTaskSlot}
	 * 		and its locality wrt the given location preferences
	 * @throws NoResourceAvailableException if no task slot could be allocated
	 */
	private SlotSharingManager.MultiTaskSlotLocality allocateMultiTaskSlot(
			AbstractID groupId,
			SlotSharingManager slotSharingManager,
			SlotProfile slotProfile,
			boolean allowQueuedScheduling,
			Time allocationTimeout) throws NoResourceAvailableException {

		// check first whether we have a resolved root slot which we can use
		SlotSharingManager.MultiTaskSlotLocality multiTaskSlotLocality = slotSharingManager.getResolvedRootSlot(
			groupId,
			schedulingStrategy,
			slotProfile);

		if (multiTaskSlotLocality != null && multiTaskSlotLocality.getLocality() == Locality.LOCAL) {
			return multiTaskSlotLocality;
		}

		final SlotRequestId allocatedSlotRequestId = new SlotRequestId();
		final SlotRequestId multiTaskSlotRequestId = new SlotRequestId();

		// check whether we have an allocated slot available which we can use to create a new multi task slot in
		final SlotAndLocality polledSlotAndLocality = pollAndAllocateSlot(allocatedSlotRequestId, slotProfile);

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
				releaseSingleSlot(
					allocatedSlotRequestId,
					new FlinkException("Locality constraint is not better fulfilled by allocated slot."));
			}
			return multiTaskSlotLocality;
		}

		if (allowQueuedScheduling) {
			// there is no slot immediately available --> check first for uncompleted slots at the slot sharing group
			SlotSharingManager.MultiTaskSlot multiTaskSlotFuture = slotSharingManager.getUnresolvedRootSlot(groupId);

			if (multiTaskSlotFuture == null) {
				// it seems as if we have to request a new slot from the resource manager, this is always the last resort!!!
				final CompletableFuture<AllocatedSlot> futureSlot = requestNewAllocatedSlot(
					allocatedSlotRequestId,
					slotProfile.getResourceProfile(),
					allocationTimeout);

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
							releaseSingleSlot(
								allocatedSlotRequestId,
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
	 * @param slotProfile slot profile that specifies the requirements for the slot
	 * @param allowQueuedScheduling true if the slot allocation can be completed in the future
	 * @param allocationTimeout timeout before the slot allocation times out
	 * @return Future containing the allocated simple slot
	 */
	private CompletableFuture<SlotAndLocality> requestAllocatedSlot(
			SlotRequestId slotRequestId,
			SlotProfile slotProfile,
			boolean allowQueuedScheduling,
			Time allocationTimeout) {

		final CompletableFuture<SlotAndLocality> allocatedSlotLocalityFuture;

		// (1) do we have a slot available already?
		SlotAndLocality slotFromPool = pollAndAllocateSlot(slotRequestId, slotProfile);

		if (slotFromPool != null) {
			allocatedSlotLocalityFuture = CompletableFuture.completedFuture(slotFromPool);
		} else if (allowQueuedScheduling) {
			// we have to request a new allocated slot
			CompletableFuture<AllocatedSlot> allocatedSlotFuture = requestNewAllocatedSlot(
				slotRequestId,
				slotProfile.getResourceProfile(),
				allocationTimeout);

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
	 * @param allocationTimeout timeout before the slot allocation times out
	 * @return An {@link AllocatedSlot} future which is completed once the slot is offered to the {@link SlotPool}
	 */
	private CompletableFuture<AllocatedSlot> requestNewAllocatedSlot(
			SlotRequestId slotRequestId,
			ResourceProfile resourceProfile,
			Time allocationTimeout) {

		final PendingRequest pendingRequest = new PendingRequest(
			slotRequestId,
			resourceProfile);

		// register request timeout
		FutureUtils
			.orTimeout(pendingRequest.getAllocatedSlotFuture(), allocationTimeout.toMilliseconds(), TimeUnit.MILLISECONDS)
			.whenCompleteAsync(
				(AllocatedSlot ignored, Throwable throwable) -> {
					if (throwable instanceof TimeoutException) {
						timeoutPendingSlotRequest(slotRequestId);
					}
				},
				getMainThreadExecutor());

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

		checkNotNull(resourceManagerGateway);
		checkNotNull(pendingRequest);

		log.info("Requesting new slot [{}] and profile {} from resource manager.", pendingRequest.getSlotRequestId(), pendingRequest.getResourceProfile());

		final AllocationID allocationId = new AllocationID();

		pendingRequests.put(pendingRequest.getSlotRequestId(), allocationId, pendingRequest);

		pendingRequest.getAllocatedSlotFuture().whenComplete(
			(AllocatedSlot allocatedSlot, Throwable throwable) -> {
				if (throwable != null || !allocationId.equals(allocatedSlot.getAllocationId())) {
					// cancel the slot request if there is a failure or if the pending request has
					// been completed with another allocated slot
					resourceManagerGateway.cancelSlotRequest(allocationId);
				}
			});

		CompletableFuture<Acknowledge> rmResponse = resourceManagerGateway.requestSlot(
			jobMasterId,
			new SlotRequest(jobId, allocationId, pendingRequest.getResourceProfile(), jobManagerAddress),
			rpcTimeout);

		// on failure, fail the request future
		rmResponse.whenCompleteAsync(
			(Acknowledge ignored, Throwable failure) -> {
				if (failure != null) {
					slotRequestToResourceManagerFailed(pendingRequest.getSlotRequestId(), failure);
				}
			},
			getMainThreadExecutor());
	}

	private void slotRequestToResourceManagerFailed(SlotRequestId slotRequestID, Throwable failure) {
		PendingRequest request = pendingRequests.removeKeyA(slotRequestID);
		if (request != null) {
			request.getAllocatedSlotFuture().completeExceptionally(new NoResourceAvailableException(
					"No pooled slot available and request to ResourceManager for new slot failed", failure));
		} else {
			if (log.isDebugEnabled()) {
				log.debug("Unregistered slot request [{}] failed.", slotRequestID, failure);
			}
		}
	}

	private void stashRequestWaitingForResourceManager(final PendingRequest pendingRequest) {

		log.info("Cannot serve slot request, no ResourceManager connected. " +
				"Adding as pending request [{}]",  pendingRequest.getSlotRequestId());

		waitingForResourceManager.put(pendingRequest.getSlotRequestId(), pendingRequest);
	}

	// ------------------------------------------------------------------------
	//  Slot releasing & offering
	// ------------------------------------------------------------------------

	@Override
	public CompletableFuture<Acknowledge> releaseSlot(
		SlotRequestId slotRequestId,
		@Nullable SlotSharingGroupId slotSharingGroupId,
		Throwable cause) {

		log.debug("Releasing slot [{}] because: {}", slotRequestId, cause != null ? cause.getMessage() : "null");

		if (slotSharingGroupId != null) {
			releaseSharedSlot(slotRequestId, slotSharingGroupId, cause);
		} else {
			releaseSingleSlot(slotRequestId, cause);
		}

		return CompletableFuture.completedFuture(Acknowledge.get());
	}

	private void releaseSharedSlot(
		SlotRequestId slotRequestId,
		@Nonnull SlotSharingGroupId slotSharingGroupId, Throwable cause) {

		final SlotSharingManager multiTaskSlotManager = slotSharingManagers.get(slotSharingGroupId);

		if (multiTaskSlotManager != null) {
			final SlotSharingManager.TaskSlot taskSlot = multiTaskSlotManager.getTaskSlot(slotRequestId);

			if (taskSlot != null) {
				taskSlot.release(cause);
			} else {
				log.debug("Could not find slot [{}] in slot sharing group {}. Ignoring release slot request.", slotRequestId, slotSharingGroupId);
			}
		} else {
			log.debug("Could not find slot sharing group {}. Ignoring release slot request.", slotSharingGroupId);
		}
	}

	private void releaseSingleSlot(SlotRequestId slotRequestId, Throwable cause) {
		final PendingRequest pendingRequest = removePendingRequest(slotRequestId);

		if (pendingRequest != null) {
			failPendingRequest(pendingRequest, new FlinkException("Pending slot request with " + slotRequestId + " has been released."));
		} else {
			final AllocatedSlot allocatedSlot = allocatedSlots.remove(slotRequestId);

			if (allocatedSlot != null) {
				allocatedSlot.releasePayload(cause);
				tryFulfillSlotRequestOrMakeAvailable(allocatedSlot);
			} else {
				log.debug("There is no allocated slot [{}]. Ignoring the release slot request.", slotRequestId);
			}
		}
	}

	/**
	 * Checks whether there exists a pending request with the given slot request id and removes it
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
		checkNotNull(pendingRequest);
		checkNotNull(e);

		if (!pendingRequest.getAllocatedSlotFuture().isDone()) {
			log.info("Failing pending slot request [{}]: {}", pendingRequest.getSlotRequestId(), e.getMessage());
			pendingRequest.getAllocatedSlotFuture().completeExceptionally(e);
		}
	}

	@Nullable
	private SlotAndLocality pollAndAllocateSlot(SlotRequestId slotRequestId, SlotProfile slotProfile) {
		SlotAndLocality slotFromPool = availableSlots.poll(schedulingStrategy, slotProfile);

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
			log.debug("Fulfilling pending slot request [{}] early with returned slot [{}]",
				pendingRequest.getSlotRequestId(), allocatedSlot.getAllocationId());

			allocatedSlots.add(pendingRequest.getSlotRequestId(), allocatedSlot);
			pendingRequest.getAllocatedSlotFuture().complete(allocatedSlot);
		} else {
			log.debug("Adding returned slot [{}] to available slots", allocatedSlot.getAllocationId());
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
			offer -> offerSlot(
					taskManagerLocation,
					taskManagerGateway,
					offer)
				.<Optional<SlotOffer>>thenApply(
					(acceptedSlot) -> acceptedSlot ? Optional.of(offer) : Optional.empty()
				)
		).collect(Collectors.toList());

		CompletableFuture<Collection<Optional<SlotOffer>>> optionalSlotOffers = FutureUtils.combineAll(acceptedSlotOffers);

		return optionalSlotOffers.thenApply(collection -> collection.stream()
				.flatMap(opt -> opt.map(Stream::of).orElseGet(Stream::empty))
				.collect(Collectors.toList()));
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
			log.debug("Received outdated slot offering [{}] from unregistered TaskManager: {}",
					slotOffer.getAllocationId(), taskManagerLocation);
			return CompletableFuture.completedFuture(false);
		}

		// check whether we have already using this slot
		AllocatedSlot existingSlot;
		if ((existingSlot = allocatedSlots.get(allocationID)) != null ||
			(existingSlot = availableSlots.get(allocationID)) != null) {

			// we need to figure out if this is a repeated offer for the exact same slot,
			// or another offer that comes from a different TaskManager after the ResourceManager
			// re-tried the request

			// we write this in terms of comparing slot IDs, because the Slot IDs are the identifiers of
			// the actual slots on the TaskManagers
			// Note: The slotOffer should have the SlotID
			final SlotID existingSlotId = existingSlot.getSlotId();
			final SlotID newSlotId = new SlotID(taskManagerLocation.getResourceID(), slotOffer.getSlotIndex());

			if (existingSlotId.equals(newSlotId)) {
				log.info("Received repeated offer for slot [{}]. Ignoring.", allocationID);

				// return true here so that the sender will get a positive acknowledgement to the retry
				// and mark the offering as a success
				return CompletableFuture.completedFuture(true);
			} else {
				// the allocation has been fulfilled by another slot, reject the offer so the task executor
				// will offer the slot to the resource manager
				return CompletableFuture.completedFuture(false);
			}
		}

		final AllocatedSlot allocatedSlot = new AllocatedSlot(
			allocationID,
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
			} else {
				log.debug("Fulfilled slot request [{}] with allocated slot [{}].", pendingRequest.getSlotRequestId(), allocationID);
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
	 * This may triggered by JobManager when some slot allocation failed with rpcTimeout.
	 * Or this could be triggered by TaskManager, when it finds out something went wrong with the slot,
	 * and decided to take it back.
	 *
	 * @param allocationID Represents the allocation which should be failed
	 * @param cause The cause of the failure
	 * @return Optional task executor if it has no more slots registered
	 */
	@Override
	public CompletableFuture<SerializableOptional<ResourceID>> failAllocation(final AllocationID allocationID, final Exception cause) {
		final PendingRequest pendingRequest = pendingRequests.removeKeyB(allocationID);
		if (pendingRequest != null) {
			// request was still pending
			failPendingRequest(pendingRequest, cause);
			return CompletableFuture.completedFuture(SerializableOptional.empty());
		}
		else {
			return tryFailingAllocatedSlot(allocationID, cause);
		}

		// TODO: add some unit tests when the previous two are ready, the allocation may failed at any phase
	}

	private CompletableFuture<SerializableOptional<ResourceID>> tryFailingAllocatedSlot(AllocationID allocationID, Exception cause) {
		AllocatedSlot allocatedSlot = availableSlots.tryRemove(allocationID);

		if (allocatedSlot == null) {
			allocatedSlot = allocatedSlots.remove(allocationID);
		}

		if (allocatedSlot != null) {
			log.debug("Failed allocated slot [{}]: {}", allocationID, cause.getMessage());

			// notify TaskExecutor about the failure
			allocatedSlot.getTaskManagerGateway().freeSlot(allocationID, cause, rpcTimeout);
			// release the slot.
			// since it is not in 'allocatedSlots' any more, it will be dropped o return'
			allocatedSlot.releasePayload(cause);

			final ResourceID taskManagerId = allocatedSlot.getTaskManagerId();

			if (!availableSlots.containsTaskManager(taskManagerId) && !allocatedSlots.containResource(taskManagerId)) {
				return CompletableFuture.completedFuture(SerializableOptional.of(taskManagerId));
			}
		}

		return CompletableFuture.completedFuture(SerializableOptional.empty());
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
		log.debug("Register new TaskExecutor {}.", resourceID);
		registeredTaskManagers.add(resourceID);

		return CompletableFuture.completedFuture(Acknowledge.get());
	}

	/**
	 * Unregister TaskManager from this pool, all the related slots will be released and tasks be canceled. Called
	 * when we find some TaskManager becomes "dead" or "abnormal", and we decide to not using slots from it anymore.
	 *
	 * @param resourceId The id of the TaskManager
	 * @param cause for the releasing of the TaskManager
	 */
	@Override
	public CompletableFuture<Acknowledge> releaseTaskManager(final ResourceID resourceId, final Exception cause) {
		if (registeredTaskManagers.remove(resourceId)) {
			releaseTaskManagerInternal(resourceId, cause);
		}

		return CompletableFuture.completedFuture(Acknowledge.get());
	}

	@Override
	public CompletableFuture<AllocatedSlotReport> createAllocatedSlotReport(ResourceID taskManagerId) {
		final Set<AllocatedSlot> availableSlotsForTaskManager = availableSlots.getSlotsForTaskManager(taskManagerId);
		final Set<AllocatedSlot> allocatedSlotsForTaskManager = allocatedSlots.getSlotsForTaskManager(taskManagerId);

		List<AllocatedSlotInfo> allocatedSlotInfos = new ArrayList<>(
			availableSlotsForTaskManager.size() + allocatedSlotsForTaskManager.size());
		for (AllocatedSlot allocatedSlot : Iterables.concat(availableSlotsForTaskManager, allocatedSlotsForTaskManager)) {
			allocatedSlotInfos.add(
				new AllocatedSlotInfo(allocatedSlot.getPhysicalSlotNumber(), allocatedSlot.getAllocationId()));
		}
		return CompletableFuture.completedFuture(new AllocatedSlotReport(jobId, allocatedSlotInfos));
	}

	// ------------------------------------------------------------------------
	//  Internal methods
	// ------------------------------------------------------------------------

	@VisibleForTesting
	protected void timeoutPendingSlotRequest(SlotRequestId slotRequestId) {
		log.info("Pending slot request [{}] timed out.", slotRequestId);
		removePendingRequest(slotRequestId);
	}

	private void releaseTaskManagerInternal(final ResourceID resourceId, final Exception cause) {
		final Set<AllocatedSlot> removedSlots = new HashSet<>(allocatedSlots.removeSlotsForTaskManager(resourceId));

		for (AllocatedSlot allocatedSlot : removedSlots) {
			allocatedSlot.releasePayload(cause);
		}

		removedSlots.addAll(availableSlots.removeAllForTaskManager(resourceId));

		for (AllocatedSlot removedSlot : removedSlots) {
			TaskManagerGateway taskManagerGateway = removedSlot.getTaskManagerGateway();
			taskManagerGateway.freeSlot(removedSlot.getAllocationId(), cause, rpcTimeout);
		}
	}

	/**
	 * Check the available slots, release the slot that is idle for a long time.
	 */
	private void checkIdleSlot() {

		// The timestamp in SlotAndTimestamp is relative
		final long currentRelativeTimeMillis = clock.relativeTimeMillis();

		final List<AllocatedSlot> expiredSlots = new ArrayList<>(availableSlots.size());

		for (SlotAndTimestamp slotAndTimestamp : availableSlots.availableSlots.values()) {
			if (currentRelativeTimeMillis - slotAndTimestamp.timestamp > idleSlotTimeout.toMilliseconds()) {
				expiredSlots.add(slotAndTimestamp.slot);
			}
		}

		final FlinkException cause = new FlinkException("Releasing idle slot.");

		for (AllocatedSlot expiredSlot : expiredSlots) {
			final AllocationID allocationID = expiredSlot.getAllocationId();
			if (availableSlots.tryRemove(allocationID) != null) {

				log.info("Releasing idle slot [{}].", allocationID);
				final CompletableFuture<Acknowledge> freeSlotFuture = expiredSlot.getTaskManagerGateway().freeSlot(
					allocationID,
					cause,
					rpcTimeout);

				freeSlotFuture.whenCompleteAsync(
					(Acknowledge ignored, Throwable throwable) -> {
						if (throwable != null) {
							// The slot status will be synced to task manager in next heartbeat.
							log.debug("Releasing slot [{}] of registered TaskExecutor {} failed. Discarding slot.",
								allocationID, expiredSlot.getTaskManagerId(), throwable);
						}
					},
					getMainThreadExecutor());
			}
		}

		scheduleRunAsync(this::checkIdleSlot, idleSlotTimeout);
	}

	/**
	 * Clear the internal state of the SlotPool.
	 */
	private void clear() {
		availableSlots.clear();
		allocatedSlots.clear();
		pendingRequests.clear();
		waitingForResourceManager.clear();
		registeredTaskManagers.clear();
		slotSharingManagers.clear();
	}

	// ------------------------------------------------------------------------
	//  Methods for tests
	// ------------------------------------------------------------------------

	private void scheduledLogStatus() {
		log.debug(printStatus());
		scheduleRunAsync(this::scheduledLogStatus, STATUS_LOG_INTERVAL_MS, TimeUnit.MILLISECONDS);
	}

	private String printStatus() {
		validateRunsInMainThread();

		final StringBuilder builder = new StringBuilder(1024).append("Slot Pool Status:\n");

		builder.append("\tstatus: ");
		if (resourceManagerGateway != null) {
			builder.append("connected to ").append(resourceManagerGateway.getAddress()).append('\n');
		} else {
			builder.append("unconnected and waiting for ResourceManager ")
					.append(waitingForResourceManager)
					.append('\n');
		}

		builder.append("\tregistered TaskManagers: ").append(registeredTaskManagers).append('\n');

		builder.append("\tavailable slots: ").append(availableSlots.printAllSlots()).append('\n');
		builder.append("\tallocated slots: ").append(allocatedSlots.printAllSlots()).append('\n');

		builder.append("\tpending requests: ").append(pendingRequests.values()).append('\n');

		builder.append("\tsharing groups: {\n");
		for (Entry<SlotSharingGroupId, SlotSharingManager> manager : slotSharingManagers.entrySet()) {
			builder.append("\t -------- ").append(manager.getKey()).append(" --------\n");
			builder.append(manager.getValue());
		}
		builder.append("\t}\n");
		return builder.toString();
	}

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

	@VisibleForTesting
	void triggerCheckIdleSlot() {
		runAsync(this::checkIdleSlot);
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

		String printAllSlots() {
			return allocatedSlotsByTaskManager.values().toString();
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
			return allocatedSlotsByTaskManager.getOrDefault(resourceId, Collections.emptySet());
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

				Set<AllocatedSlot> slotsForTaskManager =
					availableSlotsByTaskManager.computeIfAbsent(resourceID, k -> new HashSet<>());
				slotsForTaskManager.add(slot);

				Set<AllocatedSlot> slotsForHost =
					availableSlotsByHost.computeIfAbsent(host, k -> new HashSet<>());
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

		AllocatedSlot get(AllocationID allocationID) {
			SlotAndTimestamp slotAndTimestamp = availableSlots.get(allocationID);
			if (slotAndTimestamp != null) {
				return slotAndTimestamp.slot();
			} else {
				return null;
			}
		}

		/**
		 * Poll a slot which matches the required resource profile. The polling tries to satisfy the
		 * location preferences, by TaskManager and by host.
		 *
		 * @param slotProfile slot profile that specifies the requirements for the slot
		 *
		 * @return Slot which matches the resource profile, null if we can't find a match
		 */
		SlotAndLocality poll(SchedulingStrategy schedulingStrategy, SlotProfile slotProfile) {
			// fast path if no slots are available
			if (availableSlots.isEmpty()) {
				return null;
			}
			Collection<SlotAndTimestamp> slotAndTimestamps = availableSlots.values();

			SlotAndLocality matchingSlotAndLocality = schedulingStrategy.findMatchWithLocality(
				slotProfile,
				slotAndTimestamps::stream,
				SlotAndTimestamp::slot,
				(SlotAndTimestamp slot) -> slot.slot().getResourceProfile().isMatching(slotProfile.getResourceProfile()),
				(SlotAndTimestamp slotAndTimestamp, Locality locality) -> {
					AllocatedSlot slot = slotAndTimestamp.slot();
					return new SlotAndLocality(slot, locality);
				});

			if (matchingSlotAndLocality != null) {
				AllocatedSlot slot = matchingSlotAndLocality.getSlot();
				remove(slot.getAllocationId());
			}

			return matchingSlotAndLocality;
		}

		/**
		 * Remove all available slots come from specified TaskManager.
		 *
		 * @param taskManager The id of the TaskManager
		 * @return The set of removed slots for the given TaskManager
		 */
		Set<AllocatedSlot> removeAllForTaskManager(final ResourceID taskManager) {
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

				return slotsForTm;
			} else {
				return Collections.emptySet();
			}
		}

		AllocatedSlot tryRemove(AllocationID slotId) {
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

				return slot;
			}
			else {
				return null;
			}
		}

		private void remove(AllocationID slotId) throws IllegalStateException {
			if (tryRemove(slotId) == null) {
				throw new IllegalStateException("slot not contained");
			}
		}

		String printAllSlots() {
			return availableSlots.values().toString();
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

		Set<AllocatedSlot> getSlotsForTaskManager(ResourceID resourceId) {
			return availableSlotsByTaskManager.getOrDefault(resourceId, Collections.emptySet());
		}
	}

	// ------------------------------------------------------------------------

	/**
	 * An implementation of the {@link SlotOwner} and {@link SlotProvider} interfaces
	 * that delegates methods as RPC calls to the SlotPool's RPC gateway.
	 */
	public static class ProviderAndOwner implements SlotOwner, SlotProvider {

		private final SlotPoolGateway gateway;
		private final boolean requiresPreviousAllocationsForScheduling;

		ProviderAndOwner(SlotPoolGateway gateway, boolean requiresPreviousAllocationsForScheduling) {
			this.gateway = gateway;
			this.requiresPreviousAllocationsForScheduling = requiresPreviousAllocationsForScheduling;
		}

		public boolean requiresPreviousAllocationsForScheduling() {
			return requiresPreviousAllocationsForScheduling;
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
				SlotRequestId slotRequestId,
				ScheduledUnit task,
				boolean allowQueued,
				SlotProfile slotProfile,
				Time timeout) {

			CompletableFuture<LogicalSlot> slotFuture = gateway.allocateSlot(
				slotRequestId,
				task,
				slotProfile,
				allowQueued,
				timeout);

			slotFuture.whenComplete(
				(LogicalSlot slot, Throwable failure) -> {
					if (failure != null) {
						gateway.releaseSlot(
							slotRequestId,
							task.getSlotSharingGroupId(),
							failure);
					}
			});

			return slotFuture;
		}

		@Override
		public CompletableFuture<Acknowledge> cancelSlotRequest(
				SlotRequestId slotRequestId,
				@Nullable SlotSharingGroupId slotSharingGroupId,
				Throwable cause) {
			return gateway.releaseSlot(slotRequestId, slotSharingGroupId, cause);
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

		@Override
		public String toString() {
			return "PendingRequest{" +
					"slotRequestId=" + slotRequestId +
					", resourceProfile=" + resourceProfile +
					", allocatedSlotFuture=" + allocatedSlotFuture +
					'}';
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
