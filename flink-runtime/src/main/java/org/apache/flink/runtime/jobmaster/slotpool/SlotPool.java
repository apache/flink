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
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotContext;
import org.apache.flink.runtime.jobmaster.SlotOwner;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.jobmaster.message.PendingSlotRequest;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.SlotRequest;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcTimeout;
import org.apache.flink.runtime.taskexecutor.slot.SlotNotFoundException;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.util.clock.Clock;
import org.apache.flink.runtime.util.clock.SystemClock;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

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

	private Boolean enableSharedSlot;

	// ------------------------------------------------------------------------

	@VisibleForTesting
	protected SlotPool(RpcService rpcService, JobID jobId, SchedulingStrategy schedulingStrategy) {
		this(
			rpcService,
			jobId,
			schedulingStrategy,
			SystemClock.getInstance(),
			AkkaUtils.getDefaultTimeout(),
			Time.milliseconds(JobManagerOptions.SLOT_IDLE_TIMEOUT.defaultValue()),
			true);
	}

	@VisibleForTesting
	public SlotPool(
		RpcService rpcService,
		JobID jobId,
		SchedulingStrategy schedulingStrategy,
		Clock clock,
		Time rpcTimeout,
		Time idleSlotTimeout) {

		this(
			rpcService,
			jobId,
			schedulingStrategy,
			clock,
			rpcTimeout,
			idleSlotTimeout,
			true);
	}

	public SlotPool(
			RpcService rpcService,
			JobID jobId,
			SchedulingStrategy schedulingStrategy,
			Clock clock,
			Time rpcTimeout,
			Time idleSlotTimeout,
			Boolean enableSharedSlot) {

		super(rpcService);

		this.jobId = checkNotNull(jobId);
		this.schedulingStrategy = checkNotNull(schedulingStrategy);
		this.clock = checkNotNull(clock);
		this.rpcTimeout = checkNotNull(rpcTimeout);
		this.idleSlotTimeout = checkNotNull(idleSlotTimeout);
		this.enableSharedSlot = checkNotNull(enableSharedSlot);

		this.registeredTaskManagers = new HashSet<>(16);
		this.allocatedSlots = new AllocatedSlots();
		this.availableSlots = new AvailableSlots();
		this.pendingRequests = new DualKeyMap<>(16);
		this.waitingForResourceManager = new HashMap<>(16);

		this.providerAndOwner = new ProviderAndOwner(getSelfGateway(SlotPoolGateway.class));

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
	//  Failover related methods
	// ------------------------------------------------------------------------

	/**
	 * After job master failover, the slot will be added to slot pool when task executor reports.
	 */
	public LogicalSlot recoverSlot(
			AbstractID groupId,
			SlotSharingGroupId slotSharingGroupId,
			CoLocationConstraint coLocationConstraint,
			AllocationID allocationId,
			TaskManagerLocation taskManagerLocation,
			int slotIndex,
			ResourceProfile resourceProfile,
			TaskManagerGateway taskManagerGateway) throws Exception {
		SlotRequestId allocatedSlotRequestId = new SlotRequestId();
		AllocatedSlot allocatedSlot = allocatedSlots.get(allocationId);
		if (allocatedSlot == null) {
			allocatedSlot = new AllocatedSlot(
					allocationId,
					taskManagerLocation,
					slotIndex,
					resourceProfile,
					taskManagerGateway);
			allocatedSlots.add(allocatedSlotRequestId, allocatedSlot);
			log.debug("Recover allocated slot {} with request id {}.", allocatedSlot, allocatedSlotRequestId);
		}

		if ((enableSharedSlot || coLocationConstraint != null) && slotSharingGroupId != null) {
			final SlotRequestId slotRequestId = new SlotRequestId();
			final SlotSharingManager multiTaskSlotManager = slotSharingManagers.computeIfAbsent(
					slotSharingGroupId,
					id -> new SlotSharingManager(
							id,
							this,
							providerAndOwner));

			// For coLocation
			if (coLocationConstraint != null) {
				SlotRequestId coLocationSlotRequestId = coLocationConstraint.getSlotRequestId();
				if (coLocationSlotRequestId != null) {
					SlotSharingManager.TaskSlot parentSlot = multiTaskSlotManager.getTaskSlot(coLocationSlotRequestId);
					if (parentSlot == null || !(parentSlot instanceof SlotSharingManager.MultiTaskSlot)) {
						log.warn("CoLocation report {} has been assigned slot request id {}, but cannot find multi slot," +
								" this is usually logic error.", allocationId, coLocationSlotRequestId);
						throw new FlinkRuntimeException("Fail to find a slot for " + allocationId);
					}
					return ((SlotSharingManager.MultiTaskSlot) parentSlot)
							.allocateSingleTaskSlot(slotRequestId, groupId, Locality.LOCAL, coLocationConstraint)
							.getLogicalSlotFuture().get();
				}
			}
			Collection<SlotSharingManager.MultiTaskSlot> rootSlots = multiTaskSlotManager.getResolvedRootSlots();
			for (SlotSharingManager.MultiTaskSlot rootSlot : rootSlots) {
				if (rootSlot.getSlotContextFuture().get().getAllocationId().equals(allocationId)) {
					if (coLocationConstraint != null) {
						SlotRequestId coLocationSlotRequestId = new SlotRequestId();
						rootSlot = rootSlot.allocateMultiTaskSlot(coLocationSlotRequestId, coLocationConstraint.getGroupId());
						coLocationConstraint.setSlotRequestId(coLocationSlotRequestId);
						coLocationConstraint.lockLocation(taskManagerLocation);
					}
					return rootSlot.allocateSingleTaskSlot(slotRequestId, groupId, Locality.LOCAL, coLocationConstraint)
							.getLogicalSlotFuture().get();
				}
			}

			final SlotRequestId multiSlotRequestId = new SlotRequestId();
			SlotSharingManager.MultiTaskSlot rootSlot = multiTaskSlotManager.createRootSlot(
					multiSlotRequestId,
					CompletableFuture.completedFuture(allocatedSlot),
					allocatedSlotRequestId);
			if (!allocatedSlot.tryAssignPayload(rootSlot)) {
				Exception e = new FlinkRuntimeException("Fail to assign payload to allocation " + allocationId);
				rootSlot.release(e);
				throw e;
			}
			log.debug("Recover root slot {} with allocated id {}", multiSlotRequestId, allocatedSlotRequestId);
			if (coLocationConstraint != null) {
				SlotRequestId coLocationSlotRequestId = new SlotRequestId();
				rootSlot = rootSlot.allocateMultiTaskSlot(coLocationSlotRequestId, coLocationConstraint.getGroupId());
				log.debug("Recover co location slot {} with group id {}", coLocationSlotRequestId, coLocationConstraint.getGroupId());
				coLocationConstraint.setSlotRequestId(coLocationSlotRequestId);
				coLocationConstraint.lockLocation(taskManagerLocation);
			}
			return rootSlot.allocateSingleTaskSlot(slotRequestId, groupId, Locality.LOCAL, coLocationConstraint)
					.getLogicalSlotFuture().get();
		} else {

			SingleLogicalSlot slot = new SingleLogicalSlot(
					allocatedSlotRequestId,
					allocatedSlot,
					null,
					null,
					Locality.UNKNOWN,
					getSlotOwner());

			allocatedSlot.tryAssignPayload(slot);
			return slot;
		}
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

		final CoLocationConstraint coLocationConstraint = task.getCoLocationConstraint();
		final SlotSharingGroupId slotSharingGroupId = (enableSharedSlot || coLocationConstraint != null) ?
			task.getSlotSharingGroupId() : null;

		if (log.isDebugEnabled()) {
			log.debug("Allocating slot with request {} for task execution {}, CoLocationConstraint: {}, "
					+ "EnableSharedSlot: {}, SlotSharingGroup: {}",
				slotRequestId,
				task.getTaskToExecute(),
				coLocationConstraint,
				enableSharedSlot,
				task.getSlotSharingGroupId());
		}

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
				multiTaskSlotLocality.getLocality(),
				coLocationConstraint);

			setPendingScheduledUnit(slotRequestId, task);

			return leaf.getLogicalSlotFuture();
		} else {
			// request an allocated slot to assign a single logical slot to
			CompletableFuture<SlotAndLocality> slotAndLocalityFuture = requestAllocatedSlot(
				slotRequestId,
				slotProfile,
				allowQueuedScheduling,
				allocationTimeout);

			setPendingScheduledUnit(slotRequestId, task);

			return slotAndLocalityFuture.thenApply(
				(SlotAndLocality slotAndLocality) -> {
					final AllocatedSlot allocatedSlot = slotAndLocality.getSlot();

					final SingleLogicalSlot singleTaskSlot = new SingleLogicalSlot(
						slotRequestId,
						allocatedSlot,
						null,
						null,
						slotAndLocality.getLocality(),
						providerAndOwner);

					if (allocatedSlot.tryAssignPayload(singleTaskSlot)) {
						return singleTaskSlot;
					} else {
						final FlinkException flinkException = new FlinkException("Could not assign payload to allocated slot " + allocatedSlot.getAllocationId() + '.');
						releaseSlot(slotRequestId, null, null, flinkException);
						throw new CompletionException(flinkException);
					}
				});
		}
	}

	// ------------------------------------------------------------------------
	//  Slot Allocation
	// ------------------------------------------------------------------------

	@Override
	public List<CompletableFuture<LogicalSlot>> allocateSlots(
			List<SlotRequestId> slotRequestIds,
			List<ScheduledUnit> tasks,
			List<SlotProfile> slotProfiles,
			boolean allowQueuedScheduling,
			Time allocationTimeout) {

		List<CompletableFuture<LogicalSlot>> returnFutures = new ArrayList<>(slotRequestIds.size());

		List<SlotRequestId> batchSlotRequestIds = new ArrayList<>();
		List<SlotProfile> batchSlotProfiles = new ArrayList<>();
		List<Integer> indexInOriginList = new ArrayList<>();

		for (int i = 0; i < slotRequestIds.size(); i++) {
			final CoLocationConstraint coLocationConstraint = tasks.get(i).getCoLocationConstraint();
			final SlotSharingGroupId slotSharingGroupId = (enableSharedSlot || coLocationConstraint != null) ?
					tasks.get(i).getSlotSharingGroupId() : null;

			if (log.isDebugEnabled()) {
				log.debug("Allocating slot with request {} for task execution {}, CoLocationConstraint: {}, "
								+ "EnableSharedSlot: {}, SlotSharingGroup: {}, SlotProfile: {}",
						slotRequestIds.get(i),
						tasks.get(i).getTaskToExecute(),
						coLocationConstraint,
						enableSharedSlot,
						tasks.get(i).getSlotSharingGroupId(),
						slotProfiles.get(i));
			}

			if (slotSharingGroupId != null) {
				// for sharing group, just call allocateSlot one by one.
				returnFutures.add(i, allocateSlot(slotRequestIds.get(i),
						tasks.get(i),
						slotProfiles.get(i),
						allowQueuedScheduling,
						allocationTimeout));
			} else {
				returnFutures.add(null);
				batchSlotRequestIds.add(slotRequestIds.get(i));
				batchSlotProfiles.add(slotProfiles.get(i));
				indexInOriginList.add(i);
			}
		}

		// batch request allocated slots for non sharing group
		List<CompletableFuture<SlotAndLocality>> slotAndLocalityFuture = requestAllocatedSlots(
				batchSlotRequestIds,
				batchSlotProfiles,
				allowQueuedScheduling,
				allocationTimeout);

		for (int i = 0; i < batchSlotRequestIds.size(); i++) {
			setPendingScheduledUnit(batchSlotRequestIds.get(i), tasks.get(indexInOriginList.get(i)));
		}

		for (int i = 0; i < slotAndLocalityFuture.size(); i++) {
			final int index = i;
			returnFutures.set(indexInOriginList.get(i), slotAndLocalityFuture.get(i).thenApply(
					(SlotAndLocality slotAndLocality) -> {
						final AllocatedSlot allocatedSlot = slotAndLocality.getSlot();

						final SingleLogicalSlot singleTaskSlot = new SingleLogicalSlot(
								batchSlotRequestIds.get(index),
								allocatedSlot,
								null,
								null,
								slotAndLocality.getLocality(),
								providerAndOwner);

						if (allocatedSlot.tryAssignPayload(singleTaskSlot)) {
							return singleTaskSlot;
						} else {
							final FlinkException flinkException = new FlinkException("Could not assign payload to allocated slot " + allocatedSlot.getAllocationId() + '.');
							releaseSlot(batchSlotRequestIds.get(index), null, null, flinkException);
							throw new CompletionException(flinkException);
						}
					})
			);
		}
		return returnFutures;
	}

	@Override
	public CompletableFuture<Collection<PendingSlotRequest>> requestPendingSlotRequests(@RpcTimeout Time timeout) {
		List<PendingSlotRequest> pendingSlotRequests = new ArrayList<>();

		Map<SlotRequestId, ScheduledUnit[]> allUnresolvedScheduledUnits = new HashMap<>();
		Set<SlotRequestId> addedSet = new HashSet<>();
		for (SlotRequestId allocatedSlotRequestId : waitingForResourceManager.keySet()) {
			if (!addedSet.contains(allocatedSlotRequestId)) {
				requestPendingSlotRequests(allocatedSlotRequestId,
						waitingForResourceManager.get(allocatedSlotRequestId),
						allUnresolvedScheduledUnits,
						pendingSlotRequests);
			}
		}
		for (SlotRequestId allocatedSlotRequestId : pendingRequests.keySetA()) {
			if (!addedSet.contains(allocatedSlotRequestId)) {
				requestPendingSlotRequests(allocatedSlotRequestId,
						pendingRequests.getKeyA(allocatedSlotRequestId),
						allUnresolvedScheduledUnits,
						pendingSlotRequests);
			}
		}

		return CompletableFuture.completedFuture(pendingSlotRequests);
	}

	private void requestPendingSlotRequests(
			SlotRequestId allocatedSlotRequestId,
			PendingRequest pendingRequest,
			Map<SlotRequestId, ScheduledUnit[]> allUnresolvedScheduledUnits,
			List<PendingSlotRequest> pendingSlotRequests) {

		if (!allUnresolvedScheduledUnits.containsKey(allocatedSlotRequestId)) {
			ScheduledUnit task = pendingRequest.getScheduledUnit();
			if (task != null) {
				SlotSharingGroupId slotSharingGroupId = task.getSlotSharingGroupId();
				if (slotSharingManagers.containsKey(slotSharingGroupId)) {
					SlotSharingManager slotSharingManager = slotSharingManagers.get(slotSharingGroupId);
					allUnresolvedScheduledUnits.putAll(slotSharingManager.getAllUnresolvedScheduledUnits());
				}
			}
		}

		final List<PendingSlotRequest.PendingScheduledUnit> pendingTasks = new ArrayList<>();
		if (allUnresolvedScheduledUnits.containsKey(allocatedSlotRequestId)) {
			for (ScheduledUnit task : allUnresolvedScheduledUnits.get(allocatedSlotRequestId)) {
				pendingTasks.add(PendingSlotRequest.PendingScheduledUnit.of(task));
			}
		} else {
			pendingTasks.add(PendingSlotRequest.PendingScheduledUnit.of(pendingRequest.getScheduledUnit()));
		}

		pendingSlotRequests.add(new PendingSlotRequest(
				allocatedSlotRequestId, pendingRequest.getResourceProfile(), pendingRequest.getTimestamp(), pendingTasks));
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
				slotProfile.getPriorAllocations());
		}

		// get a new multi task slot
		final SlotSharingManager.MultiTaskSlotLocality multiTaskSlotLocality = allocateMultiTaskSlot(
			coLocationConstraint.getGroupId(), multiTaskSlotManager,
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
				releaseSlot(
					allocatedSlotRequestId,
					null,
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
							releaseSlot(
								allocatedSlotRequestId,
								null,
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
	 * Allocates allocated slots first by polling from the available slots and then requesting new
	 * slots from the ResourceManager if no fitting slot could be found.
	 *
	 * @param slotProfiles slot profiles that specifies the requirements for the slots
	 * @param allowQueuedScheduling true if the slot allocation can be completed in the future
	 * @param allocationTimeout timeout before the slot allocation times out
	 * @return Future containing the allocated simple slot
	 */
	private List<CompletableFuture<SlotAndLocality>> requestAllocatedSlots(
			List<SlotRequestId> slotRequestIds,
			List<SlotProfile> slotProfiles,
			boolean allowQueuedScheduling,
			Time allocationTimeout) {

		final List<CompletableFuture<SlotAndLocality>> allocatedSlotLocalityFutures = new ArrayList<>(slotProfiles.size());

		// (1) do we have a slot available already?
		List<SlotAndLocality> slotsFromPool = pollAndAllocateSlots(slotRequestIds, slotProfiles);

		for (int i = 0; i < slotRequestIds.size(); i++) {
			if (slotsFromPool.get(i) != null) {
				allocatedSlotLocalityFutures.add(i, CompletableFuture.completedFuture(slotsFromPool.get(i)));
			} else if (allowQueuedScheduling) {
				// we have to request a new allocated slot
				CompletableFuture<AllocatedSlot> allocatedSlotFuture = requestNewAllocatedSlot(
						slotRequestIds.get(i),
						slotProfiles.get(i).getResourceProfile(),
						allocationTimeout);

				allocatedSlotLocalityFutures.add(i, allocatedSlotFuture.thenApply((AllocatedSlot allocatedSlot) -> new SlotAndLocality(allocatedSlot, Locality.UNKNOWN)));
			} else {
				allocatedSlotLocalityFutures.add(i, FutureUtils.completedExceptionally(new NoResourceAvailableException("Could not allocate a simple slot for " +
						slotRequestIds.get(i) + '.')));
			}
		}

		return allocatedSlotLocalityFutures;
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

		log.info("Requesting slot with profile {} from resource manager (request = {}).", pendingRequest.getResourceProfile(), pendingRequest.getSlotRequestId());

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
				log.debug("Unregistered slot request {} failed.", slotRequestID, failure);
			}
		}
	}

	private void stashRequestWaitingForResourceManager(final PendingRequest pendingRequest) {

		log.info("Cannot serve slot request, no ResourceManager connected. " +
				"Adding as pending request {}",  pendingRequest.getSlotRequestId());

		waitingForResourceManager.put(pendingRequest.getSlotRequestId(), pendingRequest);
	}

	private void setPendingScheduledUnit(final SlotRequestId slotRequestId, ScheduledUnit task) {
		SlotRequestId allocatedSlotRequestId = null;

		// get the slot request id of the allocated slot, and add the unresolved
		// schedule-unit to SlotSharingManager if the slot is sharing
		SlotSharingGroupId slotSharingGroupId = task.getSlotSharingGroupId();
		if (slotSharingManagers.containsKey(slotSharingGroupId)) {
			SlotSharingManager slotSharingManager = slotSharingManagers.get(slotSharingGroupId);

			CoLocationConstraint coLocationConstraint = task.getCoLocationConstraint();
			SlotSharingManager.TaskSlot taskSlot = slotSharingManager.getTaskSlot(
					(coLocationConstraint != null) ? coLocationConstraint.getSlotRequestId() : slotRequestId);

			SlotRequestId rootSlotRequestId = null;
			while (taskSlot != null) {
				SlotSharingManager.TaskSlot nextSlot = taskSlot.getParent();
				if (nextSlot == null) {
					if (taskSlot instanceof SlotSharingManager.MultiTaskSlot) {
						SlotSharingManager.MultiTaskSlot rootTaskSlot = (SlotSharingManager.MultiTaskSlot) taskSlot;
						allocatedSlotRequestId = rootTaskSlot.getAllocatedSlotRequestId();
						rootSlotRequestId = rootTaskSlot.getSlotRequestId();
					}
					break;
				}
				taskSlot = nextSlot;
			}

			if (pendingRequests.containsKeyA(allocatedSlotRequestId) || waitingForResourceManager.containsKey(allocatedSlotRequestId)) {
				slotSharingManager.addUnresolvedScheduledUnit(rootSlotRequestId, task);
			}
		} else {
			allocatedSlotRequestId = slotRequestId;
		}

		// set the pending schedule-unit for the pending request
		if (allocatedSlotRequestId != null) {
			PendingRequest pendingRequest = waitingForResourceManager.get(allocatedSlotRequestId);
			if (pendingRequest == null) {
				pendingRequest = pendingRequests.getKeyA(allocatedSlotRequestId);
			}
			if (pendingRequest != null && pendingRequest.getScheduledUnit() == null) {
				pendingRequest.setScheduledUnit(task);
				pendingRequest.setTimestamp(clock.absoluteTimeMillis());
			}
		}
	}

	// ------------------------------------------------------------------------
	//  Slot releasing & offering
	// ------------------------------------------------------------------------

	@Override
	public CompletableFuture<Acknowledge> releaseSlot(
			SlotRequestId slotRequestId,
			@Nullable SlotSharingGroupId slotSharingGroupId,
			@Nullable CoLocationConstraint coLocationConstraint,
			Throwable cause) {
		log.debug("Releasing slot with slot request id {} because of {}.", slotRequestId, cause != null ? cause.getMessage() : "null");

		if ((enableSharedSlot || coLocationConstraint != null) && slotSharingGroupId != null) {
			final SlotSharingManager multiTaskSlotManager = slotSharingManagers.get(slotSharingGroupId);

			if (multiTaskSlotManager != null) {
				final SlotSharingManager.TaskSlot taskSlot = multiTaskSlotManager.getTaskSlot(slotRequestId);

				if (taskSlot != null) {
					taskSlot.release(cause);
				} else {
					log.debug("Could not find slot {} in slot sharing group {}. Ignoring release slot request.", slotRequestId, slotSharingGroupId);
				}
			} else {
				log.info("Could not find slot sharing group {}. Ignoring release slot request.", slotSharingGroupId);
			}
		} else {
			final PendingRequest pendingRequest = removePendingRequest(slotRequestId);

			if (pendingRequest != null) {
				failPendingRequest(pendingRequest, new FlinkException("Pending slot request with " + slotRequestId + " has been released."));
			} else {
				final AllocatedSlot allocatedSlot = allocatedSlots.remove(slotRequestId);

				if (allocatedSlot != null) {
					allocatedSlot.releasePayload(cause);
					tryFulfillSlotRequestOrMakeAvailable(allocatedSlot);
				} else {
					log.debug("There is no allocated slot with slot request id {}. Ignoring the release slot request.", slotRequestId);
				}
			}
		}

		return CompletableFuture.completedFuture(Acknowledge.get());
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
		Preconditions.checkNotNull(pendingRequest);
		Preconditions.checkNotNull(e);

		if (!pendingRequest.getAllocatedSlotFuture().isDone()) {
			log.info("Failing pending request {}.", pendingRequest.getSlotRequestId());
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

	private List<SlotAndLocality> pollAndAllocateSlots(List<SlotRequestId> slotRequestIds, List<SlotProfile> slotProfiles) {
		List<SlotAndLocality> slotsFromPool = availableSlots.poll(slotProfiles);

		for (int i = 0; i < slotRequestIds.size(); i++) {
			if (slotsFromPool.get(i) != null) {
				allocatedSlots.add(slotRequestIds.get(i), slotsFromPool.get(i).getSlot());
			}
		}
		return slotsFromPool;
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
			log.debug("Fulfilling pending request [{}] early with returned slot [{}]",
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
			offer -> {
				CompletableFuture<Optional<SlotOffer>> acceptedSlotOffer = offerSlot(
						taskManagerLocation,
						taskManagerGateway,
						offer)
					.thenApply(
						(acceptedSlot) -> acceptedSlot ? Optional.of(offer) : Optional.empty()
					);

				return acceptedSlotOffer;
			}
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
				log.debug("Fulfilled slot request {} with allocated slot {}.", pendingRequest.getSlotRequestId(), allocationID);
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
			log.debug("Failed available slot with allocation id {}.", allocationID, cause);
		}
		else {
			AllocatedSlot allocatedSlot = allocatedSlots.remove(allocationID);
			if (allocatedSlot != null) {
				// release the slot.
				// since it is not in 'allocatedSlots' any more, it will be dropped o return'
				allocatedSlot.releasePayload(cause);
			}
			else {
				log.trace("Outdated request to fail slot with allocation id {}.", allocationID, cause);
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

	// ------------------------------------------------------------------------
	//  Internal methods
	// ------------------------------------------------------------------------

	@VisibleForTesting
	protected void timeoutPendingSlotRequest(SlotRequestId slotRequestId) {
		log.info("Pending slot request {} timed out.", slotRequestId);
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
			if (availableSlots.tryRemove(allocationID)) {

				log.info("Releasing idle slot {}.", allocationID);
				releaseSlotToTaskManager(expiredSlot, cause);
			}
		}

		scheduleRunAsync(this::checkIdleSlot, idleSlotTimeout);
	}

	private void releaseSlotToTaskManager(AllocatedSlot expiredSlot, Throwable cause) {
		final AllocationID allocationID = expiredSlot.getAllocationId();

		final CompletableFuture<Acknowledge> freeSlotFuture = expiredSlot.getTaskManagerGateway().freeSlot(
				allocationID,
				cause,
				rpcTimeout);

		freeSlotFuture.whenCompleteAsync(
				(Acknowledge ignored, Throwable throwable) -> {
					if (throwable != null) {
						if (throwable instanceof SlotNotFoundException) {
							log.debug("The slot {} is dropped as the task executor has released it.", allocationID, throwable);
						}
						else if (registeredTaskManagers.contains(expiredSlot.getTaskManagerId())) {
							log.debug("Releasing slot {} of registered TaskExecutor {} failed. Will retry.",
									allocationID, expiredSlot.getTaskManagerId(), throwable);
							releaseSlotToTaskManager(expiredSlot, cause);
						} else {
							log.debug("Releasing slot {} failed and owning TaskExecutor {} is no " +
									"longer registered. Discarding slot.", allocationID, expiredSlot.getTaskManagerId());
						}
					}
				},
				getMainThreadExecutor());
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
	public int getAvailableSlotsSize() {
		return availableSlots.size();
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

	@VisibleForTesting
	SlotSharingManager getSlotSharingManager(SlotSharingGroupId slotSharingGroupId) {
		return slotSharingManagers.get(slotSharingGroupId);
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
				slotAndTimestamps.stream(),
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
		 * Batch poll slots which match the required resource profiles. The polling first tries to satisfy the
		 * location preferences, by TaskManager and by host.
		 *
		 * <p>If location preferences can not be satisfied and there are slots available, it will try to allocate
		 * slots by resource preferences.
		 *
		 * @param slotProfiles slot profiles that specifies the requirements for the slots
		 *
		 * @return Slots which matches the resource profiles, null if we can't find a match
		 */
		List<SlotAndLocality> poll(List<SlotProfile> slotProfiles) {
			List<SlotAndLocality> matchingSlotAndLocalities = new ArrayList<>(slotProfiles.size());

			// first fulfill the request by previous location.
			for (int i = 0; i < slotProfiles.size(); i++) {
				matchingSlotAndLocalities.add(i,
						poll(PreviousAllocationSchedulingStrategy.getInstance(), slotProfiles.get(i)));
			}

			// fulfill the request if still has available slots.
			for (int i = 0; i < slotProfiles.size(); i++) {
				if (availableSlots.isEmpty()) {
					break;
				}

				if (matchingSlotAndLocalities.get(i) == null) {
					matchingSlotAndLocalities.set(i,
							poll(LocationPreferenceSchedulingStrategy.getInstance(), slotProfiles.get(i)));
				}
			}
			return matchingSlotAndLocalities;
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
	}

	// ------------------------------------------------------------------------

	/**
	 * An implementation of the {@link SlotOwner} and {@link SlotProvider} interfaces
	 * that delegates methods as RPC calls to the SlotPool's RPC gateway.
	 */
	private static class ProviderAndOwner implements SlotOwner, SlotProvider {

		private final SlotPoolGateway gateway;

		ProviderAndOwner(SlotPoolGateway gateway) {
			this.gateway = gateway;
		}

		@Override
		public CompletableFuture<Boolean> returnAllocatedSlot(LogicalSlot slot) {
			return gateway
				.releaseSlot(
					slot.getSlotRequestId(),
					slot.getSlotSharingGroupId(),
					slot.getCoLocationConstraint(),
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
							task.getCoLocationConstraint(),
							failure);
					}
			});

			return slotFuture;
		}

		@Override
		public List<CompletableFuture<LogicalSlot>> allocateSlots(
				List<SlotRequestId> slotRequestIds,
				List<ScheduledUnit> tasks,
				boolean allowQueued,
				List<SlotProfile> slotProfiles,
				Time timeout) {

			List<CompletableFuture<LogicalSlot>> slotFutures = gateway.allocateSlots(
					slotRequestIds,
					tasks,
					slotProfiles,
					allowQueued,
					timeout);

			for (int i = 0; i < slotRequestIds.size(); i++) {
				final int index = i;
				slotFutures.get(i).whenComplete(
						(LogicalSlot slot, Throwable failure) -> {
							if (failure != null) {
								gateway.releaseSlot(
										slotRequestIds.get(index),
										tasks.get(index).getSlotSharingGroupId(),
										tasks.get(index).getCoLocationConstraint(),
										failure);
							}
						});
			}
			return slotFutures;
		}

		@Override
		public CompletableFuture<Acknowledge> cancelSlotRequest(
				SlotRequestId slotRequestId,
				@Nullable SlotSharingGroupId slotSharingGroupId,
				@Nullable CoLocationConstraint coLocationConstraint,
				Throwable cause) {
			return gateway.releaseSlot(slotRequestId, slotSharingGroupId, coLocationConstraint, cause);
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

		private ScheduledUnit scheduledUnit;

		private long timestamp;

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

		public ScheduledUnit getScheduledUnit() {
			return scheduledUnit;
		}

		public void setScheduledUnit(ScheduledUnit scheduledUnit) {
			this.scheduledUnit = scheduledUnit;
		}

		public long getTimestamp() {
			return timestamp;
		}

		public void setTimestamp(long timestamp) {
			this.timestamp = timestamp;
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
