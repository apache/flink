/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationConstraint;
import org.apache.flink.runtime.jobmanager.scheduler.Locality;
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;
import org.apache.flink.runtime.jobmanager.scheduler.ScheduledUnit;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotContext;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Scheduler that assigns tasks to slots. This class is currently work in progress, comments will be updated as we
 * move forward.
 */
public class SchedulerImpl implements Scheduler {

	private static final Logger log = LoggerFactory.getLogger(SchedulerImpl.class);

	private static final int DEFAULT_SLOT_SHARING_MANAGERS_MAP_SIZE = 128;

	/** Strategy that selects the best slot for a given slot allocation request. */
	@Nonnull
	private final SlotSelectionStrategy slotSelectionStrategy;

	/** The slot pool from which slots are allocated. */
	@Nonnull
	private final SlotPool slotPool;

	/** Executor for running tasks in the job master's main thread. */
	@Nonnull
	private ComponentMainThreadExecutor componentMainThreadExecutor;

	/** Managers for the different slot sharing groups. */
	@Nonnull
	private final Map<SlotSharingGroupId, SlotSharingManager> slotSharingManagers;

	public SchedulerImpl(
		@Nonnull SlotSelectionStrategy slotSelectionStrategy,
		@Nonnull SlotPool slotPool) {
		this(slotSelectionStrategy, slotPool, new HashMap<>(DEFAULT_SLOT_SHARING_MANAGERS_MAP_SIZE));
	}

	@VisibleForTesting
	public SchedulerImpl(
		@Nonnull SlotSelectionStrategy slotSelectionStrategy,
		@Nonnull SlotPool slotPool,
		@Nonnull Map<SlotSharingGroupId, SlotSharingManager> slotSharingManagers) {

		this.slotSelectionStrategy = slotSelectionStrategy;
		this.slotSharingManagers = slotSharingManagers;
		this.slotPool = slotPool;
		this.componentMainThreadExecutor = new ComponentMainThreadExecutor.DummyComponentMainThreadExecutor(
			"Scheduler is not initialized with proper main thread executor. " +
				"Call to Scheduler.start(...) required.");
	}

	@Override
	public void start(@Nonnull ComponentMainThreadExecutor mainThreadExecutor) {
		this.componentMainThreadExecutor = mainThreadExecutor;
	}

	//---------------------------

	@Override
	public CompletableFuture<LogicalSlot> allocateSlot(
			SlotRequestId slotRequestId,
			ScheduledUnit scheduledUnit,
			SlotProfile slotProfile,
			Time allocationTimeout) {
		return allocateSlotInternal(
			slotRequestId,
			scheduledUnit,
			slotProfile,
			allocationTimeout);
	}

	@Override
	public CompletableFuture<LogicalSlot> allocateBatchSlot(
			SlotRequestId slotRequestId,
			ScheduledUnit scheduledUnit,
			SlotProfile slotProfile) {
		return allocateSlotInternal(
			slotRequestId,
			scheduledUnit,
			slotProfile,
			null);
	}

	@Nonnull
	private CompletableFuture<LogicalSlot> allocateSlotInternal(
		SlotRequestId slotRequestId,
		ScheduledUnit scheduledUnit,
		SlotProfile slotProfile,
		@Nullable Time allocationTimeout) {
		log.debug("Received slot request [{}] for task: {}", slotRequestId, scheduledUnit.getJobVertexId());

		componentMainThreadExecutor.assertRunningInMainThread();

		final CompletableFuture<LogicalSlot> allocationResultFuture = new CompletableFuture<>();
		internalAllocateSlot(
				allocationResultFuture,
				slotRequestId,
				scheduledUnit,
				slotProfile,
				allocationTimeout);
		return allocationResultFuture;
	}

	private void internalAllocateSlot(
			CompletableFuture<LogicalSlot> allocationResultFuture,
			SlotRequestId slotRequestId,
			ScheduledUnit scheduledUnit,
			SlotProfile slotProfile,
			Time allocationTimeout) {
		CompletableFuture<LogicalSlot> allocationFuture = allocateSharedSlot(
			slotRequestId,
			scheduledUnit,
			slotProfile,
			allocationTimeout);

		allocationFuture.whenComplete((LogicalSlot slot, Throwable failure) -> {
			if (failure != null) {
				cancelSlotRequest(
					slotRequestId,
					scheduledUnit.getSlotSharingGroupId(),
					failure);
				allocationResultFuture.completeExceptionally(failure);
			} else {
				allocationResultFuture.complete(slot);
			}
		});
	}

	@Override
	public void cancelSlotRequest(
		SlotRequestId slotRequestId,
		@Nullable SlotSharingGroupId slotSharingGroupId,
		Throwable cause) {

		componentMainThreadExecutor.assertRunningInMainThread();

		if (slotSharingGroupId != null) {
			releaseSharedSlot(slotRequestId, slotSharingGroupId, cause);
		} else {
			slotPool.releaseSlot(slotRequestId, cause);
		}
	}

	@Override
	public void returnLogicalSlot(LogicalSlot logicalSlot) {
		SlotRequestId slotRequestId = logicalSlot.getSlotRequestId();
		SlotSharingGroupId slotSharingGroupId = logicalSlot.getSlotSharingGroupId();
		FlinkException cause = new FlinkException("Slot is being returned to the SlotPool.");
		cancelSlotRequest(slotRequestId, slotSharingGroupId, cause);
	}

	//---------------------------

	@Nonnull
	private CompletableFuture<PhysicalSlot> requestNewAllocatedSlot(
			SlotRequestId slotRequestId,
			SlotProfile slotProfile,
			@Nullable Time allocationTimeout) {
		if (allocationTimeout == null) {
			return slotPool.requestNewAllocatedBatchSlot(slotRequestId, slotProfile.getPhysicalSlotResourceProfile());
		} else {
			return slotPool.requestNewAllocatedSlot(slotRequestId, slotProfile.getPhysicalSlotResourceProfile(), allocationTimeout);
		}
	}

	private Optional<SlotAndLocality> tryAllocateFromAvailable(
		@Nonnull SlotRequestId slotRequestId,
		@Nonnull SlotProfile slotProfile) {

		Collection<SlotSelectionStrategy.SlotInfoAndResources> slotInfoList =
				slotPool.getAvailableSlotsInformation()
						.stream()
						.map(SlotSelectionStrategy.SlotInfoAndResources::fromSingleSlot)
						.collect(Collectors.toList());

		Optional<SlotSelectionStrategy.SlotInfoAndLocality> selectedAvailableSlot =
			slotSelectionStrategy.selectBestSlotForProfile(slotInfoList, slotProfile);

		return selectedAvailableSlot.flatMap(slotInfoAndLocality -> {
			Optional<PhysicalSlot> optionalAllocatedSlot = slotPool.allocateAvailableSlot(
				slotRequestId,
				slotInfoAndLocality.getSlotInfo().getAllocationId());

			return optionalAllocatedSlot.map(
				allocatedSlot -> new SlotAndLocality(allocatedSlot, slotInfoAndLocality.getLocality()));
		});
	}

	// ------------------------------- slot sharing code

	private CompletableFuture<LogicalSlot> allocateSharedSlot(
		SlotRequestId slotRequestId,
		ScheduledUnit scheduledUnit,
		SlotProfile slotProfile,
		@Nullable Time allocationTimeout) {
		// allocate slot with slot sharing
		final SlotSharingManager multiTaskSlotManager = slotSharingManagers.computeIfAbsent(
			scheduledUnit.getSlotSharingGroupId(),
			id -> new SlotSharingManager(
				id,
				slotPool,
				this));

		final SlotSharingManager.MultiTaskSlotLocality multiTaskSlotLocality;
		try {
			if (scheduledUnit.getCoLocationConstraint() != null) {
				multiTaskSlotLocality = allocateCoLocatedMultiTaskSlot(
					scheduledUnit.getCoLocationConstraint(),
					multiTaskSlotManager,
					slotProfile,
					allocationTimeout);
			} else {
				multiTaskSlotLocality = allocateMultiTaskSlot(
					scheduledUnit.getJobVertexId(),
					multiTaskSlotManager,
					slotProfile,
					allocationTimeout);
			}
		} catch (NoResourceAvailableException noResourceException) {
			return FutureUtils.completedExceptionally(noResourceException);
		}

		// sanity check
		Preconditions.checkState(!multiTaskSlotLocality.getMultiTaskSlot().contains(scheduledUnit.getJobVertexId()));

		final SlotSharingManager.SingleTaskSlot leaf = multiTaskSlotLocality.getMultiTaskSlot().allocateSingleTaskSlot(
			slotRequestId,
			slotProfile.getTaskResourceProfile(),
			scheduledUnit.getJobVertexId(),
			multiTaskSlotLocality.getLocality());
		return leaf.getLogicalSlotFuture();
	}

	/**
	 * Allocates a co-located {@link SlotSharingManager.MultiTaskSlot} for the given {@link CoLocationConstraint}.
	 *
	 * <p>The returned {@link SlotSharingManager.MultiTaskSlot} can be uncompleted.
	 *
	 * @param coLocationConstraint for which to allocate a {@link SlotSharingManager.MultiTaskSlot}
	 * @param multiTaskSlotManager responsible for the slot sharing group for which to allocate the slot
	 * @param slotProfile specifying the requirements for the requested slot
	 * @param allocationTimeout timeout before the slot allocation times out
	 * @return A {@link SlotAndLocality} which contains the allocated{@link SlotSharingManager.MultiTaskSlot}
	 * 		and its locality wrt the given location preferences
	 */
	private SlotSharingManager.MultiTaskSlotLocality allocateCoLocatedMultiTaskSlot(
		CoLocationConstraint coLocationConstraint,
		SlotSharingManager multiTaskSlotManager,
		SlotProfile slotProfile,
		@Nullable Time allocationTimeout) throws NoResourceAvailableException {
		final SlotRequestId coLocationSlotRequestId = coLocationConstraint.getSlotRequestId();

		if (coLocationSlotRequestId != null) {
			// we have a slot assigned --> try to retrieve it
			final SlotSharingManager.TaskSlot taskSlot = multiTaskSlotManager.getTaskSlot(coLocationSlotRequestId);

			if (taskSlot != null) {
				Preconditions.checkState(taskSlot instanceof SlotSharingManager.MultiTaskSlot);

				SlotSharingManager.MultiTaskSlot multiTaskSlot = (SlotSharingManager.MultiTaskSlot) taskSlot;

				if (multiTaskSlot.mayHaveEnoughResourcesToFulfill(slotProfile.getTaskResourceProfile())) {
					return SlotSharingManager.MultiTaskSlotLocality.of(multiTaskSlot, Locality.LOCAL);
				}

				throw new NoResourceAvailableException("Not enough resources in the slot for all co-located tasks.");
			} else {
				// the slot may have been cancelled in the mean time
				coLocationConstraint.setSlotRequestId(null);
			}
		}

		if (coLocationConstraint.isAssigned()) {
			// refine the preferred locations of the slot profile
			slotProfile = SlotProfile.priorAllocation(
				slotProfile.getTaskResourceProfile(),
				slotProfile.getPhysicalSlotResourceProfile(),
				Collections.singleton(coLocationConstraint.getLocation()),
				slotProfile.getPreferredAllocations(),
				slotProfile.getPreviousExecutionGraphAllocations());
		}

		// get a new multi task slot
		SlotSharingManager.MultiTaskSlotLocality multiTaskSlotLocality = allocateMultiTaskSlot(
			coLocationConstraint.getGroupId(),
			multiTaskSlotManager,
			slotProfile,
			allocationTimeout);

		// check whether we fulfill the co-location constraint
		if (coLocationConstraint.isAssigned() && multiTaskSlotLocality.getLocality() != Locality.LOCAL) {
			multiTaskSlotLocality.getMultiTaskSlot().release(
				new FlinkException("Multi task slot is not local and, thus, does not fulfill the co-location constraint."));

			throw new NoResourceAvailableException("Could not allocate a local multi task slot for the " +
				"co location constraint " + coLocationConstraint + '.');
		}

		final SlotRequestId slotRequestId = new SlotRequestId();
		final SlotSharingManager.MultiTaskSlot coLocationSlot =
			multiTaskSlotLocality.getMultiTaskSlot().allocateMultiTaskSlot(
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
	 * <p>The method can return an uncompleted {@link SlotSharingManager.MultiTaskSlot}.
	 *
	 * @param groupId for which to allocate a new {@link SlotSharingManager.MultiTaskSlot}
	 * @param slotSharingManager responsible for the slot sharing group for which to allocate the slot
	 * @param slotProfile slot profile that specifies the requirements for the slot
	 * @param allocationTimeout timeout before the slot allocation times out; null if requesting a batch slot
	 * @return A {@link SlotSharingManager.MultiTaskSlotLocality} which contains the allocated {@link SlotSharingManager.MultiTaskSlot}
	 * 		and its locality wrt the given location preferences
	 */
	private SlotSharingManager.MultiTaskSlotLocality allocateMultiTaskSlot(
			AbstractID groupId,
			SlotSharingManager slotSharingManager,
			SlotProfile slotProfile,
			@Nullable Time allocationTimeout) {

		Collection<SlotSelectionStrategy.SlotInfoAndResources> resolvedRootSlotsInfo =
				slotSharingManager.listResolvedRootSlotInfo(groupId);

		SlotSelectionStrategy.SlotInfoAndLocality bestResolvedRootSlotWithLocality =
			slotSelectionStrategy.selectBestSlotForProfile(resolvedRootSlotsInfo, slotProfile).orElse(null);

		final SlotSharingManager.MultiTaskSlotLocality multiTaskSlotLocality = bestResolvedRootSlotWithLocality != null ?
			new SlotSharingManager.MultiTaskSlotLocality(
				slotSharingManager.getResolvedRootSlot(bestResolvedRootSlotWithLocality.getSlotInfo()),
				bestResolvedRootSlotWithLocality.getLocality()) :
			null;

		if (multiTaskSlotLocality != null && multiTaskSlotLocality.getLocality() == Locality.LOCAL) {
			return multiTaskSlotLocality;
		}

		final SlotRequestId allocatedSlotRequestId = new SlotRequestId();
		final SlotRequestId multiTaskSlotRequestId = new SlotRequestId();

		Optional<SlotAndLocality> optionalPoolSlotAndLocality = tryAllocateFromAvailable(allocatedSlotRequestId, slotProfile);

		if (optionalPoolSlotAndLocality.isPresent()) {
			SlotAndLocality poolSlotAndLocality = optionalPoolSlotAndLocality.get();
			if (poolSlotAndLocality.getLocality() == Locality.LOCAL || bestResolvedRootSlotWithLocality == null) {

				final PhysicalSlot allocatedSlot = poolSlotAndLocality.getSlot();
				final SlotSharingManager.MultiTaskSlot multiTaskSlot = slotSharingManager.createRootSlot(
					multiTaskSlotRequestId,
					CompletableFuture.completedFuture(poolSlotAndLocality.getSlot()),
					allocatedSlotRequestId);

				if (allocatedSlot.tryAssignPayload(multiTaskSlot)) {
					return SlotSharingManager.MultiTaskSlotLocality.of(multiTaskSlot, poolSlotAndLocality.getLocality());
				} else {
					multiTaskSlot.release(new FlinkException("Could not assign payload to allocated slot " +
						allocatedSlot.getAllocationId() + '.'));
				}
			}
		}

		if (multiTaskSlotLocality != null) {
			// prefer slot sharing group slots over unused slots
			if (optionalPoolSlotAndLocality.isPresent()) {
				slotPool.releaseSlot(
					allocatedSlotRequestId,
					new FlinkException("Locality constraint is not better fulfilled by allocated slot."));
			}
			return multiTaskSlotLocality;
		}

		// there is no slot immediately available --> check first for uncompleted slots at the slot sharing group
		SlotSharingManager.MultiTaskSlot multiTaskSlot = slotSharingManager.getUnresolvedRootSlot(groupId);

		if (multiTaskSlot == null) {
			// it seems as if we have to request a new slot from the resource manager, this is always the last resort!!!
			final CompletableFuture<PhysicalSlot> slotAllocationFuture = requestNewAllocatedSlot(
				allocatedSlotRequestId,
				slotProfile,
				allocationTimeout);

			multiTaskSlot = slotSharingManager.createRootSlot(
				multiTaskSlotRequestId,
				slotAllocationFuture,
				allocatedSlotRequestId);

			slotAllocationFuture.whenComplete(
				(PhysicalSlot allocatedSlot, Throwable throwable) -> {
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
						slotPool.releaseSlot(
							allocatedSlotRequestId,
							new FlinkException("Could not find task slot with " + multiTaskSlotRequestId + '.'));
					}
				});
		}

		return SlotSharingManager.MultiTaskSlotLocality.of(multiTaskSlot, Locality.UNKNOWN);
	}

	private void releaseSharedSlot(
		@Nonnull SlotRequestId slotRequestId,
		@Nonnull SlotSharingGroupId slotSharingGroupId,
		Throwable cause) {

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

	@Override
	public boolean requiresPreviousExecutionGraphAllocations() {
		return slotSelectionStrategy instanceof PreviousAllocationSlotSelectionStrategy;
	}
}
