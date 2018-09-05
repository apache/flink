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
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobmanager.scheduler.Locality;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotContext;
import org.apache.flink.runtime.jobmaster.SlotOwner;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.util.AbstractCollection;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Manager which is responsible for slot sharing. Slot sharing allows to run different
 * tasks in the same slot and to realize co-location constraints.
 *
 * <p>The SlotSharingManager allows to create a hierarchy of {@link TaskSlot} such that
 * every {@link TaskSlot} is uniquely identified by a {@link SlotRequestId} identifying
 * the request for the TaskSlot and a {@link AbstractID} identifying the task or the
 * co-location constraint running in this slot.
 *
 * <p>The {@link TaskSlot} hierarchy is implemented by {@link MultiTaskSlot} and
 * {@link SingleTaskSlot}. The former class represents inner nodes which can contain
 * a number of other {@link TaskSlot} and the latter class represents the leaf nodes.
 * The hierarchy starts with a root {@link MultiTaskSlot} which is a future
 * {@link SlotContext} assigned. The {@link SlotContext} represents the allocated slot
 * on the TaskExecutor in which all slots of this hierarchy run. A {@link MultiTaskSlot}
 * can be assigned multiple {@link SingleTaskSlot} or {@link MultiTaskSlot} if and only if
 * the task slot does not yet contain another child with the same {@link AbstractID} identifying
 * the actual task or the co-location constraint.
 *
 * <p>Normal slot sharing is represented by a root {@link MultiTaskSlot} which contains a set
 * of {@link SingleTaskSlot} on the second layer. Each {@link SingleTaskSlot} represents a different
 * task.
 *
 * <p>Co-location constraints are modeled by adding a {@link MultiTaskSlot} to the root node. The co-location
 * constraint is uniquely identified by a {@link AbstractID} such that we cannot add a second co-located
 * {@link MultiTaskSlot} to the same root node. Now all co-located tasks will be added to co-located
 * multi task slot.
 */
public class SlotSharingManager {

	private static final Logger LOG = LoggerFactory.getLogger(SlotSharingManager.class);

	/** Lock for the internal data structures. */
	private final Object lock = new Object();

	private final SlotSharingGroupId slotSharingGroupId;

	/** Actions to release allocated slots after a complete multi task slot hierarchy has been released. */
	private final AllocatedSlotActions allocatedSlotActions;

	/** Owner of the slots to which to return them when they are released from the outside. */
	private final SlotOwner slotOwner;

	private final Map<SlotRequestId, TaskSlot> allTaskSlots;

	/** Root nodes which have not been completed because the allocated slot is still pending. */
	@GuardedBy("lock")
	private final Map<SlotRequestId, MultiTaskSlot> unresolvedRootSlots;

	/** Root nodes which have been completed (the underlying allocated slot has been assigned). */
	@GuardedBy("lock")
	private final Map<TaskManagerLocation, Set<MultiTaskSlot>> resolvedRootSlots;

	SlotSharingManager(
			SlotSharingGroupId slotSharingGroupId,
			AllocatedSlotActions allocatedSlotActions,
			SlotOwner slotOwner) {
		this.slotSharingGroupId = Preconditions.checkNotNull(slotSharingGroupId);
		this.allocatedSlotActions = Preconditions.checkNotNull(allocatedSlotActions);
		this.slotOwner = Preconditions.checkNotNull(slotOwner);

		allTaskSlots = new HashMap<>(16);
		unresolvedRootSlots = new HashMap<>(16);
		resolvedRootSlots = new HashMap<>(16);
	}

	public boolean isEmpty() {
		return allTaskSlots.isEmpty();
	}

	public boolean contains(SlotRequestId slotRequestId) {
		return allTaskSlots.containsKey(slotRequestId);
	}

	@Nullable
	TaskSlot getTaskSlot(SlotRequestId slotRequestId) {
		return allTaskSlots.get(slotRequestId);
	}

	/**
	 * Creates a new root slot with the given {@link SlotRequestId}, {@link SlotContext} future and
	 * the {@link SlotRequestId} of the allocated slot.
	 *
	 * @param slotRequestId of the root slot
	 * @param slotContextFuture with which we create the root slot
	 * @param allocatedSlotRequestId slot request id of the underlying allocated slot which can be used
	 *                               to cancel the pending slot request or release the allocated slot
	 * @return New root slot
	 */
	MultiTaskSlot createRootSlot(
			SlotRequestId slotRequestId,
			CompletableFuture<? extends SlotContext> slotContextFuture,
			SlotRequestId allocatedSlotRequestId) {
		final MultiTaskSlot rootMultiTaskSlot = new MultiTaskSlot(
			slotRequestId,
			slotContextFuture,
			allocatedSlotRequestId);

		LOG.debug("Create multi task slot [{}] in slot [{}].", slotRequestId, allocatedSlotRequestId);

		allTaskSlots.put(slotRequestId, rootMultiTaskSlot);

		synchronized (lock) {
			unresolvedRootSlots.put(slotRequestId, rootMultiTaskSlot);
		}

		// add the root node to the set of resolved root nodes once the SlotContext future has
		// been completed and we know the slot's TaskManagerLocation
		slotContextFuture.whenComplete(
			(SlotContext slotContext, Throwable throwable) -> {
				if (slotContext != null) {
					synchronized (lock) {
						final MultiTaskSlot resolvedRootNode = unresolvedRootSlots.remove(slotRequestId);

						if (resolvedRootNode != null) {
							LOG.trace("Fulfill multi task slot [{}] with slot [{}].", slotRequestId, slotContext.getAllocationId());

							final Set<MultiTaskSlot> innerCollection = resolvedRootSlots.computeIfAbsent(
								slotContext.getTaskManagerLocation(),
								taskManagerLocation -> new HashSet<>(4));

							innerCollection.add(resolvedRootNode);
						}
					}
				} else {
					rootMultiTaskSlot.release(throwable);
				}
			});

		return rootMultiTaskSlot;
	}

	/**
	 * Gets a resolved root slot which does not yet contain the given groupId. First the given set of
	 * preferred locations is checked.
	 *
	 * @param groupId which the returned slot must not contain
	 * @param matcher slot profile matcher to match slot with the profile requirements
	 * @return the resolved root slot and its locality wrt to the specified location preferences
	 * 		or null if there was no root slot which did not contain the given groupId
	 */
	@Nullable
	MultiTaskSlotLocality getResolvedRootSlot(AbstractID groupId, SchedulingStrategy matcher, SlotProfile slotProfile) {
		synchronized (lock) {
			Collection<Set<MultiTaskSlot>> resolvedRootSlotsValues = this.resolvedRootSlots.values();
			return matcher.findMatchWithLocality(
				slotProfile,
				resolvedRootSlotsValues.stream().flatMap(Collection::stream),
				(MultiTaskSlot multiTaskSlot) -> multiTaskSlot.getSlotContextFuture().join(),
				(MultiTaskSlot multiTaskSlot) -> !multiTaskSlot.contains(groupId),
				MultiTaskSlotLocality::of);
		}
	}

	/**
	 * Gets an unresolved slot which does not yet contain the given groupId. An unresolved
	 * slot is a slot whose underlying allocated slot has not been allocated yet.
	 *
	 * @param groupId which the returned slot must not contain
	 * @return the unresolved slot or null if there was no root slot with free capacities
	 */
	@Nullable
	MultiTaskSlot getUnresolvedRootSlot(AbstractID groupId) {
		synchronized (lock) {
			for (MultiTaskSlot multiTaskSlot : unresolvedRootSlots.values()) {
				if (!multiTaskSlot.contains(groupId)) {
					return multiTaskSlot;
				}
			}
		}

		return null;
	}

	@Override
	public String toString() {
		final StringBuilder builder = new StringBuilder("{\n\tgroupId=").append(slotSharingGroupId).append('\n');

		synchronized (lock) {
			builder.append("\tunresolved=").append(unresolvedRootSlots).append('\n');
			builder.append("\tresolved=").append(resolvedRootSlots).append('\n');
			builder.append("\tall=").append(allTaskSlots).append('\n');
		}

		return builder.append('}').toString();
	}

	// ------------------------------------------------------------------------
	// Inner classes: TaskSlot hierarchy and helper classes
	// ------------------------------------------------------------------------

	/**
	 * Helper class which contains a {@link MultiTaskSlot} and its {@link Locality}.
	 */
	static final class MultiTaskSlotLocality {
		private final MultiTaskSlot multiTaskSlot;

		private final Locality locality;

		MultiTaskSlotLocality(MultiTaskSlot multiTaskSlot, Locality locality) {
			this.multiTaskSlot = Preconditions.checkNotNull(multiTaskSlot);
			this.locality = Preconditions.checkNotNull(locality);
		}

		MultiTaskSlot getMultiTaskSlot() {
			return multiTaskSlot;
		}

		public Locality getLocality() {
			return locality;
		}

		public static MultiTaskSlotLocality of(MultiTaskSlot multiTaskSlot, Locality locality) {
			return new MultiTaskSlotLocality(multiTaskSlot, locality);
		}
	}

	/**
	 * Base class for all task slots.
	 */
	public abstract static class TaskSlot {
		// every TaskSlot has an associated slot request id
		private final SlotRequestId slotRequestId;

		// all task slots except for the root slots have a group id assigned
		@Nullable
		private final AbstractID groupId;

		TaskSlot(SlotRequestId slotRequestId, @Nullable AbstractID groupId) {
			this.slotRequestId = Preconditions.checkNotNull(slotRequestId);
			this.groupId = groupId;
		}

		public SlotRequestId getSlotRequestId() {
			return slotRequestId;
		}

		@Nullable
		public AbstractID getGroupId() {
			return groupId;
		}

		/**
		 * Check whether the task slot contains the given groupId.
		 *
		 * @param groupId which to check whether it is contained
		 * @return true if the task slot contains the given groupId, otherwise false
		 */
		public boolean contains(AbstractID groupId) {
			return Objects.equals(this.groupId, groupId);
		}

		/**
		 * Release the task slot.
		 *
		 * @param cause for the release
		 */
		public abstract void release(Throwable cause);
	}

	/**
	 * {@link TaskSlot} implementation which can have multiple other task slots assigned as children.
	 */
	public final class MultiTaskSlot extends TaskSlot implements AllocatedSlot.Payload {

		private final Map<AbstractID, TaskSlot> children;

		// the root node has its parent set to null
		@Nullable
		private final MultiTaskSlot parent;

		// underlying allocated slot
		private final CompletableFuture<? extends SlotContext> slotContextFuture;

		// slot request id of the allocated slot
		@Nullable
		private final SlotRequestId allocatedSlotRequestId;

		// true if we are currently releasing our children
		private boolean releasingChildren;

		private MultiTaskSlot(
				SlotRequestId slotRequestId,
				AbstractID groupId,
				MultiTaskSlot parent) {
			this(
				slotRequestId,
				groupId,
				Preconditions.checkNotNull(parent),
				parent.getSlotContextFuture(),
				null);
		}

		private MultiTaskSlot(
				SlotRequestId slotRequestId,
				CompletableFuture<? extends SlotContext> slotContextFuture,
				SlotRequestId allocatedSlotRequestId) {
			this(
				slotRequestId,
				null,
				null,
				slotContextFuture,
				allocatedSlotRequestId);
		}

		private MultiTaskSlot(
				SlotRequestId slotRequestId,
				@Nullable AbstractID groupId,
				@Nullable MultiTaskSlot parent,
				CompletableFuture<? extends SlotContext> slotContextFuture,
				@Nullable SlotRequestId allocatedSlotRequestId) {
			super(slotRequestId, groupId);

			this.parent = parent;
			this.slotContextFuture = Preconditions.checkNotNull(slotContextFuture);
			this.allocatedSlotRequestId = allocatedSlotRequestId;

			this.children = new HashMap<>(16);
			this.releasingChildren = false;

			slotContextFuture.whenComplete(
				(SlotContext ignored, Throwable throwable) -> {
					if (throwable != null) {
						release(throwable);
					}
				});
		}

		CompletableFuture<? extends SlotContext> getSlotContextFuture() {
			return slotContextFuture;
		}

		/**
		 * Allocates a MultiTaskSlot and registers it under the given groupId at
		 * this MultiTaskSlot.
		 *
		 * @param slotRequestId of the new multi task slot
		 * @param groupId under which the new multi task slot is registered
		 * @return the newly allocated MultiTaskSlot
		 */
		MultiTaskSlot allocateMultiTaskSlot(SlotRequestId slotRequestId, AbstractID groupId) {
			Preconditions.checkState(!super.contains(groupId));

			LOG.debug("Create nested multi task slot [{}] in parent multi task slot [{}] for group [{}].", slotRequestId, getSlotRequestId(), groupId);

			final MultiTaskSlot inner = new MultiTaskSlot(
				slotRequestId,
				groupId,
				this);

			children.put(groupId, inner);

			// register the newly allocated slot also at the SlotSharingManager
			allTaskSlots.put(slotRequestId, inner);

			return inner;
		}

		/**
		 * Allocates a {@link SingleTaskSlot} and registers it under the given groupId at
		 * this MultiTaskSlot.
		 *
		 * @param slotRequestId of the new single task slot
		 * @param groupId under which the new single task slot is registered
		 * @param locality of the allocation
		 * @return the newly allocated {@link SingleTaskSlot}
		 */
		SingleTaskSlot allocateSingleTaskSlot(
				SlotRequestId slotRequestId,
				AbstractID groupId,
				Locality locality) {
			Preconditions.checkState(!super.contains(groupId));

			LOG.debug("Create single task slot [{}] in multi task slot [{}] for group {}.", slotRequestId, getSlotRequestId(), groupId);

			final SingleTaskSlot leaf = new SingleTaskSlot(
				slotRequestId,
				groupId,
				this,
				locality);

			children.put(groupId, leaf);

			// register the newly allocated slot also at the SlotSharingManager
			allTaskSlots.put(slotRequestId, leaf);

			return leaf;
		}

		/**
		 * Checks whether this slot or any of its children contains the given groupId.
		 *
		 * @param groupId which to check whether it is contained
		 * @return true if this or any of its children contains the given groupId, otherwise false
		 */
		@Override
		public boolean contains(AbstractID groupId) {
			if (super.contains(groupId)) {
				return true;
			} else {
				for (TaskSlot taskSlot : children.values()) {
					if (taskSlot.contains(groupId)) {
						return true;
					}
				}

				return false;
			}
		}

		@Override
		public void release(Throwable cause) {
			releasingChildren = true;

			// first release all children and remove them if they could be released immediately
			for (TaskSlot taskSlot : children.values()) {
				taskSlot.release(cause);
				allTaskSlots.remove(taskSlot.getSlotRequestId());
			}

			children.clear();

			releasingChildren = false;

			if (parent != null) {
				// we remove ourselves from our parent if we no longer have children
				parent.releaseChild(getGroupId());
			} else if (allTaskSlots.remove(getSlotRequestId()) != null) {
				// we are the root node --> remove the root node from the list of task slots

				if (!slotContextFuture.isDone() || slotContextFuture.isCompletedExceptionally()) {
					synchronized (lock) {
						// the root node should still be unresolved
						unresolvedRootSlots.remove(getSlotRequestId());
					}
				} else {
					// the root node should be resolved --> we can access the slot context
					final SlotContext slotContext = slotContextFuture.getNow(null);

					if (slotContext != null) {
						synchronized (lock) {
							final Set<MultiTaskSlot> multiTaskSlots = resolvedRootSlots.get(slotContext.getTaskManagerLocation());

							if (multiTaskSlots != null) {
								multiTaskSlots.remove(this);

								if (multiTaskSlots.isEmpty()) {
									resolvedRootSlots.remove(slotContext.getTaskManagerLocation());
								}
							}
						}
					}
				}

				// release the underlying allocated slot
				allocatedSlotActions.releaseSlot(allocatedSlotRequestId, null, cause);
			}
		}

		/**
		 * Releases the child with the given childGroupId.
		 *
		 * @param childGroupId identifying the child to release
		 */
		private void releaseChild(AbstractID childGroupId) {
			if (!releasingChildren) {
				TaskSlot child = children.remove(childGroupId);

				if (child != null) {
					allTaskSlots.remove(child.getSlotRequestId());
				}

				if (children.isEmpty()) {
					release(new FlinkException("Release multi task slot because all children have been released."));
				}
			}
		}

		@Override
		public String toString() {
			String physicalSlotDescription = "";
			try {
				physicalSlotDescription = String.valueOf(slotContextFuture.getNow(null));
			}
			catch (Exception e) {
				physicalSlotDescription = '(' + ExceptionUtils.stripCompletionException(e).getMessage() + ')';
			}

			return "MultiTaskSlot{"
					+ "requestId=" + getSlotRequestId()
					+ ", allocatedRequestId=" + allocatedSlotRequestId
					+ ", groupId=" + getGroupId()
					+ ", physicalSlot=" + physicalSlotDescription
					+ ", children=" + children.values().toString()
					+ '}';
		}
	}

	/**
	 * {@link TaskSlot} implementation which harbours a {@link LogicalSlot}. The {@link SingleTaskSlot}
	 * cannot have any children assigned.
	 */
	public final class SingleTaskSlot extends TaskSlot {
		private final MultiTaskSlot parent;

		// future containing a LogicalSlot which is completed once the underlying SlotContext future is completed
		private final CompletableFuture<SingleLogicalSlot> singleLogicalSlotFuture;

		private SingleTaskSlot(
				SlotRequestId slotRequestId,
				AbstractID groupId,
				MultiTaskSlot parent,
				Locality locality) {
			super(slotRequestId, groupId);

			this.parent = Preconditions.checkNotNull(parent);

			Preconditions.checkNotNull(locality);
			singleLogicalSlotFuture = parent.getSlotContextFuture()
				.thenApply(
					(SlotContext slotContext) -> {
						LOG.trace("Fulfill single task slot [{}] with slot [{}].", slotRequestId, slotContext.getAllocationId());
						return new SingleLogicalSlot(
							slotRequestId,
							slotContext,
							slotSharingGroupId,
							locality,
							slotOwner);
					});
		}

		CompletableFuture<LogicalSlot> getLogicalSlotFuture() {
			return singleLogicalSlotFuture.thenApply(Function.identity());
		}

		@Override
		public void release(Throwable cause) {
			singleLogicalSlotFuture.completeExceptionally(cause);

			if (singleLogicalSlotFuture.isDone() && !singleLogicalSlotFuture.isCompletedExceptionally()) {
				// we have a single task slot which we first have to release
				final SingleLogicalSlot singleLogicalSlot = singleLogicalSlotFuture.getNow(null);

				singleLogicalSlot.release(cause);
			}

			parent.releaseChild(getGroupId());
		}

		@Override
		public String toString() {
			String logicalSlotString = "(pending)";
			try {
				LogicalSlot slot = singleLogicalSlotFuture.getNow(null);
				if (slot != null) {
					logicalSlotString = "(requestId=" + slot.getSlotRequestId()
							+ ", allocationId=" + slot.getAllocationId() + ')';
				}
			}
			catch (Exception e) {
				logicalSlotString = '(' + ExceptionUtils.stripCompletionException(e).getMessage() + ')';
			}

			return "SingleTaskSlot{"
					+ "logicalSlot=" + logicalSlotString
					+ ", request=" + getSlotRequestId()
					+ ", group=" + getGroupId()
					+ '}';
		}
	}

	// ------------------------------------------------------------------------
	// Methods and classes for testing
	// ------------------------------------------------------------------------

	/**
	 * Returns a collection of all resolved root slots.
	 *
	 * @return Collection of all resolved root slots
	 */
	@VisibleForTesting
	public Collection<MultiTaskSlot> getResolvedRootSlots() {
		return new ResolvedRootSlotValues();
	}

	@VisibleForTesting
	Collection<MultiTaskSlot> getUnresolvedRootSlots() {
		synchronized (lock) {
			return unresolvedRootSlots.values();
		}
	}

	/**
	 * Collection of all resolved {@link MultiTaskSlot} root slots.
	 */
	private final class ResolvedRootSlotValues extends AbstractCollection<MultiTaskSlot> {

		@Override
		public Iterator<MultiTaskSlot> iterator() {
			synchronized (lock) {
				return new ResolvedRootSlotIterator(resolvedRootSlots.values().iterator());
			}
		}

		@Override
		public int size() {
			int numberResolvedMultiTaskSlots = 0;

			synchronized (lock) {
				for (Set<MultiTaskSlot> multiTaskSlots : resolvedRootSlots.values()) {
					numberResolvedMultiTaskSlots += multiTaskSlots.size();
				}
			}

			return numberResolvedMultiTaskSlots;
		}
	}

	/**
	 * Iterator over all resolved {@link MultiTaskSlot} root slots.
	 */
	private static final class ResolvedRootSlotIterator implements Iterator<MultiTaskSlot> {
		private final Iterator<Set<MultiTaskSlot>> baseIterator;
		private Iterator<MultiTaskSlot> currentIterator;

		private ResolvedRootSlotIterator(Iterator<Set<MultiTaskSlot>> baseIterator) {
			this.baseIterator = Preconditions.checkNotNull(baseIterator);

			if (baseIterator.hasNext()) {
				currentIterator = baseIterator.next().iterator();
			} else {
				currentIterator = Collections.emptyIterator();
			}
		}

		@Override
		public boolean hasNext() {
			progressToNextElement();

			return currentIterator.hasNext();
		}

		@Override
		public MultiTaskSlot next() {
			progressToNextElement();

			return currentIterator.next();
		}

		private void progressToNextElement() {
			while (baseIterator.hasNext() && !currentIterator.hasNext()) {
				currentIterator = baseIterator.next().iterator();
			}
		}
	}
}
