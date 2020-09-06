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

package org.apache.flink.runtime.state.heap;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.state.StateEntry;
import org.apache.flink.runtime.state.StateTransformationFunction;
import org.apache.flink.runtime.state.heap.space.Allocator;
import org.apache.flink.runtime.state.heap.space.Chunk;
import org.apache.flink.runtime.state.heap.space.SpaceUtils;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.ResourceGuard;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.Spliterators;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.apache.flink.runtime.state.heap.SkipListUtils.HEAD_NODE;
import static org.apache.flink.runtime.state.heap.SkipListUtils.NIL_NODE;
import static org.apache.flink.runtime.state.heap.SkipListUtils.NIL_VALUE_POINTER;

/**
 * Implementation of state map which is based on skip list with copy-on-write support. states will
 * be serialized to bytes and stored in the space allocated with the given allocator.
 *
 * @param <K> type of key
 * @param <N> type of namespace
 * @param <S> type of state
 */
public final class CopyOnWriteSkipListStateMap<K, N, S> extends StateMap<K, N, S> implements AutoCloseable {

	private static final Logger LOG = LoggerFactory.getLogger(CopyOnWriteSkipListStateMap.class);

	/**
	 * Default max number of logically-removed keys to delete one time.
	 */
	static final int DEFAULT_MAX_KEYS_TO_DELETE_ONE_TIME = 3;

	/**
	 * Default ratio of the logically-removed keys to trigger deletion when snapshot.
	 */
	static final float DEFAULT_LOGICAL_REMOVED_KEYS_RATIO = 0.2f;

	/**
	 * The serializer used to serialize the key and namespace to bytes stored in skip list.
	 */
	private final SkipListKeySerializer<K, N> skipListKeySerializer;

	/**
	 * The serializer used to serialize the state to bytes stored in skip list.
	 */
	private final SkipListValueSerializer<S> skipListValueSerializer;

	/**
	 * Space allocator.
	 */
	private final Allocator spaceAllocator;

	/**
	 * The level index header.
	 */
	private final LevelIndexHeader levelIndexHeader;

	/**
	 * Seed to generate random index level.
	 */
	private int randomSeed;

	/**
	 * The current version of this map. Used for copy-on-write mechanics.
	 */
	private volatile int stateMapVersion;

	/**
	 * Any version less than this value is still required by some unreleased snapshot. 0 means no snapshot ongoing.
	 */
	private volatile int highestRequiredSnapshotVersionPlusOne;

	/**
	 * Snapshots no more than this version must have been finished, but there may be some
	 * snapshots more than this version are still running.
	 */
	private volatile int highestFinishedSnapshotVersion;

	/**
	 * Maintains an ordered set of version ids that are still used by unreleased snapshots.
	 */
	private final TreeSet<Integer> snapshotVersions;

	/**
	 * The size of skip list which includes the logical removed keys.
	 */
	private int totalSize;

	/**
	 * Number of requests for this skip list.
	 */
	private int requestCount;

	/**
	 * Set of logical removed nodes.
	 */
	private final Set<Long> logicallyRemovedNodes;

	/**
	 * Number of keys to remove physically one time.
	 */
	private int numKeysToDeleteOneTime;

	/**
	 * Ratio of the logically-removed keys to trigger deletion when snapshot.
	 */
	private float logicalRemovedKeysRatio;

	/**
	 * Set of nodes whose values are being pruned by snapshots.
	 */
	private final Set<Long> pruningValueNodes;

	/**
	 * Whether this map has been closed.
	 */
	private final AtomicBoolean closed;

	/**
	 * Guards for the free of space when state map is closed. This is mainly
	 * used to synchronize with snapshots.
	 */
	private final ResourceGuard resourceGuard;

	public CopyOnWriteSkipListStateMap(
			@Nonnull TypeSerializer<K> keySerializer,
			@Nonnull TypeSerializer<N> namespaceSerializer,
			@Nonnull TypeSerializer<S> stateSerializer,
			@Nonnull Allocator spaceAllocator,
			int numKeysToDeleteOneTime,
			float logicalRemovedKeysRatio) {
		this.skipListKeySerializer = new SkipListKeySerializer<>(keySerializer, namespaceSerializer);
		this.skipListValueSerializer = new SkipListValueSerializer<>(stateSerializer);
		this.spaceAllocator = spaceAllocator;
		Preconditions.checkArgument(numKeysToDeleteOneTime >= 0,
			"numKeysToDeleteOneTime should be non-negative, but is "  + numKeysToDeleteOneTime);
		this.numKeysToDeleteOneTime = numKeysToDeleteOneTime;
		Preconditions.checkArgument(logicalRemovedKeysRatio >= 0 && logicalRemovedKeysRatio <= 1,
			"logicalRemovedKeysRatio should be in [0, 1], but is " + logicalRemovedKeysRatio);
		this.logicalRemovedKeysRatio = logicalRemovedKeysRatio;

		this.levelIndexHeader = new OnHeapLevelIndexHeader();
		// Refers to JDK implementation of Xor-shift random number generator, 0x0100 is to ensure non-zero seed.
		// See https://github.com/openjdk-mirror/jdk7u-jdk/blob/master/src/share/classes/java/util/concurrent/ConcurrentSkipListMap.java#L373
		this.randomSeed = ThreadLocalRandom.current().nextInt() | 0x0100;

		this.stateMapVersion = 0;
		this.highestRequiredSnapshotVersionPlusOne = 0;
		this.highestFinishedSnapshotVersion = 0;
		this.snapshotVersions = new TreeSet<>();

		this.totalSize = 0;
		this.requestCount = 0;

		this.logicallyRemovedNodes = new HashSet<>();
		this.pruningValueNodes = ConcurrentHashMap.newKeySet();

		this.closed = new AtomicBoolean(false);
		this.resourceGuard = new ResourceGuard();
	}

	@Override
	public int size() {
		return totalSize - logicallyRemovedNodes.size();
	}

	/**
	 * Returns total size of this map, including logically removed state.
	 */
	int totalSize() {
		return totalSize;
	}

	public int getRequestCount() {
		return requestCount;
	}

	@Override
	public S get(K key, N namespace) {
		updateStat();

		return getNodeInternal(key, namespace);
	}

	@Override
	public boolean containsKey(K key, N namespace) {
		updateStat();

		S node = getNodeInternal(key, namespace);

		return node != null;
	}

	@Override
	public void put(K key, N namespace, S state) {
		updateStat();
		MemorySegment keySegment = getKeySegment(key, namespace);
		int keyLen = keySegment.size();
		byte[] value = skipListValueSerializer.serialize(state);

		putValue(keySegment, 0, keyLen, value, false);
	}

	@Override
	public S putAndGetOld(K key, N namespace, S state) {
		updateStat();
		MemorySegment keySegment = getKeySegment(key, namespace);
		int keyLen = keySegment.size();
		byte[] value = skipListValueSerializer.serialize(state);

		return putValue(keySegment, 0, keyLen, value, true);
	}

	@Override
	public void remove(K key, N namespace) {
		updateStat();
		MemorySegment keySegment = getKeySegment(key, namespace);
		int keyLen = keySegment.size();

		removeNode(keySegment, 0, keyLen, false);
	}

	@Override
	public S removeAndGetOld(K key, N namespace) {
		updateStat();
		MemorySegment keySegment = getKeySegment(key, namespace);
		int keyLen = keySegment.size();

		return removeNode(keySegment, 0, keyLen, true);
	}

	@Override
	public <T> void transform(
		K key,
		N namespace,
		T value,
		StateTransformationFunction<S, T> transformation) throws Exception {
		updateStat();
		MemorySegment keySegment = getKeySegment(key, namespace);
		int keyLen = keySegment.size();

		S oldState = getNode(keySegment, 0, keyLen);
		S newState = transformation.apply(oldState, value);
		byte[] stateBytes = skipListValueSerializer.serialize(newState);
		putValue(keySegment, 0, keyLen, stateBytes, false);
	}

	// Detail implementation methods ---------------------------------------------------------------

	/**
	 * Find the node containing the given key.
	 *
	 * @param keySegment    memory segment storing the key.
	 * @param keyOffset     offset of the key.
	 * @param keyLen        length of the key.
	 * @return the state. Null will be returned if key does not exist.
	 */
	@VisibleForTesting
	@Nullable S getNode(MemorySegment keySegment, int keyOffset, int keyLen) {
		SkipListIterateAndProcessResult result =  iterateAndProcess(keySegment, keyOffset, keyLen,
			(pointers, isRemoved) -> {
				long currentNode = pointers.currentNode;
				return isRemoved ? null : getNodeStateHelper(currentNode);
			});
		return result.isKeyFound ? result.state : null;
	}

	/**
	 * Put the key into the skip list. If the key does not exist before, a new node will be created.
	 * If the key exists before, return the old state or null depending on {@code returnOldState}.
	 *
	 * @param keySegment     memory segment storing the key.
	 * @param keyOffset      offset of the key.
	 * @param keyLen         length of the key.
	 * @param value          the value.
	 * @param returnOldState whether to return old state.
	 * @return the old state. Null will be returned if key does not exist or returnOldState is false.
	 */
	@VisibleForTesting
	S putValue(MemorySegment keySegment, int keyOffset, int keyLen, byte[] value, boolean returnOldState) {
		SkipListIterateAndProcessResult result =  iterateAndProcess(keySegment, keyOffset, keyLen,
			(pointers, isLogicallyRemoved) -> putValue(pointers.currentNode, value, returnOldState));

		if (result.isKeyFound) {
			return result.state;
		}

		long prevNode = result.prevNode;
		long currentNode = result.currentNode;

		int level = getRandomIndexLevel();
		levelIndexHeader.updateLevel(level);

		int totalMetaKeyLen = SkipListUtils.getKeyMetaLen(level) + keyLen;
		long node = allocateSpace(totalMetaKeyLen);

		int totalValueLen = SkipListUtils.getValueMetaLen() + value.length;
		long valuePointer = allocateSpace(totalValueLen);

		doWriteKey(node, level, keySegment, keyOffset, keyLen, valuePointer, currentNode);
		doWriteValue(valuePointer, value, stateMapVersion, node, NIL_VALUE_POINTER);

		helpSetNextNode(prevNode, node, 0);

		if (level > 0) {
			SkipListUtils.buildLevelIndex(node, level, keySegment, keyOffset, levelIndexHeader, spaceAllocator);
		}

		totalSize++;

		return null;
	}

	/**
	 * Update or insert the value for the given node.
	 *
	 * @param currentNode the node to put value for.
	 * @param value the value to put.
	 * @param returnOldState whether to return the old state.
	 * @return the old state if it exists and {@code returnOldState} is true, or else null.
	 */
	private S putValue(long currentNode, byte[] value, boolean returnOldState) {
		int version = SkipListUtils.helpGetNodeLatestVersion(currentNode, spaceAllocator);
		boolean needCopyOnWrite = version < highestRequiredSnapshotVersionPlusOne;
		long oldValuePointer;

		if (needCopyOnWrite) {
			oldValuePointer = updateValueWithCopyOnWrite(currentNode, value);
		} else {
			oldValuePointer = updateValueWithReplace(currentNode, value);
		}

		NodeStatus oldStatus = helpSetNodeStatus(currentNode, NodeStatus.PUT);
		if (oldStatus == NodeStatus.REMOVE) {
			logicallyRemovedNodes.remove(currentNode);
		}

		S oldState = null;
		if (returnOldState) {
			oldState = helpGetState(oldValuePointer);
		}

		// for the replace, old value space need to free
		if (!needCopyOnWrite) {
			spaceAllocator.free(oldValuePointer);
		}

		return oldState;
	}

	/**
	 * Remove the key from the skip list. The key can be removed logically or physically.
	 * Logical remove means put a null value whose size is 0. If the key exists before,
	 * the old value state will be returned.
	 *
	 * @param keySegment     memory segment storing the key.
	 * @param keyOffset      offset of the key.
	 * @param keyLen         length of the key.
	 * @param returnOldState whether to return old state.
	 * @return the old state. Null will be returned if key does not exist or returnOldState is false.
	 */
	private S removeNode(MemorySegment keySegment, int keyOffset, int keyLen, boolean returnOldState) {
		SkipListIterateAndProcessResult result = iterateAndProcess(keySegment, keyOffset, keyLen,
			(pointers, isLogicallyRemoved) -> removeNode(pointers, isLogicallyRemoved, returnOldState));
		return result.isKeyFound ? result.state : null;
	}

	/**
	 * Remove the given node indicated by {@link SkipListNodePointers#currentNode}.
	 *
	 * @param pointers pointers of the node to remove and its prev/next node.
	 * @param isLogicallyRemoved whether the node to remove is already logically removed.
	 * @param returnOldState whether to return the old state after removal.
	 * @return the old state if {@code returnOldState} is true, or else return null.
	 */
	private S removeNode(SkipListNodePointers pointers, Boolean isLogicallyRemoved, boolean returnOldState) {
		long prevNode = pointers.prevNode;
		long currentNode = pointers.currentNode;
		long nextNode = pointers.nextNode;
		// if the node has been logically removed, and can not be physically
		// removed here, just return null
		if (isLogicallyRemoved && highestRequiredSnapshotVersionPlusOne != 0) {
			return null;
		}

		long oldValuePointer;
		boolean oldValueNeedFree;

		if (highestRequiredSnapshotVersionPlusOne == 0) {
			// do physically remove only when there is no snapshot running
			oldValuePointer = doPhysicalRemoveAndGetValue(currentNode, prevNode, nextNode);
			// the node has been logically removed, and remove it from the set
			if (isLogicallyRemoved) {
				logicallyRemovedNodes.remove(currentNode);
			}
			oldValueNeedFree = true;
		} else {
			int version = SkipListUtils.helpGetNodeLatestVersion(currentNode, spaceAllocator);
			if (version < highestRequiredSnapshotVersionPlusOne) {
				// the newest-version value may be used by snapshots, and update it with copy-on-write
				oldValuePointer = updateValueWithCopyOnWrite(currentNode, null);
				oldValueNeedFree = false;
			} else {
				// replace the newest-version value.
				oldValuePointer = updateValueWithReplace(currentNode, null);
				oldValueNeedFree = true;
			}

			helpSetNodeStatus(currentNode, NodeStatus.REMOVE);
			logicallyRemovedNodes.add(currentNode);
		}

		S oldState = null;
		if (returnOldState) {
			oldState = helpGetState(oldValuePointer);
		}

		if (oldValueNeedFree) {
			spaceAllocator.free(oldValuePointer);
		}

		return oldState;
	}

	private long allocateSpace(int size) {
		try {
			return spaceAllocator.allocate(size);
		} catch (Exception e) {
			throw new FlinkRuntimeException("Failed to allocate space in CopyOnWriteSkipListStateMap", e);
		}
	}

	/**
	 * Iterate the skip list and perform given function.
	 *
	 * @param keySegment memory segment storing the key.
	 * @param keyOffset offset of the key.
	 * @param keyLen length of the key.
	 * @param function the function to apply when the skip list contains the given key, which accepts two parameters:
	 *                 an encapsulation of [previous_node, current_node, next_node] and a boolean indicating
	 *                 whether the node with same key has been logically removed, and returns a state.
	 * @return the iterate and processing result
	 */
	private SkipListIterateAndProcessResult iterateAndProcess(
		MemorySegment keySegment,
		int keyOffset,
		int keyLen,
		BiFunction<SkipListNodePointers, Boolean, S> function) {
		int deleteCount = 0;
		long prevNode = findPredecessor(keySegment, keyOffset, 1);
		long currentNode = helpGetNextNode(prevNode, 0);
		long nextNode;

		int c;
		while (currentNode != NIL_NODE) {
			nextNode = helpGetNextNode(currentNode, 0);

			// Check whether the current code is already logically removed to save some comparisons on key,
			// with the cost of an additional remove-then-add operation if the to-be-removed node has the same key
			// with the to-be-put one.
			boolean isRemoved = isNodeRemoved(currentNode);
			if (isRemoved && highestRequiredSnapshotVersionPlusOne == 0 && deleteCount < numKeysToDeleteOneTime) {
				doPhysicalRemove(currentNode, prevNode, nextNode);
				logicallyRemovedNodes.remove(currentNode);
				currentNode = nextNode;
				deleteCount++;
				continue;
			}

			c = compareSegmentAndNode(keySegment, keyOffset, keyLen, currentNode);

			if (c < 0) {
				// The given key is less than the current node, break the loop
				break;
			} else if (c > 0) {
				// The given key is larger than the current node, continue
				prevNode = currentNode;
				currentNode = nextNode;
			} else {
				// The given key is equal to the current node, apply the function
				S state = function.apply(new SkipListNodePointers(prevNode, currentNode, nextNode), isRemoved);
				return new SkipListIterateAndProcessResult(prevNode, currentNode, true, state);
			}
		}
		return new SkipListIterateAndProcessResult(prevNode, currentNode, false, null);
	}

	/**
	 * Find the predecessor node for the given key at the given level.
	 * The key is in the memory segment positioning at the given offset.
	 *
	 * @param keySegment    memory segment which contains the key.
	 * @param keyOffset     offset of the key in the memory segment.
	 * @param level         the level.
	 * @return node id before the key at the given level.
	 */
	private long findPredecessor(MemorySegment keySegment, int keyOffset, int level) {
		return SkipListUtils.findPredecessor(keySegment, keyOffset, level, levelIndexHeader, spaceAllocator);
	}

	/**
	 * Compare the first skip list key in the given memory segment with the second skip list key in the given node.
	 *
	 * @param keySegment    memory segment storing the first key.
	 * @param keyOffset     offset of the first key in memory segment.
	 * @param keyLen        length of the first key.
	 * @param targetNode    the node storing the second key.
	 * @return Returns a negative integer, zero, or a positive integer as the first key is less than,
	 * equal to, or greater than the second.
	 */
	private int compareSegmentAndNode(MemorySegment keySegment, int keyOffset, int keyLen, long targetNode) {
		return SkipListUtils.compareSegmentAndNode(keySegment, keyOffset, targetNode, spaceAllocator);
	}

	/**
	 * Compare the first namespace in the given memory segment with the second namespace in the given node.
	 *
	 * @param namespaceSegment    memory segment storing the first namespace.
	 * @param namespaceOffset     offset of the first namespace in memory segment.
	 * @param namespaceLen        length of the first namespace.
	 * @param targetNode          the node storing the second namespace.
	 * @return Returns a negative integer, zero, or a positive integer as the first key is less than,
	 * equal to, or greater than the second.
	 */
	private int compareNamespaceAndNode(MemorySegment namespaceSegment, int namespaceOffset, int namespaceLen, long targetNode) {
		Node nodeStorage = getNodeSegmentAndOffset(targetNode);
		MemorySegment targetSegment = nodeStorage.nodeSegment;
		int offsetInSegment = nodeStorage.nodeOffset;

		int level = SkipListUtils.getLevel(targetSegment, offsetInSegment);
		int targetKeyOffset = offsetInSegment + SkipListUtils.getKeyDataOffset(level);

		return SkipListKeyComparator.compareNamespaceAndNode(namespaceSegment, namespaceOffset, namespaceLen,
			targetSegment, targetKeyOffset);
	}

	/**
	 * Update the value of the node with copy-on-write mode. The old value will
	 * be linked after the new value, and can be still accessed.
	 *
	 * @param node  the node to update.
	 * @param value the value.
	 * @return the old value pointer.
	 */
	private long updateValueWithCopyOnWrite(long node, byte[] value) {
		// a null value indicates this is a removed node
		int valueSize = value == null ? 0 : value.length;
		int totalValueLen = SkipListUtils.getValueMetaLen() + valueSize;
		long valuePointer = allocateSpace(totalValueLen);

		Node nodeStorage = getNodeSegmentAndOffset(node);
		MemorySegment nodeSegment = nodeStorage.nodeSegment;
		int offsetInNodeSegment = nodeStorage.nodeOffset;
		long oldValuePointer = SkipListUtils.getValuePointer(nodeSegment, offsetInNodeSegment);

		doWriteValue(valuePointer, value, stateMapVersion, node, oldValuePointer);

		// update value pointer in node after the new value has points the older value so that
		// old value can be accessed concurrently
		SkipListUtils.putValuePointer(nodeSegment, offsetInNodeSegment, valuePointer);

		return oldValuePointer;
	}

	/**
	 * Update the value of the node with replace mode. The old value will be unlinked and replaced
	 * by the new value, and can not be accessed later. Note that the space of the old value
	 * is not freed here, and the caller of this method should be responsible for the space management.
	 *
	 * @param node  the node whose value will be replaced.
	 * @param value the value.
	 * @return the old value pointer.
	 */
	private long updateValueWithReplace(long node, byte[] value) {
		// a null value indicates this is a removed node
		int valueSize = value == null ? 0 : value.length;
		int totalValueLen = SkipListUtils.getValueMetaLen() + valueSize;
		long valuePointer = allocateSpace(totalValueLen);

		Node nodeStorage = getNodeSegmentAndOffset(node);
		MemorySegment nodeSegment = nodeStorage.nodeSegment;
		int offsetInNodeSegment = nodeStorage.nodeOffset;

		long oldValuePointer = SkipListUtils.getValuePointer(nodeSegment, offsetInNodeSegment);
		long nextValuePointer = SkipListUtils.helpGetNextValuePointer(oldValuePointer, spaceAllocator);

		doWriteValue(valuePointer, value, stateMapVersion, node, nextValuePointer);

		// update value pointer in node after the new value has points the older value so that
		// old value can be accessed concurrently
		SkipListUtils.putValuePointer(nodeSegment, offsetInNodeSegment, valuePointer);

		return oldValuePointer;
	}

	/**
	 * Removes the node physically, and free all space used by the key and value.
	 *
	 * @param node     node to remove.
	 * @param prevNode previous node at the level 0.
	 * @param nextNode next node at the level 0.
	 */
	private void doPhysicalRemove(long node, long prevNode, long nextNode) {
		// free space used by key and level index
		long valuePointer = deleteNodeMeta(node, prevNode, nextNode);
		// free space used by value
		SkipListUtils.removeAllValues(valuePointer, spaceAllocator);
	}

	/**
	 * Removes the node physically, and return the newest-version value pointer.
	 * Space used by key and value will be freed here, but the space of newest-version
	 * value will not be freed, and the caller should be responsible for the free
	 * of space.
	 *
	 * @param node     node to remove.
	 * @param prevNode previous node at the level 0.
	 * @param nextNode next node at the level 0.
	 * @return newest-version value pointer.
	 */
	private long doPhysicalRemoveAndGetValue(long node, long prevNode, long nextNode) {
		// free space used by key and level index
		long valuePointer = deleteNodeMeta(node, prevNode, nextNode);
		// free space used by values except for the newest-version
		long nextValuePointer = SkipListUtils.helpGetNextValuePointer(valuePointer, spaceAllocator);
		SkipListUtils.removeAllValues(nextValuePointer, spaceAllocator);

		return valuePointer;
	}

	/**
	 * Physically delte the meta of the node, including the node level index, the node key, and reduce the total size of
	 * the skip list.
	 *
	 * @param node node to remove.
	 * @param prevNode previous node at the level 0.
	 * @param nextNode next node at the level 0.
	 * @return value pointer of the node.
	 */
	private long deleteNodeMeta(long node, long prevNode, long nextNode) {
		// set next node of prevNode at level 0 to nextNode
		helpSetNextNode(prevNode, nextNode, 0);

		// remove the level index for the node
		SkipListUtils.removeLevelIndex(node, spaceAllocator, levelIndexHeader);

		// free space used by key
		long valuePointer = SkipListUtils.helpGetValuePointer(node, spaceAllocator);
		this.spaceAllocator.free(node);

		// reduce total size of the skip list
		// note that we regard the node to be removed once its meta is deleted
		totalSize--;

		return valuePointer;
	}

	/**
	 * Return a random level for new node.
	 * <p/>
	 * The implementation refers to the {@code randomLevel} method of JDK7's ConcurrentSkipListMap. See
	 * https://github.com/openjdk-mirror/jdk7u-jdk/blob/master/src/share/classes/java/util/concurrent/ConcurrentSkipListMap.java#L899
	 */
	private int getRandomIndexLevel() {
		int x = randomSeed;
		x ^= x << 13;
		x ^= x >>> 17;
		x ^= x << 5;
		randomSeed = x;
		// test highest and lowest bits
		if ((x & 0x8001) != 0) {
			return 0;
		}
		int level = 1;
		int curMax = levelIndexHeader.getLevel();
		x >>>= 1;
		while ((x & 1) != 0) {
			++level;
			x >>>= 1;
			// the level only be increased by step
			if (level > curMax) {
				break;
			}
		}
		return level;
	}

	/**
	 * Write the meta and data for the key to the given node.
	 *
	 * @param node          the node for the key to write.
	 * @param level         level of this node.
	 * @param keySegment    memory segment storing the key.
	 * @param keyOffset     offset of key in memory segment.
	 * @param keyLen        length of the key.
	 * @param valuePointer  pointer to value.
	 * @param nextNode      next node on level 0.
	 */
	private void doWriteKey(
		long node,
		int level,
		MemorySegment keySegment,
		int keyOffset,
		int keyLen,
		long valuePointer,
		long nextNode) {
		Node nodeStorage = getNodeSegmentAndOffset(node);
		MemorySegment segment = nodeStorage.nodeSegment;
		int offsetInSegment = nodeStorage.nodeOffset;

		SkipListUtils.putLevelAndNodeStatus(segment, offsetInSegment, level, NodeStatus.PUT);
		SkipListUtils.putKeyLen(segment, offsetInSegment, keyLen);
		SkipListUtils.putValuePointer(segment, offsetInSegment, valuePointer);
		SkipListUtils.putNextKeyPointer(segment, offsetInSegment, nextNode);
		SkipListUtils.putKeyData(segment, offsetInSegment, keySegment, keyOffset, keyLen, level);
	}

	/**
	 * Write the meta and data for the value to the space where the value pointer points.
	 *
	 * @param valuePointer     pointer to the space where the meta and data is written.
	 * @param value            data of the value.
	 * @param version          version of this value.
	 * @param keyPointer       pointer to the key.
	 * @param nextValuePointer pointer to the next value.
	 */
	private void doWriteValue(
		long valuePointer,
		byte[] value,
		int version,
		long keyPointer,
		long nextValuePointer) {
		Node node = getNodeSegmentAndOffset(valuePointer);
		MemorySegment segment = node.nodeSegment;
		int offsetInSegment = node.nodeOffset;

		SkipListUtils.putValueVersion(segment, offsetInSegment, version);
		SkipListUtils.putKeyPointer(segment, offsetInSegment, keyPointer);
		SkipListUtils.putNextValuePointer(segment, offsetInSegment, nextValuePointer);
		SkipListUtils.putValueLen(segment, offsetInSegment, value == null ? 0 : value.length);
		if (value != null) {
			SkipListUtils.putValueData(segment, offsetInSegment, value);
		}
	}

	/**
	 * Find the first node with the given namespace at level 0.
	 *
	 * @param namespaceSegment memory segment storing the namespace.
	 * @param namespaceOffset     offset of the namespace.
	 * @param namespaceLen        length of the namespace.
	 * @return the first node with the given namespace.
	 *  NIL_NODE will be returned if not exist.
	 */
	private long getFirstNodeWithNamespace(MemorySegment namespaceSegment, int namespaceOffset, int namespaceLen) {
		int currentLevel = levelIndexHeader.getLevel();
		long prevNode = HEAD_NODE;
		long currentNode = helpGetNextNode(prevNode, currentLevel);

		int c;
		// find the predecessor node at level 0.
		for ( ; ; ) {
			if (currentNode != NIL_NODE) {
				c = compareNamespaceAndNode(namespaceSegment, namespaceOffset, namespaceLen, currentNode);
				if (c > 0) {
					prevNode = currentNode;
					currentNode = helpGetNextNode(prevNode, currentLevel);
					continue;
				}
			}

			currentLevel--;
			if (currentLevel < 0) {
				break;
			}
			currentNode = helpGetNextNode(prevNode, currentLevel);
		}

		// find the first node that has not been logically removed
		while (currentNode != NIL_NODE) {
			if (isNodeRemoved(currentNode)) {
				currentNode = helpGetNextNode(currentNode, 0);
				continue;
			}

			c = compareNamespaceAndNode(namespaceSegment, namespaceOffset, namespaceLen, currentNode);
			if (c == 0) {
				return currentNode;
			}

			if (c < 0) {
				break;
			}
		}

		return NIL_NODE;
	}

	/**
	 * Try to delete some nodes that has been logically removed.
	 */
	private void tryToDeleteNodesPhysically() {
		if (highestRequiredSnapshotVersionPlusOne != 0) {
			return;
		}

		int threshold = (int) (totalSize * logicalRemovedKeysRatio);
		int size = logicallyRemovedNodes.size();
		if (size > threshold) {
			deleteLogicallyRemovedNodes(size - threshold);
		}
	}

	private void deleteLogicallyRemovedNodes(int maxNodes) {
		int count = 0;
		Iterator<Long> nodeIterator = logicallyRemovedNodes.iterator();
		while (count < maxNodes && nodeIterator.hasNext()) {
			deleteNode(nodeIterator.next());
			nodeIterator.remove();
			count++;
		}
	}

	private void deleteNode(long node) {
		long prevNode = SkipListUtils.findPredecessor(node, 1, levelIndexHeader, spaceAllocator);
		long currentNode = helpGetNextNode(prevNode, 0);
		while (currentNode != node) {
			prevNode = currentNode;
			currentNode = helpGetNextNode(prevNode, 0);
		}

		long nextNode = helpGetNextNode(currentNode, 0);
		doPhysicalRemove(currentNode, prevNode, nextNode);
	}

	/**
	 * Release all resource used by the map.
	 */
	private void releaseAllResource() {
		long node = levelIndexHeader.getNextNode(0);
		while (node != NIL_NODE) {
			long nextNode = helpGetNextNode(node, 0);
			long valuePointer = SkipListUtils.helpGetValuePointer(node, spaceAllocator);
			spaceAllocator.free(node);
			SkipListUtils.removeAllValues(valuePointer, spaceAllocator);
			node = nextNode;
		}
		totalSize = 0;
		logicallyRemovedNodes.clear();
	}

	/**
	 * Returns the value pointer used by the snapshot of the given version.
	 *
	 * @param snapshotVersion version of snapshot.
	 * @return the value pointer of the version used by the given snapshot. NIL_VALUE_POINTER
	 * 	will be returned if there is no value for this snapshot.
	 */
	long getValueForSnapshot(long node, int snapshotVersion) {
		long snapshotValuePointer = NIL_VALUE_POINTER;
		ValueVersionIterator versionIterator = new ValueVersionIterator(node);
		long valuePointer;
		while (versionIterator.hasNext()) {
			valuePointer = versionIterator.getValuePointer();
			int version = versionIterator.next();
			// the first value whose version is less than snapshotVersion
			if (version < snapshotVersion) {
				snapshotValuePointer = valuePointer;
				break;
			}
		}

		return snapshotValuePointer;
	}

	/**
	 * Returns the value pointer used by the snapshot of the given version,
	 * and useless version values will be pruned.
	 *
	 * @param snapshotVersion version of snapshot.
	 * @return the value pointer of the version used by the given snapshot. NIL_VALUE_POINTER
	 * 	will be returned if there is no value for this snapshot.
	 */
	long getAndPruneValueForSnapshot(long node, int snapshotVersion) {
		// whether the node is being pruned by some snapshot
		boolean isPruning = pruningValueNodes.add(node);
		try {
			long snapshotValuePointer = NIL_VALUE_POINTER;
			long valuePointer;
			ValueVersionIterator versionIterator = new ValueVersionIterator(node);
			while (versionIterator.hasNext()) {
				valuePointer = versionIterator.getValuePointer();
				int version = versionIterator.next();
				// find the first value whose version is less than snapshotVersion
				if (snapshotValuePointer == NIL_VALUE_POINTER && version < snapshotVersion) {
					snapshotValuePointer = valuePointer;
					if (!isPruning) {
						break;
					}
				}

				// if the version of the value is no more than highestFinishedSnapshotVersion,
				// snapshots that is running and to be run will not use the values who are
				// older than this version, so these values can be safely removed.
				if (highestFinishedSnapshotVersion >= version) {
					long nextValuePointer = SkipListUtils.helpGetNextValuePointer(valuePointer, spaceAllocator);
					if (nextValuePointer != NIL_VALUE_POINTER) {
						SkipListUtils.helpSetNextValuePointer(valuePointer, NIL_VALUE_POINTER, spaceAllocator);
						SkipListUtils.removeAllValues(nextValuePointer, spaceAllocator);
					}
					break;
				}
			}

			return snapshotValuePointer;
		} finally {
			// only remove the node from the set when this snapshot has pruned values
			if (isPruning) {
				pruningValueNodes.remove(node);
			}
		}
	}

	/**
	 * Update some statistics.
	 */
	private void updateStat() {
		requestCount++;
	}

	/**
	 * Find the node containing the given key.
	 *
	 * @param key       the key.
	 * @param namespace the namespace.
	 * @return id of the node. NIL_NODE will be returned if key does no exist.
	 */
	private S getNodeInternal(K key, N namespace) {
		MemorySegment keySegment = getKeySegment(key, namespace);
		int keyLen = keySegment.size();

		return getNode(keySegment, 0, keyLen);
	}

	/**
	 * Get the {@link MemorySegment} wrapping up the serialized key bytes.
	 *
	 * @param key       the key.
	 * @param namespace the namespace.
	 * @return the {@link MemorySegment} wrapping up the serialized key bytes.
	 */
	private MemorySegment getKeySegment(K key, N namespace) {
		return skipListKeySerializer.serializeToSegment(key, namespace);
	}

	// Help methods ---------------------------------------------------------------

	/**
	 * Whether the node has been logically removed.
	 */
	private boolean isNodeRemoved(long node) {
		return SkipListUtils.isNodeRemoved(node, spaceAllocator);
	}

	/**
	 * Set the next node of the given node at the given level.
	 */
	private void helpSetNextNode(long node, long nextNode, int level) {
		SkipListUtils.helpSetNextNode(node, nextNode, level, levelIndexHeader, spaceAllocator);
	}

	/**
	 * Return the next of the given node at the given level.
	 */
	long helpGetNextNode(long node, int level) {
		return SkipListUtils.helpGetNextNode(node, level, this.levelIndexHeader, this.spaceAllocator);
	}

	/**
	 * Returns the length of the value.
	 */
	int helpGetValueLen(long valuePointer) {
		return SkipListUtils.helpGetValueLen(valuePointer, spaceAllocator);
	}

	/**
	 * Set node status to the given new status, and return old status.
	 */
	private NodeStatus helpSetNodeStatus(long node, NodeStatus newStatus) {
		Node nodeStorage = getNodeSegmentAndOffset(node);
		MemorySegment segment = nodeStorage.nodeSegment;
		int offsetInSegment = nodeStorage.nodeOffset;
		NodeStatus oldStatus = SkipListUtils.getNodeStatus(segment, offsetInSegment);
		if (oldStatus != newStatus) {
			int level = SkipListUtils.getLevel(segment, offsetInSegment);
			SkipListUtils.putLevelAndNodeStatus(segment, offsetInSegment, level, newStatus);
		}

		return oldStatus;
	}

	/**
	 * Return the state of the node. null will be returned if the node is removed.
	 */
	private S getNodeStateHelper(long node) {
		Node nodeStorage = getNodeSegmentAndOffset(node);
		MemorySegment segment = nodeStorage.nodeSegment;
		int offsetInSegment = nodeStorage.nodeOffset;
		long valuePointer = SkipListUtils.getValuePointer(segment, offsetInSegment);

		return helpGetState(valuePointer);
	}

	/**
	 * Returns the byte arrays of serialized key and namespace.
	 *
	 * @param node the node.
	 * @return a tuple of byte arrays of serialized key and namespace
	 */
	Tuple2<byte[], byte[]> helpGetBytesForKeyAndNamespace(long node) {
		Node nodeStorage = getNodeSegmentAndOffset(node);
		MemorySegment segment = nodeStorage.nodeSegment;
		int offsetInSegment = nodeStorage.nodeOffset;

		int level = SkipListUtils.getLevel(segment, offsetInSegment);
		int keyDataOffset = offsetInSegment + SkipListUtils.getKeyDataOffset(level);

		return skipListKeySerializer.getSerializedKeyAndNamespace(segment, keyDataOffset);
	}

	/**
	 * Returns the byte array of serialized state.
	 *
	 * @param valuePointer pointer to value.
	 * @return byte array of serialized value.
	 */
	byte[] helpGetBytesForState(long valuePointer) {
		Node node = getNodeSegmentAndOffset(valuePointer);
		MemorySegment segment = node.nodeSegment;
		int offsetInSegment = node.nodeOffset;

		int valueLen = SkipListUtils.getValueLen(segment, offsetInSegment);
		MemorySegment valueSegment = MemorySegmentFactory.allocateUnpooledSegment(valueLen);
		segment.copyTo(offsetInSegment + SkipListUtils.getValueMetaLen(), valueSegment, 0 , valueLen);

		return valueSegment.getArray();
	}

	/**
	 * Returns the key of the node.
	 */
	private K helpGetKey(long node) {
		Node nodeStorage = getNodeSegmentAndOffset(node);
		MemorySegment segment = nodeStorage.nodeSegment;
		int offsetInSegment = nodeStorage.nodeOffset;

		int level = SkipListUtils.getLevel(segment, offsetInSegment);
		int keyDataLen = SkipListUtils.getKeyLen(segment, offsetInSegment);
		int keyDataOffset = offsetInSegment + SkipListUtils.getKeyDataOffset(level);

		return skipListKeySerializer.deserializeKey(segment, keyDataOffset, keyDataLen);
	}

	/**
	 * Return the state pointed by the given pointer. The value will be de-serialized
	 * with the given serializer.
	 */
	S helpGetState(long valuePointer, SkipListValueSerializer<S> serializer) {
		if (valuePointer == NIL_VALUE_POINTER) {
			return null;
		}

		Node node = getNodeSegmentAndOffset(valuePointer);
		MemorySegment segment = node.nodeSegment;
		int offsetInSegment = node.nodeOffset;

		int valueLen = SkipListUtils.getValueLen(segment, offsetInSegment);
		if (valueLen == 0) {
			// it is a removed key
			return null;
		}

		return serializer.deserializeState(segment,
			offsetInSegment + SkipListUtils.getValueMetaLen(), valueLen);
	}

	/**
	 * Return the state pointed by the given pointer. The serializer used is the
	 * {@link #skipListValueSerializer}. Because serializer is not thread safe, so
	 * this method should only be called in the state map synchronously.
	 */
	S helpGetState(long valuePointer) {
		return helpGetState(valuePointer, skipListValueSerializer);
	}

	/**
	 * Returns the state entry of the node.
	 */
	private StateEntry<K, N, S> helpGetStateEntry(long node) {
		Node nodeStorage = getNodeSegmentAndOffset(node);
		MemorySegment segment = nodeStorage.nodeSegment;
		int offsetInSegment = nodeStorage.nodeOffset;

		int level = SkipListUtils.getLevel(segment, offsetInSegment);
		int keyDataLen = SkipListUtils.getKeyLen(segment, offsetInSegment);
		int keyDataOffset = offsetInSegment + SkipListUtils.getKeyDataOffset(level);

		K key = skipListKeySerializer.deserializeKey(segment, keyDataOffset, keyDataLen);
		N namespace = skipListKeySerializer.deserializeNamespace(segment, keyDataOffset, keyDataLen);
		long valuePointer = SkipListUtils.getValuePointer(segment, offsetInSegment);
		S state = helpGetState(valuePointer);

		return new StateEntry.SimpleStateEntry<>(key, namespace, state);
	}

	// ----------------------------------------------------------------------------------

	@Override
	public Stream<K> getKeys(N namespace) {
		updateStat();
		MemorySegment namespaceSegment = skipListKeySerializer.serializeNamespaceToSegment(namespace);
		Iterator<Long> nodeIter = new NamespaceNodeIterator(namespaceSegment, 0, namespaceSegment.size());
		return StreamSupport.stream(Spliterators.spliteratorUnknownSize(nodeIter, 0), false)
			.map(this::helpGetKey);
	}

	@SuppressWarnings("unchecked")
	@Override
	public int sizeOfNamespace(Object namespace) {
		updateStat();
		MemorySegment namespaceSegment = skipListKeySerializer.serializeNamespaceToSegment((N) namespace);
		Iterator<Long> nodeIter = new NamespaceNodeIterator(namespaceSegment, 0, namespaceSegment.size());
		int size = 0;
		while (nodeIter.hasNext()) {
			nodeIter.next();
			size++;
		}

		return size;
	}

	@Nonnull
	@Override
	public Iterator<StateEntry<K, N, S>> iterator() {
		updateStat();
		final Iterator<Long> nodeIter = new NodeIterator();
		return new Iterator<StateEntry<K, N, S>>() {
			@Override
			public boolean hasNext() {
				return nodeIter.hasNext();
			}

			@Override
			public StateEntry<K, N, S> next() {
				return helpGetStateEntry(nodeIter.next());
			}
		};
	}

	@Override
	public InternalKvState.StateIncrementalVisitor<K, N, S> getStateIncrementalVisitor(int recommendedMaxNumberOfReturnedRecords) {
		return new StateIncrementalVisitor(recommendedMaxNumberOfReturnedRecords);
	}

	@Nonnull
	@Override
	public CopyOnWriteSkipListStateMapSnapshot<K, N, S> stateSnapshot() {
		tryToDeleteNodesPhysically();

		ResourceGuard.Lease lease;
		try {
			lease = resourceGuard.acquireResource();
		} catch (Exception e) {
			throw new RuntimeException("Acquire resource failed, and can't make snapshot of state map", e);
		}

		synchronized (snapshotVersions) {
			// increase the map version for copy-on-write and register the snapshot
			if (++stateMapVersion < 0) {
				// this is just a safety net against overflows, but should never happen in practice (i.e., only after 2^31 snapshots)
				throw new IllegalStateException("Version count overflow. Enforcing restart.");
			}

			highestRequiredSnapshotVersionPlusOne = stateMapVersion;
			snapshotVersions.add(highestRequiredSnapshotVersionPlusOne);
		}

		return new CopyOnWriteSkipListStateMapSnapshot<>(this, lease);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void releaseSnapshot(StateMapSnapshot<K, N, S, ? extends StateMap<K, N, S>> snapshotToRelease) {
		CopyOnWriteSkipListStateMapSnapshot snapshot = (CopyOnWriteSkipListStateMapSnapshot) snapshotToRelease;
		int snapshotVersion = snapshot.getSnapshotVersion();

		Preconditions.checkArgument(snapshot.isOwner(this),
			"Cannot release snapshot which is owned by a different state map.");

		synchronized (snapshotVersions) {
			Preconditions.checkState(snapshotVersions.remove(snapshotVersion), "Attempt to release unknown snapshot version");
			highestRequiredSnapshotVersionPlusOne = snapshotVersions.isEmpty() ? 0 : snapshotVersions.last();
			highestFinishedSnapshotVersion = snapshotVersions.isEmpty() ? stateMapVersion : snapshotVersions.first() - 1;
		}
	}

	LevelIndexHeader getLevelIndexHeader() {
		return levelIndexHeader;
	}

	int getStateMapVersion() {
		return stateMapVersion;
	}

	@VisibleForTesting
	int getHighestRequiredSnapshotVersionPlusOne() {
		return highestRequiredSnapshotVersionPlusOne;
	}

	@VisibleForTesting
	int getHighestFinishedSnapshotVersion() {
		return highestFinishedSnapshotVersion;
	}

	@VisibleForTesting
	Set<Integer> getSnapshotVersions() {
		return snapshotVersions;
	}

	@VisibleForTesting
	Set<Long> getLogicallyRemovedNodes() {
		return logicallyRemovedNodes;
	}

	@VisibleForTesting
	Set<Long> getPruningValueNodes() {
		return pruningValueNodes;
	}

	@VisibleForTesting
	ResourceGuard getResourceGuard() {
		return resourceGuard;
	}

	boolean isClosed() {
		return closed.get();
	}

	@Override
	public void close() {
		if (!closed.compareAndSet(false, true)) {
			LOG.warn("State map has been closed");
			return;
		}

		// wait for all running snapshots finished
		resourceGuard.close();

		releaseAllResource();
	}

	/**
	 * Iterates all nodes in the skip list.
	 */
	class NodeIterator implements Iterator<Long> {

		private long nextNode;

		NodeIterator() {
			this.nextNode = getNextNode(HEAD_NODE);
		}

		private long getNextNode(long node) {
			long n = helpGetNextNode(node, 0);
			while (n != NIL_NODE && isNodeRemoved(n)) {
				n = helpGetNextNode(n, 0);
			}

			return n;
		}

		@Override
		public boolean hasNext() {
			return nextNode != NIL_NODE;
		}

		@Override
		public Long next() {
			if (!hasNext()) {
				throw new NoSuchElementException();
			}

			long node = nextNode;
			nextNode = getNextNode(node);

			return node;
		}
	}

	private Node getNodeSegmentAndOffset(long node) {
		Chunk nodeChunk = spaceAllocator.getChunkById(SpaceUtils.getChunkIdByAddress(node));
		int offsetInNodeChunk = SpaceUtils.getChunkOffsetByAddress(node);
		MemorySegment nodeSegment = nodeChunk.getMemorySegment(offsetInNodeChunk);
		int offsetInNodeSegment = nodeChunk.getOffsetInSegment(offsetInNodeChunk);
		return new Node(nodeSegment, offsetInNodeSegment);
	}

	/**
	 * Iterates nodes with the given namespace.
	 */
	class NamespaceNodeIterator implements Iterator<Long> {

		private final MemorySegment namespaceSegment;
		private final int namespaceOffset;
		private final int namespaceLen;
		private long nextNode;

		NamespaceNodeIterator(MemorySegment namespaceSegment, int namespaceOffset, int namespaceLen) {
			this.namespaceSegment = namespaceSegment;
			this.namespaceOffset = namespaceOffset;
			this.namespaceLen = namespaceLen;
			this.nextNode = getFirstNodeWithNamespace(namespaceSegment, namespaceOffset, namespaceLen);
		}

		private long getNextNode(long node) {
			long n = helpGetNextNode(node, 0);
			while (n != NIL_NODE && isNodeRemoved(n)) {
				n = helpGetNextNode(n, 0);
			}

			if (n != NIL_NODE &&
				compareNamespaceAndNode(namespaceSegment, namespaceOffset, namespaceLen, n) == 0) {
				return n;
			}

			return NIL_NODE;
		}

		@Override
		public boolean hasNext() {
			return nextNode != NIL_NODE;
		}

		@Override
		public Long next() {
			if (!hasNext()) {
				throw new NoSuchElementException();
			}

			long node = nextNode;
			nextNode = getNextNode(node);

			return node;
		}
	}

	class StateIncrementalVisitor implements InternalKvState.StateIncrementalVisitor<K, N , S> {

		private final int recommendedMaxNumberOfReturnedRecords;
		private MemorySegment nextKeySegment;
		private int nextKeyOffset;
		private final Collection<StateEntry<K, N, S>> entryToReturn = new ArrayList<>(5);

		StateIncrementalVisitor(int recommendedMaxNumberOfReturnedRecords) {
			this.recommendedMaxNumberOfReturnedRecords = recommendedMaxNumberOfReturnedRecords;
			init();
		}

		private void init() {
			long node = getNextNode(HEAD_NODE);
			if (node != NIL_NODE) {
				setKeySegment(node);
			}
		}

		private long findNextNode(MemorySegment segment, int offset) {
			long node = findPredecessor(segment, offset, 0);
			return getNextNode(node);
		}

		private long getNextNode(long node) {
			long n = helpGetNextNode(node, 0);
			while (n != NIL_NODE && isNodeRemoved(n)) {
				n = helpGetNextNode(n, 0);
			}

			return n;
		}

		private void updateNextKeySegment(long node) {
			if (node != NIL_NODE) {
				node = getNextNode(node);
				if (node != NIL_NODE) {
					setKeySegment(node);
					return;
				}
			}
			nextKeySegment = null;
		}

		private void setKeySegment(long node) {
			Node nodeStorage = getNodeSegmentAndOffset(node);
			MemorySegment segment = nodeStorage.nodeSegment;
			int offsetInSegment = nodeStorage.nodeOffset;

			int level = SkipListUtils.getLevel(segment, offsetInSegment);
			int keyLen = SkipListUtils.getKeyLen(segment, offsetInSegment);
			int keyDataOffset = offsetInSegment + SkipListUtils.getKeyDataOffset(level);

			MemorySegment nextKeySegment = MemorySegmentFactory.allocateUnpooledSegment(keyLen);
			segment.copyTo(keyDataOffset, nextKeySegment, 0, keyLen);
			this.nextKeySegment = nextKeySegment;
			nextKeyOffset = 0;
		}

		@Override
		public boolean hasNext() {
			// visitor may be held by the external for a long time, and the map
			// can be closed between two nextEntries(), so check the status of map
			return !isClosed() && nextKeySegment != null &&
				findNextNode(nextKeySegment, nextKeyOffset) != NIL_NODE;
		}

		@Override
		public Collection<StateEntry<K, N, S>> nextEntries() {
			if (nextKeySegment == null) {
				return Collections.emptyList();
			}

			long node = findNextNode(nextKeySegment, nextKeyOffset);
			if (node == NIL_NODE) {
				nextKeySegment = null;
				return Collections.emptyList();
			}

			entryToReturn.clear();
			entryToReturn.add(helpGetStateEntry(node));
			int n = 1;
			while (n < recommendedMaxNumberOfReturnedRecords) {
				node = getNextNode(node);
				if (node == NIL_NODE) {
					break;
				}
				entryToReturn.add(helpGetStateEntry(node));
				n++;
			}

			updateNextKeySegment(node);

			return entryToReturn;
		}

		@Override
		public void remove(StateEntry<K, N, S> stateEntry) {
			CopyOnWriteSkipListStateMap.this.remove(stateEntry.getKey(), stateEntry.getNamespace());
		}

		@Override
		public void update(StateEntry<K, N, S> stateEntry, S newValue) {
			CopyOnWriteSkipListStateMap.this.put(stateEntry.getKey(), stateEntry.getNamespace(), newValue);
		}
	}

	/**
	 * Iterate versions of the given node.
	 */
	private class ValueVersionIterator implements Iterator<Integer> {
		private long valuePointer;

		ValueVersionIterator(long node) {
			valuePointer = SkipListUtils.helpGetValuePointer(node, spaceAllocator);
		}

		@Override
		public boolean hasNext() {
			return valuePointer != NIL_VALUE_POINTER;
		}

		@Override
		public Integer next() {
			int version = SkipListUtils.helpGetValueVersion(valuePointer, spaceAllocator);
			valuePointer = SkipListUtils.helpGetNextValuePointer(valuePointer, spaceAllocator);
			return version;
		}

		long getValuePointer() {
			return valuePointer;
		}
	}

	/**
	 * Encapsulation of skip list iterate and process result.
	 */
	private class SkipListIterateAndProcessResult {
		long prevNode;
		long currentNode;
		boolean isKeyFound;
		S state;
		SkipListIterateAndProcessResult(long prevNode, long currentNode, boolean isKeyFound, S state) {
			this.prevNode = prevNode;
			this.currentNode = currentNode;
			this.isKeyFound = isKeyFound;
			this.state = state;
		}
	}

	/**
	 * Encapsulation of skip list node pointers.
	 */
	private class SkipListNodePointers {
		long prevNode;
		long currentNode;
		long nextNode;
		SkipListNodePointers(long prevNode, long currentNode, long nextNode) {
			this.prevNode = prevNode;
			this.currentNode = currentNode;
			this.nextNode = nextNode;
		}
	}

	/**
	 * Encapsulation of the storage of the node.
	 */
	private class Node {
		MemorySegment nodeSegment;
		int nodeOffset;
		Node(MemorySegment nodeSegment, int nodeOffset) {
			this.nodeSegment = nodeSegment;
			this.nodeOffset = nodeOffset;
		}
	}

}
