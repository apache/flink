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
import org.apache.flink.core.memory.ByteBufferUtils;
import org.apache.flink.runtime.state.StateEntry;
import org.apache.flink.runtime.state.StateTransformationFunction;
import org.apache.flink.runtime.state.heap.space.Allocator;
import org.apache.flink.runtime.state.heap.space.Chunk;
import org.apache.flink.runtime.state.heap.space.SpaceUtils;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.ResourceGuard;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.nio.ByteBuffer;
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
public class CopyOnWriteSkipListStateMap<K, N, S> extends StateMap<K, N, S> implements AutoCloseable {

	private static final Logger LOG = LoggerFactory.getLogger(CopyOnWriteSkipListStateMap.class);

	/**
	 * Default max number of logically-removed keys to delete one time.
	 */
	private static final int DEFAULT_MAX_KEYS_TO_DELETE_ONE_TIME = 3;

	/**
	 * Default ratio of the logically-removed keys to trigger deletion when snapshot.
	 */
	private static final float DEFAULT_LOGICAL_REMOVED_KEYS_RATIO = 0.2f;

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
	private int stateMapVersion;

	/**
	 * The highest version of this map that is still required by any unreleased snapshot.
	 */
	private int highestRequiredSnapshotVersion;

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
			@Nonnull Allocator spaceAllocator) {
		this(keySerializer, namespaceSerializer, stateSerializer, spaceAllocator,
			DEFAULT_MAX_KEYS_TO_DELETE_ONE_TIME, DEFAULT_LOGICAL_REMOVED_KEYS_RATIO);
	}

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
		this.highestRequiredSnapshotVersion = 0;
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
	public int totalSize() {
		return totalSize;
	}

	public int getRequestCount() {
		return requestCount;
	}

	@Override
	public S get(K key, N namespace) {
		updateStat();

		long node = getNodeInternal(key, namespace);

		if (node == NIL_NODE) {
			return null;
		}

		return helpGetNodeState(node);
	}

	@Override
	public boolean containsKey(K key, N namespace) {
		updateStat();

		long node = getNodeInternal(key, namespace);

		return node != NIL_NODE;
	}

	@Override
	public void put(K key, N namespace, S state) {
		updateStat();
		ByteBuffer keyByteBuffer = getKeyByteBuffer(key, namespace);
		int keyLen = keyByteBuffer.limit();
		byte[] value = skipListValueSerializer.serialize(state);

		putNode(keyByteBuffer, 0, keyLen, value, false);
	}

	@Override
	public S putAndGetOld(K key, N namespace, S state) {
		updateStat();
		ByteBuffer keyByteBuffer = getKeyByteBuffer(key, namespace);
		int keyLen = keyByteBuffer.limit();
		byte[] value = skipListValueSerializer.serialize(state);

		return putNode(keyByteBuffer, 0, keyLen, value, true);
	}

	@Override
	public void remove(K key, N namespace) {
		updateStat();
		ByteBuffer keyByteBuffer = getKeyByteBuffer(key, namespace);
		int keyLen = keyByteBuffer.limit();

		removeNode(keyByteBuffer, 0, keyLen, false);
	}

	@Override
	public S removeAndGetOld(K key, N namespace) {
		updateStat();
		ByteBuffer keyByteBuffer = getKeyByteBuffer(key, namespace);
		int keyLen = keyByteBuffer.limit();

		return removeNode(keyByteBuffer, 0, keyLen, true);
	}

	@Override
	public <T> void transform(
		K key,
		N namespace,
		T value,
		StateTransformationFunction<S, T> transformation) throws Exception {
		updateStat();
		ByteBuffer keyByteBuffer = getKeyByteBuffer(key, namespace);
		int keyLen = keyByteBuffer.limit();

		long node = getNode(keyByteBuffer, 0, keyLen);
		S oldState = node == NIL_NODE ? null : helpGetNodeState(node);
		S newState = transformation.apply(oldState, value);
		byte[] stateBytes = skipListValueSerializer.serialize(newState);
		putNode(keyByteBuffer, 0, keyLen, stateBytes, false);
	}

	// Detail implementation methods ---------------------------------------------------------------

	/**
	 * Find the node containing the given key.
	 *
	 * @param keyByteBuffer byte buffer storing the key.
	 * @param keyOffset     offset of the key.
	 * @param keyLen        length of the key.
	 * @return id of the node. NIL_NODE will be returned if key does no exist.
	 */
	@VisibleForTesting
	long getNode(ByteBuffer keyByteBuffer, int keyOffset, int keyLen) {
		int deleteCount = 0;
		long prevNode = findPredecessor(keyByteBuffer, keyOffset, 1);
		long currentNode  = helpGetNextNode(prevNode, 0);
		long nextNode;

		int c;
		while (currentNode != NIL_NODE) {
			nextNode = helpGetNextNode(currentNode, 0);

			boolean isRemoved = isNodeRemoved(currentNode);
			if (isRemoved) {
				// remove the node physically when there is no snapshot running
				if (highestRequiredSnapshotVersion == 0 && deleteCount < numKeysToDeleteOneTime) {
					doPhysicalRemove(currentNode, prevNode, nextNode);
					logicallyRemovedNodes.remove(currentNode);
					totalSize--;
					deleteCount++;
				} else {
					prevNode = currentNode;
				}
				currentNode = nextNode;
				continue;
			}

			c = compareByteBufferAndNode(keyByteBuffer, keyOffset, keyLen, currentNode);

			// find the key
			if (c == 0) {
				return currentNode;
			}

			// the key is less than the current node, and nodes after current
			// node can not be equal to the key.
			if (c < 0) {
				break;
			}

			prevNode = currentNode;
			currentNode = helpGetNextNode(currentNode, 0);
		}

		return NIL_NODE;
	}

	/**
	 * Put the key into the skip list. If the key does not exist before, a new node
	 * will be created. If the key exists before, the old state will be returned.
	 *
	 * @param keyByteBuffer  byte buffer storing the key.
	 * @param keyOffset      offset of the key.
	 * @param keyLen         length of the key.
	 * @param value          the value.
	 * @param returnOldState whether to return old state.
	 * @return the old state. Null will be returned if key does not exist or returnOldState is false.
	 */
	@VisibleForTesting
	S putNode(ByteBuffer keyByteBuffer, int keyOffset, int keyLen, byte[] value, boolean returnOldState) {
		int deleteCount = 0;
		long prevNode = findPredecessor(keyByteBuffer, keyOffset, 1);
		long currentNode = helpGetNextNode(prevNode, 0);
		long nextNode;

		int c;
		for ( ; ; ) {
			if (currentNode != NIL_NODE) {
				nextNode = helpGetNextNode(currentNode, 0);

				boolean isRemoved = isNodeRemoved(currentNode);

				// note that the key may be put again before the node is physically removed,
				// so the node still need to compare with the key although they can not be
				// physically removed here
				if (isRemoved && highestRequiredSnapshotVersion == 0 && deleteCount < numKeysToDeleteOneTime) {
					doPhysicalRemove(currentNode, prevNode, nextNode);
					logicallyRemovedNodes.remove(currentNode);
					totalSize--;
					deleteCount++;
					currentNode = nextNode;
					continue;
				}

				c = compareByteBufferAndNode(keyByteBuffer, keyOffset, keyLen, currentNode);

				if (c > 0) {
					prevNode = currentNode;
					currentNode = nextNode;
					continue;
				}

				if (c == 0) {
					int version = SkipListUtils.helpGetNodeLatestVersion(currentNode, spaceAllocator);
					boolean needCopyOnWrite = version < highestRequiredSnapshotVersion;
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
			}

			// if current node is NIL_NODE or larger than the key, a new node will be inserted
			break;
		}

		int level = getRandomIndexLevel();
		levelIndexHeader.updateLevel(level);

		int totalMetaKeyLen = SkipListUtils.getKeyMetaLen(level) + keyLen;
		long node = this.spaceAllocator.allocate(totalMetaKeyLen);

		int totalValueLen = SkipListUtils.getValueMetaLen() + value.length;
		long valuePointer = spaceAllocator.allocate(totalValueLen);

		doWriteKey(node, level, keyByteBuffer, keyOffset, keyLen, valuePointer, currentNode);
		doWriteValue(valuePointer, value, stateMapVersion, node, NIL_VALUE_POINTER);

		helpSetNextNode(prevNode, node, 0);

		if (level > 0) {
			SkipListUtils.buildLevelIndex(node, level, keyByteBuffer, keyOffset, levelIndexHeader, spaceAllocator);
		}

		totalSize++;

		return null;
	}

	/**
	 * Remove the key from the skip list. The key can be removed logically or physically.
	 * Logical remove means put a null value whose size is 0. If the key exists before,
	 * the old value state will be returned.
	 *
	 * @param keyByteBuffer  byte buffer storing the key.
	 * @param keyOffset      offset of the key.
	 * @param keyLen         length of the key.
	 * @param returnOldState whether to return old state.
	 * @return the old state. Null will be returned if key does not exist or returnOldState is false.
	 */
	private S removeNode(ByteBuffer keyByteBuffer, int keyOffset, int keyLen, boolean returnOldState) {
		int deleteCount = 0;
		long prevNode = findPredecessor(keyByteBuffer, keyOffset, 1);
		long currentNode = helpGetNextNode(prevNode, 0);
		long nextNode;

		int c;
		while (currentNode != NIL_NODE) {
			nextNode = helpGetNextNode(currentNode, 0);

			boolean isRemoved = isNodeRemoved(currentNode);
			if (isRemoved && highestRequiredSnapshotVersion == 0 && deleteCount < numKeysToDeleteOneTime) {
				doPhysicalRemove(currentNode, prevNode, nextNode);
				logicallyRemovedNodes.remove(currentNode);
				currentNode = nextNode;
				totalSize--;
				deleteCount++;
				continue;
			}

			c = compareByteBufferAndNode(keyByteBuffer, keyOffset, keyLen, currentNode);

			if (c < 0) {
				break;
			}

			if (c > 0) {
				prevNode = currentNode;
				currentNode = nextNode;
				continue;
			}

			// if the node has been logically removed, and can not be physically
			// removed here, just return null
			if (isRemoved && highestRequiredSnapshotVersion != 0) {
				return null;
			}

			long oldValuePointer;
			boolean oldValueNeedFree;

			if (highestRequiredSnapshotVersion == 0) {
				// do physically remove only when there is no snapshot running
				oldValuePointer = doPhysicalRemoveAndGetValue(currentNode, prevNode, nextNode);
				// the node has been logically removed, and remove it from the set
				if (isRemoved) {
					logicallyRemovedNodes.remove(currentNode);
				}
				oldValueNeedFree = true;
				totalSize--;
			} else {
				int version = SkipListUtils.helpGetNodeLatestVersion(currentNode, spaceAllocator);
				if (version < highestRequiredSnapshotVersion) {
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

		return null;
	}

	/**
	 * Find the predecessor node for the given key at the given level.
	 * The key is in the byte buffer positioning at the given offset.
	 *
	 * @param keyByteBuffer byte buffer which contains the key.
	 * @param keyOffset     offset of the key in the byte buffer.
	 * @param level         the level.
	 * @return node id before the key at the given level.
	 */
	private long findPredecessor(ByteBuffer keyByteBuffer, int keyOffset, int level) {
		return SkipListUtils.findPredecessor(keyByteBuffer, keyOffset, level, levelIndexHeader, spaceAllocator);
	}

	/**
	 * Compare the first skip list key in the given byte buffer with the second skip list key in the given node.
	 *
	 * @param keyByteBuffer byte buffer storing the first key.
	 * @param keyOffset     offset of the first key in byte buffer.
	 * @param keyLen        length of the first key.
	 * @param targetNode    the node storing the second key.
	 * @return Returns a negative integer, zero, or a positive integer as the first key is less than,
	 * equal to, or greater than the second.
	 */
	private int compareByteBufferAndNode(ByteBuffer keyByteBuffer, int keyOffset, int keyLen, long targetNode) {
		return SkipListUtils.compareByteBufferAndNode(keyByteBuffer, keyOffset, targetNode, spaceAllocator);
	}

	/**
	 * Compare the first namespace in the given byte buffer with the second namespace in the given node.
	 *
	 * @param namespaceByteBuffer byte buffer storing the first namespace.
	 * @param namespaceOffset     offset of the first namespace in byte buffer.
	 * @param namespaceLen        length of the first namespace.
	 * @param targetNode          the node storing the second namespace.
	 * @return Returns a negative integer, zero, or a positive integer as the first key is less than,
	 * equal to, or greater than the second.
	 */
	private int compareNamespaceAndNode(ByteBuffer namespaceByteBuffer, int namespaceOffset, int namespaceLen, long targetNode) {
		Chunk chunk = spaceAllocator.getChunkById(SpaceUtils.getChunkIdByAddress(targetNode));
		int offsetInChunk = SpaceUtils.getChunkOffsetByAddress(targetNode);
		ByteBuffer targetKeyByteBuffer = chunk.getByteBuffer(offsetInChunk);
		int offsetInByteBuffer = chunk.getOffsetInByteBuffer(offsetInChunk);

		int level = SkipListUtils.getLevel(targetKeyByteBuffer, offsetInByteBuffer);
		int targetKeyOffset = offsetInByteBuffer + SkipListUtils.getKeyDataOffset(level);

		return SkipListKeyComparator.compareNamespaceAndNode(namespaceByteBuffer, namespaceOffset, namespaceLen,
			targetKeyByteBuffer, targetKeyOffset);
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
		long valuePointer = spaceAllocator.allocate(totalValueLen);

		Chunk nodeChunk = spaceAllocator.getChunkById(SpaceUtils.getChunkIdByAddress(node));
		int offsetInNodeChunk = SpaceUtils.getChunkOffsetByAddress(node);
		ByteBuffer nodeByteBuffer = nodeChunk.getByteBuffer(offsetInNodeChunk);
		int offsetInNodeByteBuffer = nodeChunk.getOffsetInByteBuffer(offsetInNodeChunk);
		long oldValuePointer = SkipListUtils.getValuePointer(nodeByteBuffer, offsetInNodeByteBuffer);

		doWriteValue(valuePointer, value, stateMapVersion, node, oldValuePointer);

		// update value pointer in node after the new value has points the older value so that
		// old value can be accessed concurrently
		SkipListUtils.putValuePointer(nodeByteBuffer, offsetInNodeByteBuffer, valuePointer);

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
		long valuePointer = spaceAllocator.allocate(totalValueLen);

		Chunk nodeChunk = spaceAllocator.getChunkById(SpaceUtils.getChunkIdByAddress(node));
		int offsetInNodeChunk = SpaceUtils.getChunkOffsetByAddress(node);
		ByteBuffer nodeByteBuffer = nodeChunk.getByteBuffer(offsetInNodeChunk);
		int offsetInNodeByteBuffer = nodeChunk.getOffsetInByteBuffer(offsetInNodeChunk);

		long oldValuePointer = SkipListUtils.getValuePointer(nodeByteBuffer, offsetInNodeByteBuffer);
		long nextValuePointer = SkipListUtils.helpGetNextValuePointer(oldValuePointer, spaceAllocator);

		doWriteValue(valuePointer, value, stateMapVersion, node, nextValuePointer);

		// update value pointer in node after the new value has points the older value so that
		// old value can be accessed concurrently
		SkipListUtils.putValuePointer(nodeByteBuffer, offsetInNodeByteBuffer, valuePointer);

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
		// set next node of prevNode at level 0 to nextNode
		helpSetNextNode(prevNode, nextNode, 0);

		// remove the level index for the node
		SkipListUtils.removeLevelIndex(node, spaceAllocator, levelIndexHeader);

		// free space used by key and value
		long valuePointer = SkipListUtils.helpGetValuePointer(node, spaceAllocator);
		this.spaceAllocator.free(node);
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
		// set next node of prevNode at level 0 to nextNode
		helpSetNextNode(prevNode, nextNode, 0);

		// remove the level index for the node
		SkipListUtils.removeLevelIndex(node, spaceAllocator, levelIndexHeader);

		// free space used by key and value
		long valuePointer = SkipListUtils.helpGetValuePointer(node, spaceAllocator);
		long nextValuePointer = SkipListUtils.helpGetNextValuePointer(valuePointer, spaceAllocator);
		this.spaceAllocator.free(node);
		SkipListUtils.removeAllValues(nextValuePointer, spaceAllocator);

		return valuePointer;
	}

	/**
	 * Return a random level for new node.
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
	 * @param keyByteBuffer byte buffer storing the key.
	 * @param keyOffset     offset of key in byte buffer.
	 * @param keyLen        length of the key.
	 * @param valuePointer  pointer to value.
	 * @param nextNode      next node on level 0.
	 */
	private void doWriteKey(
		long node,
		int level,
		ByteBuffer keyByteBuffer,
		int keyOffset,
		int keyLen,
		long valuePointer,
		long nextNode) {
		Chunk chunk = this.spaceAllocator.getChunkById(SpaceUtils.getChunkIdByAddress(node));
		int offsetInChunk = SpaceUtils.getChunkOffsetByAddress(node);
		ByteBuffer bb = chunk.getByteBuffer(offsetInChunk);
		int offsetInByteBuffer = chunk.getOffsetInByteBuffer(offsetInChunk);

		SkipListUtils.putLevelAndNodeStatus(bb, offsetInByteBuffer, level, NodeStatus.PUT);
		SkipListUtils.putKeyLen(bb, offsetInByteBuffer, keyLen);
		SkipListUtils.putValuePointer(bb, offsetInByteBuffer, valuePointer);
		SkipListUtils.putNextKeyPointer(bb, offsetInByteBuffer, nextNode);
		SkipListUtils.putKeyData(bb, offsetInByteBuffer, keyByteBuffer, keyOffset, keyLen, level);
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
		Chunk chunk = this.spaceAllocator.getChunkById(SpaceUtils.getChunkIdByAddress(valuePointer));
		int offsetInChunk = SpaceUtils.getChunkOffsetByAddress(valuePointer);
		ByteBuffer bb = chunk.getByteBuffer(offsetInChunk);
		int offsetInByteBuffer = chunk.getOffsetInByteBuffer(offsetInChunk);

		SkipListUtils.putValueVersion(bb, offsetInByteBuffer, version);
		SkipListUtils.putKeyPointer(bb, offsetInByteBuffer, keyPointer);
		SkipListUtils.putNextValuePointer(bb, offsetInByteBuffer, nextValuePointer);
		SkipListUtils.putValueLen(bb, offsetInByteBuffer, value == null ? 0 : value.length);
		if (value != null) {
			SkipListUtils.putValueData(bb, offsetInByteBuffer, value);
		}
	}

	/**
	 * Find the first node with the given namespace at level 0.
	 *
	 * @param namespaceByteBuffer byte buffer storing the namespace.
	 * @param namespaceOffset     offset of the namespace.
	 * @param namespaceLen        length of the namespace.
	 * @return the first node with the given namespace.
	 *  NIL_NODE will be returned if not exist.
	 */
	private long getFirstNodeWithNamespace(ByteBuffer namespaceByteBuffer, int namespaceOffset, int namespaceLen) {
		int currentLevel = levelIndexHeader.getLevel();
		long prevNode = HEAD_NODE;
		long currentNode = helpGetNextNode(prevNode, currentLevel);

		int c;
		// find the predecessor node at level 0.
		for ( ; ; ) {
			if (currentNode != NIL_NODE) {
				c = compareNamespaceAndNode(namespaceByteBuffer, namespaceOffset, namespaceLen, currentNode);
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

			c = compareNamespaceAndNode(namespaceByteBuffer, namespaceOffset, namespaceLen, currentNode);
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
		if (highestRequiredSnapshotVersion != 0) {
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
			totalSize--;
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
	 * @return the value pointer used by the given snapshot. NIL_VALUE_POINTER
	 * 	will be returned if there is no value for this snapshot.
	 */
	long getValueForSnapshot(long node, int snapshotVersion) {
		long snapshotValuePointer = NIL_VALUE_POINTER;
		long valuePointer = SkipListUtils.helpGetValuePointer(node, spaceAllocator);

		while (valuePointer != NIL_VALUE_POINTER) {
			int version = SkipListUtils.helpGetValueVersion(valuePointer, spaceAllocator);

			// the first value whose version is less than snapshotVersion
			if (version < snapshotVersion) {
				snapshotValuePointer = valuePointer;
				break;
			}

			valuePointer = SkipListUtils.helpGetNextValuePointer(valuePointer, spaceAllocator);
		}

		return snapshotValuePointer;
	}

	/**
	 * Returns the value pointer used by the snapshot of the given version,
	 * and useless version values will be pruned.
	 *
	 * @param snapshotVersion version of snapshot.
	 * @return the value pointer used by the given snapshot. NIL_VALUE_POINTER
	 * 	will be returned if there is no value for this snapshot.
	 */
	long getAndPruneValueForSnapshot(long node, int snapshotVersion) {
		// whether the node is being pruned by some snapshot
		boolean isPruning = pruningValueNodes.add(node);
		try {
			long snapshotValuePointer = NIL_VALUE_POINTER;
			long valuePointer = SkipListUtils.helpGetValuePointer(node, spaceAllocator);
			while (valuePointer != NIL_VALUE_POINTER) {
				int version = SkipListUtils.helpGetValueVersion(valuePointer, spaceAllocator);

				// find the first value whose version is less than snapshotVersion
				if (version < snapshotVersion && snapshotValuePointer == NIL_VALUE_POINTER) {
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

				valuePointer = SkipListUtils.helpGetNextValuePointer(valuePointer, spaceAllocator);
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
	private long getNodeInternal(K key, N namespace) {
		ByteBuffer keyByteBuffer = getKeyByteBuffer(key, namespace);
		int keyLen = keyByteBuffer.limit();

		return getNode(keyByteBuffer, 0, keyLen);
	}

	/**
	 * Get the {@link ByteBuffer} wrapping up the serialized key bytes.
	 *
	 * @param key       the key.
	 * @param namespace the namespace.
	 * @return the {@link ByteBuffer} wrapping up the serialized key bytes.
	 */
	private ByteBuffer getKeyByteBuffer(K key, N namespace) {
		byte[] keyBytes = skipListKeySerializer.serialize(key, namespace);
		return ByteBuffer.wrap(keyBytes);
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
		Chunk chunk = this.spaceAllocator.getChunkById(SpaceUtils.getChunkIdByAddress(node));
		int offsetInChunk = SpaceUtils.getChunkOffsetByAddress(node);
		ByteBuffer bb = chunk.getByteBuffer(offsetInChunk);
		int offsetInByteBuffer = chunk.getOffsetInByteBuffer(offsetInChunk);
		NodeStatus oldStatus = SkipListUtils.getNodeStatus(bb, offsetInByteBuffer);
		if (oldStatus != newStatus) {
			int level = SkipListUtils.getLevel(bb, offsetInByteBuffer);
			SkipListUtils.putLevelAndNodeStatus(bb, offsetInByteBuffer, level, newStatus);
		}

		return oldStatus;
	}

	/**
	 * Return the state of the node. null will be returned if the node is removed.
	 */
	@VisibleForTesting
	S helpGetNodeState(long node) {
		Chunk chunk = spaceAllocator.getChunkById(SpaceUtils.getChunkIdByAddress(node));
		int offsetInChunk = SpaceUtils.getChunkOffsetByAddress(node);
		ByteBuffer byteBuffer = chunk.getByteBuffer(offsetInChunk);
		int offsetInByteBuffer = chunk.getOffsetInByteBuffer(offsetInChunk);
		long valuePointer = SkipListUtils.getValuePointer(byteBuffer, offsetInByteBuffer);

		return helpGetState(valuePointer);
	}

	/**
	 * Returns the byte arrays of serialized key and namespace.
	 *
	 * @param node the node.
	 * @return a tuple of byte arrays of serialized key and namespace
	 */
	Tuple2<byte[], byte[]> helpGetBytesForKeyAndNamespace(long node) {
		Chunk chunk = spaceAllocator.getChunkById(SpaceUtils.getChunkIdByAddress(node));
		int offsetInChunk = SpaceUtils.getChunkOffsetByAddress(node);
		ByteBuffer byteBuffer = chunk.getByteBuffer(offsetInChunk);
		int offsetInByteBuffer = chunk.getOffsetInByteBuffer(offsetInChunk);

		int level = SkipListUtils.getLevel(byteBuffer, offsetInByteBuffer);
		int keyDataOffset = offsetInByteBuffer + SkipListUtils.getKeyDataOffset(level);

		return skipListKeySerializer.getSerializedKeyAndNamespace(byteBuffer, keyDataOffset);
	}

	/**
	 * Returns the byte array of serialized state.
	 *
	 * @param valuePointer pointer to value.
	 * @return byte array of serialized value.
	 */
	byte[] helpGetBytesForState(long valuePointer) {
		Chunk chunk = spaceAllocator.getChunkById(SpaceUtils.getChunkIdByAddress(valuePointer));
		int offsetInChunk = SpaceUtils.getChunkOffsetByAddress(valuePointer);
		ByteBuffer bb = chunk.getByteBuffer(offsetInChunk);
		int offsetInByteBuffer = chunk.getOffsetInByteBuffer(offsetInChunk);

		int valueLen = SkipListUtils.getValueLen(bb, offsetInByteBuffer);
		byte[] valueBytes = new byte[valueLen];
		ByteBufferUtils.copyFromBufferToArray(bb, offsetInByteBuffer + SkipListUtils.getValueMetaLen(), valueBytes,
			0, valueLen);

		return valueBytes;
	}

	/**
	 * Returns the key of the node.
	 */
	private K helpGetKey(long node) {
		Chunk chunk = spaceAllocator.getChunkById(SpaceUtils.getChunkIdByAddress(node));
		int offsetInChunk = SpaceUtils.getChunkOffsetByAddress(node);
		ByteBuffer byteBuffer = chunk.getByteBuffer(offsetInChunk);
		int offsetInByteBuffer = chunk.getOffsetInByteBuffer(offsetInChunk);

		int level = SkipListUtils.getLevel(byteBuffer, offsetInByteBuffer);
		int keyDataLen = SkipListUtils.getKeyLen(byteBuffer, offsetInByteBuffer);
		int keyDataOffset = offsetInByteBuffer + SkipListUtils.getKeyDataOffset(level);

		return skipListKeySerializer.deserializeKey(byteBuffer, keyDataOffset, keyDataLen);
	}

	/**
	 * Return the state pointed by the given pointer. The value will be de-serialized
	 * with the given serializer.
	 */
	S helpGetState(long valuePointer, SkipListValueSerializer<S> serializer) {
		if (valuePointer == NIL_VALUE_POINTER) {
			return null;
		}

		Chunk chunk = spaceAllocator.getChunkById(SpaceUtils.getChunkIdByAddress(valuePointer));
		int offsetInChunk = SpaceUtils.getChunkOffsetByAddress(valuePointer);
		ByteBuffer bb = chunk.getByteBuffer(offsetInChunk);
		int offsetInByteBuffer = chunk.getOffsetInByteBuffer(offsetInChunk);

		int valueLen = SkipListUtils.getValueLen(bb, offsetInByteBuffer);
		if (valueLen == 0) {
			// it is a removed key
			return null;
		}

		return serializer.deserializeState(bb,
			offsetInByteBuffer + SkipListUtils.getValueMetaLen(), valueLen);
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
		Chunk chunk = spaceAllocator.getChunkById(SpaceUtils.getChunkIdByAddress(node));
		int offsetInChunk = SpaceUtils.getChunkOffsetByAddress(node);
		ByteBuffer byteBuffer = chunk.getByteBuffer(offsetInChunk);
		int offsetInByteBuffer = chunk.getOffsetInByteBuffer(offsetInChunk);

		int level = SkipListUtils.getLevel(byteBuffer, offsetInByteBuffer);
		int keyDataLen = SkipListUtils.getKeyLen(byteBuffer, offsetInByteBuffer);
		int keyDataOffset = offsetInByteBuffer + SkipListUtils.getKeyDataOffset(level);

		K key = skipListKeySerializer.deserializeKey(byteBuffer, keyDataOffset, keyDataLen);
		N namespace = skipListKeySerializer.deserializeNamespace(byteBuffer, keyDataOffset, keyDataLen);
		long valuePointer = SkipListUtils.getValuePointer(byteBuffer, offsetInByteBuffer);
		S state = helpGetState(valuePointer);

		return new StateEntry.SimpleStateEntry<>(key, namespace, state);
	}

	// ----------------------------------------------------------------------------------

	@Override
	public Stream<K> getKeys(N namespace) {
		updateStat();
		byte[] namespaceBytes = skipListKeySerializer.serializeNamespace(namespace);
		ByteBuffer namespaceByteBuffer = ByteBuffer.wrap(namespaceBytes);
		Iterator<Long> nodeIter = new NamespaceNodeIterator(namespaceByteBuffer, 0, namespaceBytes.length);
		return StreamSupport.stream(Spliterators.spliteratorUnknownSize(nodeIter, 0), false)
			.map(this::helpGetKey);
	}

	@SuppressWarnings("unchecked")
	@Override
	public int sizeOfNamespace(Object namespace) {
		updateStat();
		byte[] namespaceBytes = skipListKeySerializer.serializeNamespace((N) namespace);
		ByteBuffer namespaceByteBuffer = ByteBuffer.wrap(namespaceBytes);
		Iterator<Long> nodeIter = new NamespaceNodeIterator(namespaceByteBuffer, 0, namespaceBytes.length);
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
	public StateMapSnapshot<K, N, S, ? extends StateMap<K, N, S>> stateSnapshot() {
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

			highestRequiredSnapshotVersion = stateMapVersion;
			snapshotVersions.add(highestRequiredSnapshotVersion);
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
			highestRequiredSnapshotVersion = snapshotVersions.isEmpty() ? 0 : snapshotVersions.last();
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
	int getHighestRequiredSnapshotVersion() {
		return highestRequiredSnapshotVersion;
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

	/**
	 * Iterates nodes with the given namespace.
	 */
	class NamespaceNodeIterator implements Iterator<Long> {

		private final ByteBuffer namespaceByteBuffer;
		private final int namespaceOffset;
		private final int namespaceLen;
		private long nextNode;

		NamespaceNodeIterator(ByteBuffer namespaceByteBuffer, int namespaceOffset, int namespaceLen) {
			this.namespaceByteBuffer = namespaceByteBuffer;
			this.namespaceOffset = namespaceOffset;
			this.namespaceLen = namespaceLen;
			this.nextNode = getFirstNodeWithNamespace(namespaceByteBuffer, namespaceOffset, namespaceLen);
		}

		private long getNextNode(long node) {
			long n = helpGetNextNode(node, 0);
			while (n != NIL_NODE && isNodeRemoved(n)) {
				n = helpGetNextNode(n, 0);
			}

			if (n != NIL_NODE &&
				compareNamespaceAndNode(namespaceByteBuffer, namespaceOffset, namespaceLen, n) == 0) {
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
		private ByteBuffer nextKeyByteBuffer;
		private int nextKeyOffset;
		private final Collection<StateEntry<K, N, S>> entryToReturn = new ArrayList<>(5);

		StateIncrementalVisitor(int recommendedMaxNumberOfReturnedRecords) {
			this.recommendedMaxNumberOfReturnedRecords = recommendedMaxNumberOfReturnedRecords;
			init();
		}

		private void init() {
			long node = getNextNode(HEAD_NODE);
			if (node != NIL_NODE) {
				setKeyByteBuffer(node);
			}
		}

		private long findNextNode(ByteBuffer byteBuffer, int offset) {
			long node = findPredecessor(byteBuffer, offset, 0);
			return getNextNode(node);
		}

		private long getNextNode(long node) {
			long n = helpGetNextNode(node, 0);
			while (n != NIL_NODE && isNodeRemoved(n)) {
				n = helpGetNextNode(n, 0);
			}

			return n;
		}

		private void updateNextKeyByteBuffer(long node) {
			if (node != NIL_NODE) {
				node = getNextNode(node);
				if (node != NIL_NODE) {
					setKeyByteBuffer(node);
					return;
				}
			}
			nextKeyByteBuffer = null;
		}

		private void setKeyByteBuffer(long node) {
			Chunk chunk = spaceAllocator.getChunkById(SpaceUtils.getChunkIdByAddress(node));
			int offsetInChunk = SpaceUtils.getChunkOffsetByAddress(node);
			ByteBuffer bb = chunk.getByteBuffer(offsetInChunk);
			int offsetInByteBuffer = chunk.getOffsetInByteBuffer(offsetInChunk);

			int level = SkipListUtils.getLevel(bb, offsetInByteBuffer);
			int keyLen = SkipListUtils.getKeyLen(bb, offsetInByteBuffer);
			int keyDataOffset = offsetInByteBuffer + SkipListUtils.getKeyDataOffset(level);

			ByteBuffer byteBuffer = ByteBuffer.allocate(keyLen);
			ByteBufferUtils.copyFromBufferToBuffer(bb, keyDataOffset, byteBuffer, 0, keyLen);
			nextKeyByteBuffer = byteBuffer;
			nextKeyOffset = 0;
		}

		@Override
		public boolean hasNext() {
			// visitor may be held by the external for a long time, and the map
			// can be closed between two nextEntries(), so check the status of map
			return !isClosed() && nextKeyByteBuffer != null &&
				findNextNode(nextKeyByteBuffer, nextKeyOffset) != NIL_NODE;
		}

		@Override
		public Collection<StateEntry<K, N, S>> nextEntries() {
			if (nextKeyByteBuffer == null) {
				return Collections.emptyList();
			}

			long node = findNextNode(nextKeyByteBuffer, nextKeyOffset);
			if (node == NIL_NODE) {
				nextKeyByteBuffer = null;
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

			updateNextKeyByteBuffer(node);

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

}
