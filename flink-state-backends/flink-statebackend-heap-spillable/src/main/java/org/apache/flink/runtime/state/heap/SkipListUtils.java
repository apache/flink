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

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.state.heap.space.Allocator;
import org.apache.flink.runtime.state.heap.space.Chunk;
import org.apache.flink.runtime.state.heap.space.SpaceUtils;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

/**
 * Utilities for skip list.
 */
@SuppressWarnings("WeakerAccess")
public class SkipListUtils {
	static final long NIL_NODE = -1;
	static final long HEAD_NODE = -2;
	static final long NIL_VALUE_POINTER = -1;
	static final int MAX_LEVEL = 255;
	static final int DEFAULT_LEVEL = 32;
	static final int BYTE_MASK = 0xFF;

	/**
	 * Key space schema.
	 * - key meta
	 * -- int: level & status
	 * --   byte 0: level of node in skip list
	 * --   byte 1: node status
	 * --   byte 2: preserve
	 * --   byte 3: preserve
	 * -- int: length of key
	 * -- long: pointer to the newest value
	 * -- long: pointer to next node on level 0
	 * -- long[]: array of pointers to next node on different levels excluding level 0
	 * -- long[]: array of pointers to previous node on different levels excluding level 0
	 * - byte[]: data of key
	 */
	static final int KEY_META_OFFSET = 0;
	static final int KEY_LEN_OFFSET = KEY_META_OFFSET + Integer.BYTES;
	static final int VALUE_POINTER_OFFSET = KEY_LEN_OFFSET + Integer.BYTES;
	static final int NEXT_KEY_POINTER_OFFSET = VALUE_POINTER_OFFSET + Long.BYTES;
	static final int LEVEL_INDEX_OFFSET = NEXT_KEY_POINTER_OFFSET + Long.BYTES;


	/**
	 * Pre-compute the offset of index for different levels to dismiss the duplicated
	 * computation at runtime.
	 */
	private static final int[] INDEX_NEXT_OFFSET_BY_LEVEL_ARRAY = new int[MAX_LEVEL + 1];

	/**
	 * Pre-compute the length of key meta for different levels to dismiss the duplicated
	 * computation at runtime.
	 */
	private static final int[] KEY_META_LEN_BY_LEVEL_ARRAY = new int[MAX_LEVEL + 1];

	static {
		for (int i = 1; i < INDEX_NEXT_OFFSET_BY_LEVEL_ARRAY.length; i++) {
			INDEX_NEXT_OFFSET_BY_LEVEL_ARRAY[i] = LEVEL_INDEX_OFFSET + (i - 1) * Long.BYTES;
		}

		for (int i = 0; i < KEY_META_LEN_BY_LEVEL_ARRAY.length; i++) {
			KEY_META_LEN_BY_LEVEL_ARRAY[i] = LEVEL_INDEX_OFFSET + 2 * i * Long.BYTES;
		}
	}

	/**
	 * Returns the level of the node.
	 *
	 * @param memorySegment memory segment for key space.
	 * @param offset offset of key space in the memory segment.
	 */
	public static int getLevel(MemorySegment memorySegment, int offset) {
		return memorySegment.getInt(offset + KEY_META_OFFSET) & BYTE_MASK;
	}

	/**
	 * Returns the status of the node.
	 *
	 * @param memorySegment memory segment for key space.
	 * @param offset offset of key space in the memory segment.
	 */
	public static NodeStatus getNodeStatus(MemorySegment memorySegment, int offset) {
		byte status = (byte) ((memorySegment.getInt(offset + KEY_META_OFFSET) >>> 8) & BYTE_MASK);
		return NodeStatus.valueOf(status);
	}

	/**
	 * Puts the level and status to the key space.
	 *
	 * @param memorySegment memory segment for key space.
	 * @param offset offset of key space in the memory segment.
	 * @param level the level.
	 * @param status the status.
	 */
	public static void putLevelAndNodeStatus(MemorySegment memorySegment, int offset, int level, NodeStatus status) {
		int data = ((status.getValue() & BYTE_MASK) << 8) | level;
		memorySegment.putInt(offset + SkipListUtils.KEY_META_OFFSET, data);
	}

	/**
	 * Returns the length of the key.
	 *
	 * @param memorySegment memory segment for key space.
	 * @param offset offset of key space in the memory segment.
	 */
	public static int getKeyLen(MemorySegment memorySegment, int offset) {
		return memorySegment.getInt(offset + KEY_LEN_OFFSET);
	}

	/**
	 * Puts the length of key to the key space.
	 *
	 * @param memorySegment memory segment for key space.
	 * @param offset offset of key space in the memory segment.
	 * @param keyLen length of key.
	 */
	public static void putKeyLen(MemorySegment memorySegment, int offset, int keyLen) {
		memorySegment.putInt(offset + KEY_LEN_OFFSET, keyLen);
	}

	/**
	 * Returns the value pointer.
	 *
	 * @param memorySegment memory segment for key space.
	 * @param offset offset of key space in the memory segment.
	 */
	public static long getValuePointer(MemorySegment memorySegment, int offset) {
		return memorySegment.getLong(offset + VALUE_POINTER_OFFSET);
	}

	/**
	 * Puts the value pointer to key space.
	 *
	 * @param memorySegment memory segment for key space.
	 * @param offset offset of key space in the memory segment.
	 * @param valuePointer the value pointer.
	 */
	public static void putValuePointer(MemorySegment memorySegment, int offset, long valuePointer) {
		memorySegment.putLong(offset + VALUE_POINTER_OFFSET, valuePointer);
	}

	/**
	 * Returns the next key pointer on level 0.
	 *
	 * @param memorySegment memory segment for key space.
	 * @param offset offset of key space in the memory segment.
	 */
	public static long getNextKeyPointer(MemorySegment memorySegment, int offset) {
		return memorySegment.getLong(offset + NEXT_KEY_POINTER_OFFSET);
	}

	/**
	 * Puts the next key pointer on level 0 to key space.
	 *
	 * @param memorySegment memory segment for key space.
	 * @param offset offset of key space in the memory segment.
	 * @param nextKeyPointer next key pointer on level 0.
	 */
	public static void putNextKeyPointer(MemorySegment memorySegment, int offset, long nextKeyPointer) {
		memorySegment.putLong(offset + NEXT_KEY_POINTER_OFFSET, nextKeyPointer);
	}

	/**
	 * Returns next key pointer on the given index level.
	 *
	 * @param memorySegment memory segment for key space.
	 * @param offset offset of key space in the memory segment.
	 * @param level level of index.
	 */
	public static long getNextIndexNode(MemorySegment memorySegment, int offset, int level) {
		return memorySegment.getLong(offset + INDEX_NEXT_OFFSET_BY_LEVEL_ARRAY[level]);
	}

	/**
	 * Puts next key pointer on the given index level to key space.
	 *
	 * @param memorySegment memory segment for key space.
	 * @param offset offset of key space in the memory segment.
	 * @param level level of index.
	 * @param nextKeyPointer next key pointer on the given level.
	 */
	public static void putNextIndexNode(MemorySegment memorySegment, int offset, int level, long nextKeyPointer) {
		memorySegment.putLong(offset + INDEX_NEXT_OFFSET_BY_LEVEL_ARRAY[level], nextKeyPointer);
	}

	/**
	 * Returns previous key pointer on the given index level.
	 *
	 * @param memorySegment memory segment for key space.
	 * @param offset offset of key space in the memory segment.
	 * @param totalLevel the level of the node.
	 * @param level on which level to get the previous key pointer of the node.
	 */
	public static long getPrevIndexNode(MemorySegment memorySegment, int offset, int totalLevel, int level) {
		int of = getIndexOffset(offset, totalLevel, level);
		return memorySegment.getLong(of);
	}

	private static int getIndexOffset(int offset, int totalLevel, int level) {
		return offset + INDEX_NEXT_OFFSET_BY_LEVEL_ARRAY[totalLevel] + level * Long.BYTES;
	}

	/**
	 * Puts previous key pointer on the given index level to key space.
	 *
	 * @param memorySegment memory segment for key space.
	 * @param offset offset of key space in the memory segment.
	 * @param totalLevel top level of the key.
	 * @param level level of index.
	 * @param prevKeyPointer previous key pointer on the given level.
	 */
	public static void putPrevIndexNode(
		MemorySegment memorySegment, int offset, int totalLevel, int level, long prevKeyPointer) {
		int of = getIndexOffset(offset, totalLevel, level);
		memorySegment.putLong(of, prevKeyPointer);
	}

	/**
	 * Returns the length of key meta with the given level.
	 *
	 * @param level level of the key.
	 */
	public static int getKeyMetaLen(int level) {
		Preconditions.checkArgument(level >= 0 && level < KEY_META_LEN_BY_LEVEL_ARRAY.length,
			"level " + level + " out of range [0, " + KEY_META_LEN_BY_LEVEL_ARRAY.length + ")");
		return KEY_META_LEN_BY_LEVEL_ARRAY[level];
	}

	/**
	 * Returns the offset of key data in the key space.
	 *
	 * @param level level of the key.
	 */
	public static int getKeyDataOffset(int level) {
		return SkipListUtils.getKeyMetaLen(level);
	}

	/**
	 * Puts the key data into key space.
	 *
	 * @param segment memory segment for key space.
	 * @param offset offset of key space in memory segment.
	 * @param keySegment memory segment for key data.
	 * @param keyOffset offset of key data in memory segment.
	 * @param keyLen length of key data.
	 * @param level level of the key.
	 */
	public static void putKeyData(
		MemorySegment segment, int offset, MemorySegment keySegment, int keyOffset, int keyLen, int level) {
		keySegment.copyTo(keyOffset, segment, offset + getKeyDataOffset(level), keyLen);
	}

	/**
	 * Value space schema.
	 * - value meta
	 * -- int: version of this value to support copy on write
	 * -- long: pointer to the key space
	 * -- long: pointer to next older value
	 * -- int: length of data
	 * - byte[] data of value
	 */
	static final int VALUE_META_OFFSET = 0;
	static final int VALUE_VERSION_OFFSET = VALUE_META_OFFSET;
	static final int KEY_POINTER_OFFSET = VALUE_VERSION_OFFSET + Integer.BYTES;
	static final int NEXT_VALUE_POINTER_OFFSET = KEY_POINTER_OFFSET + Long.BYTES;
	static final int VALUE_LEN_OFFSET = NEXT_VALUE_POINTER_OFFSET + Long.BYTES;
	static final int VALUE_DATA_OFFSET = VALUE_LEN_OFFSET + Integer.BYTES;

	/**
	 * Returns the version of value.
	 *
	 * @param memorySegment memory segment for value space.
	 * @param offset offset of value space in memory segment.
	 */
	public static int getValueVersion(MemorySegment memorySegment, int offset) {
		return memorySegment.getInt(offset + VALUE_VERSION_OFFSET);
	}

	/**
	 * Puts the version of value to value space.
	 *
	 * @param memorySegment memory segment for value space.
	 * @param offset offset of value space in memory segment.
	 * @param version version of value.
	 */
	public static void putValueVersion(MemorySegment memorySegment, int offset, int version) {
		memorySegment.putInt(offset + VALUE_VERSION_OFFSET, version);
	}

	/**
	 * Return the pointer to key space.
	 *
	 * @param memorySegment memory segment for value space.
	 * @param offset offset of value space in memory segment.
	 */
	public static long getKeyPointer(MemorySegment memorySegment, int offset) {
		return memorySegment.getLong(offset + KEY_POINTER_OFFSET);
	}

	/**
	 * Puts the pointer of key space.
	 *
	 * @param memorySegment memory segment for value space.
	 * @param offset offset of value space in memory segment.
	 * @param keyPointer pointer to key space.
	 */
	public static void putKeyPointer(MemorySegment memorySegment, int offset, long keyPointer) {
		memorySegment.putLong(offset + KEY_POINTER_OFFSET, keyPointer);
	}

	/**
	 * Return the pointer to next value space.
	 *
	 * @param memorySegment memory segment for value space.
	 * @param offset offset of value space in memory segment.
	 */
	public static long getNextValuePointer(MemorySegment memorySegment, int offset) {
		return memorySegment.getLong(offset + NEXT_VALUE_POINTER_OFFSET);
	}

	/**
	 * Puts the pointer of next value space.
	 *
	 * @param memorySegment memory segment for value space.
	 * @param offset offset of value space in memory segment.
	 * @param nextValuePointer pointer to next value space.
	 */
	public static void putNextValuePointer(MemorySegment memorySegment, int offset, long nextValuePointer) {
		memorySegment.putLong(offset + NEXT_VALUE_POINTER_OFFSET, nextValuePointer);
	}

	/**
	 * Return the length of value data.
	 *
	 * @param memorySegment memory segment for value space.
	 * @param offset offset of value space in memory segment.
	 */
	public static int getValueLen(MemorySegment memorySegment, int offset) {
		return memorySegment.getInt(offset + VALUE_LEN_OFFSET);
	}

	/**
	 * Puts the length of value data.
	 *
	 * @param memorySegment memory segment for value space.
	 * @param offset offset of value space in memory segment.
	 * @param valueLen length of value data.
	 */
	public static void putValueLen(MemorySegment memorySegment, int offset, int valueLen) {
		memorySegment.putInt(offset + VALUE_LEN_OFFSET, valueLen);
	}

	/**
	 * Returns the length of value meta.
	 */
	public static int getValueMetaLen() {
		return VALUE_DATA_OFFSET;
	}

	/**
	 * Puts the value data into value space.
	 *
	 * @param memorySegment memory segment for value space.
	 * @param offset offset of value space in memory segment.
	 * @param value value data.
	 */
	public static void putValueData(MemorySegment memorySegment, int offset, byte[] value) {
		MemorySegment valueSegment = MemorySegmentFactory.wrap(value);
		valueSegment.copyTo(0, memorySegment, offset + getValueMetaLen(), value.length);
	}

	/**
	 * Set the next node of the given node at the given level.
	 *
	 * @param node             the node.
	 * @param nextNode         the next node to set.
	 * @param level            the level to find the next node.
	 * @param levelIndexHeader the header of the level index.
	 * @param spaceAllocator   the space allocator.
	 */
	static void helpSetNextNode(
			long node,
			long nextNode,
			int level,
			LevelIndexHeader levelIndexHeader,
			Allocator spaceAllocator) {
		if (node == HEAD_NODE) {
			levelIndexHeader.updateNextNode(level, nextNode);
			return;
		}

		Chunk chunk = spaceAllocator.getChunkById(SpaceUtils.getChunkIdByAddress(node));
		int offsetInChunk = SpaceUtils.getChunkOffsetByAddress(node);
		MemorySegment segment = chunk.getMemorySegment(offsetInChunk);
		int offsetInByteBuffer = chunk.getOffsetInSegment(offsetInChunk);

		if (level == 0) {
			putNextKeyPointer(segment, offsetInByteBuffer, nextNode);
		} else {
			putNextIndexNode(segment, offsetInByteBuffer, level, nextNode);
		}
	}

	/**
	 * Return the next of the given node at the given level.
	 *
	 * @param node             the node to find the next node for.
	 * @param level            the level to find the next node.
	 * @param levelIndexHeader the header of the level index.
	 * @param spaceAllocator   the space allocator.
	 * @return the pointer to the next node of the given node at the given level.
	 */
	static long helpGetNextNode(
			long node,
			int level,
			LevelIndexHeader levelIndexHeader,
			Allocator spaceAllocator) {
		if (node == HEAD_NODE) {
			return levelIndexHeader.getNextNode(level);
		}

		Chunk chunk = spaceAllocator.getChunkById(SpaceUtils.getChunkIdByAddress(node));
		int offsetInChunk = SpaceUtils.getChunkOffsetByAddress(node);
		MemorySegment segment = chunk.getMemorySegment(offsetInChunk);
		int offsetInByteBuffer = chunk.getOffsetInSegment(offsetInChunk);

		return level == 0 ? getNextKeyPointer(segment, offsetInByteBuffer)
			: getNextIndexNode(segment, offsetInByteBuffer, level);
	}

	/**
	 * Set the previous node of the given node at the given level. The level must be positive.
	 *
	 * @param node           the node.
	 * @param prevNode       the previous node to set.
	 * @param level          the level to find the next node.
	 * @param spaceAllocator the space allocator.
	 */
	static void helpSetPrevNode(long node, long prevNode, int level, Allocator spaceAllocator) {
		Preconditions.checkArgument(level > 0, "only index level have previous node");

		if (node == HEAD_NODE || node == NIL_NODE) {
			return;
		}

		Chunk chunk = spaceAllocator.getChunkById(SpaceUtils.getChunkIdByAddress(node));
		int offsetInChunk = SpaceUtils.getChunkOffsetByAddress(node);
		MemorySegment segment = chunk.getMemorySegment(offsetInChunk);
		int offsetInByteBuffer = chunk.getOffsetInSegment(offsetInChunk);

		int topLevel = getLevel(segment, offsetInByteBuffer);

		putPrevIndexNode(segment, offsetInByteBuffer, topLevel, level, prevNode);
	}

	/**
	 * Set the previous node and the next node of the given node at the given level.
	 * The level must be positive.
	 * @param node           the node.
	 * @param prevNode       the previous node to set.
	 * @param nextNode       the next node to set.
	 * @param level          the level to find the next node.
	 * @param spaceAllocator the space allocator.
	 */
	static void helpSetPrevAndNextNode(
			long node,
			long prevNode,
			long nextNode,
			int level,
			Allocator spaceAllocator) {
		Preconditions.checkArgument(node != HEAD_NODE, "head node does not have previous node");
		Preconditions.checkArgument(level > 0, "only index level have previous node");

		Chunk chunk = spaceAllocator.getChunkById(SpaceUtils.getChunkIdByAddress(node));
		int offsetInChunk = SpaceUtils.getChunkOffsetByAddress(node);
		MemorySegment segment = chunk.getMemorySegment(offsetInChunk);
		int offsetInByteBuffer = chunk.getOffsetInSegment(offsetInChunk);

		int topLevel = getLevel(segment, offsetInByteBuffer);

		putNextIndexNode(segment, offsetInByteBuffer, level, nextNode);
		putPrevIndexNode(segment, offsetInByteBuffer, topLevel, level, prevNode);
	}

	/**
	 * Whether the node has been logically removed.
	 *
	 * @param node           the node to check against
	 * @param spaceAllocator the space allocator
	 * @return true if the node has been logically removed.
	 */
	static boolean isNodeRemoved(long node, Allocator spaceAllocator) {
		if (node == NIL_NODE) {
			return false;
		}

		Chunk chunk = spaceAllocator.getChunkById(SpaceUtils.getChunkIdByAddress(node));
		int offsetInChunk = SpaceUtils.getChunkOffsetByAddress(node);
		MemorySegment segment = chunk.getMemorySegment(offsetInChunk);
		int offsetInByteBuffer = chunk.getOffsetInSegment(offsetInChunk);
		return getNodeStatus(segment, offsetInByteBuffer) == NodeStatus.REMOVE;
	}

	/**
	 * Compare the first skip list key in the given memory segment with the second skip list key in the given node.
	 *
	 * @param keySegment     memory segment storing the first key.
	 * @param keyOffset      offset of the first key in memory segment.
	 * @param targetNode     the node storing the second key.
	 * @param spaceAllocator the space allocator.
	 * @return Returns a negative integer, zero, or a positive integer as the first key is less than,
	 * equal to, or greater than the second.
	 */
	static int compareSegmentAndNode(
		MemorySegment keySegment,
		int keyOffset,
		long targetNode,
		@Nonnull Allocator spaceAllocator) {
		Chunk chunk = spaceAllocator.getChunkById(SpaceUtils.getChunkIdByAddress(targetNode));
		int offsetInChunk = SpaceUtils.getChunkOffsetByAddress(targetNode);
		MemorySegment targetKeySegment = chunk.getMemorySegment(offsetInChunk);
		int offsetInByteBuffer = chunk.getOffsetInSegment(offsetInChunk);

		int level = getLevel(targetKeySegment, offsetInByteBuffer);
		int targetKeyOffset = offsetInByteBuffer + getKeyDataOffset(level);

		return SkipListKeyComparator.compareTo(keySegment, keyOffset, targetKeySegment, targetKeyOffset);
	}

	/**
	 * Find the predecessor node for the given node at the given level.
	 *
	 * @param node             the node.
	 * @param level            the level.
	 * @param levelIndexHeader the head level index.
	 * @param spaceAllocator   the space allocator.
	 * @return node id before the key at the given level.
	 */
	static long findPredecessor(
		long node,
		int level,
		LevelIndexHeader levelIndexHeader,
		@Nonnull Allocator spaceAllocator) {
		Chunk chunk = spaceAllocator.getChunkById(SpaceUtils.getChunkIdByAddress(node));
		int offsetInChunk = SpaceUtils.getChunkOffsetByAddress(node);
		MemorySegment keySegment = chunk.getMemorySegment(offsetInChunk);
		int offsetInByteBuffer = chunk.getOffsetInSegment(offsetInChunk);

		int keyLevel = getLevel(keySegment, offsetInByteBuffer);
		int keyOffset = offsetInByteBuffer + getKeyDataOffset(keyLevel);

		return findPredecessor(keySegment, keyOffset, level, levelIndexHeader, spaceAllocator);
	}

	/**
	 * Find the predecessor node for the given key at the given level.
	 * The key is in the memory segment positioning at the given offset.
	 *
	 * @param keySegment       memory segment which contains the key.
	 * @param keyOffset        offset of the key in the memory segment.
	 * @param level            the level.
	 * @param levelIndexHeader the head level index.
	 * @param spaceAllocator   the space allocator.
	 * @return node id before the key at the given level.
	 */
	static long findPredecessor(
		MemorySegment keySegment,
		int keyOffset,
		int level,
		@Nonnull LevelIndexHeader levelIndexHeader,
		Allocator spaceAllocator) {
		int currentLevel = levelIndexHeader.getLevel();
		long currentNode = HEAD_NODE;
		long nextNode = levelIndexHeader.getNextNode(currentLevel);

		for ( ; ; ) {
			if (nextNode != NIL_NODE) {
				int c = compareSegmentAndNode(keySegment, keyOffset, nextNode, spaceAllocator);
				if (c > 0) {
					currentNode = nextNode;
					nextNode = helpGetNextNode(currentNode, currentLevel, levelIndexHeader, spaceAllocator);
					continue;
				}
			}

			if (currentLevel <= level) {
				return currentNode;
			}

			currentLevel--;
			nextNode = helpGetNextNode(currentNode, currentLevel, levelIndexHeader, spaceAllocator);
		}
	}

	/**
	 * Returns the next value pointer of the value.
	 *
	 * @param valuePointer   the value pointer of current value.
	 * @param spaceAllocator the space allocator.
	 */
	static long helpGetNextValuePointer(long valuePointer, Allocator spaceAllocator) {
		Chunk chunk = spaceAllocator.getChunkById(SpaceUtils.getChunkIdByAddress(valuePointer));
		int offsetInChunk = SpaceUtils.getChunkOffsetByAddress(valuePointer);
		MemorySegment segment = chunk.getMemorySegment(offsetInChunk);
		int offsetInByteBuffer = chunk.getOffsetInSegment(offsetInChunk);

		return getNextValuePointer(segment, offsetInByteBuffer);
	}

	/**
	 * Sets the next value pointer of the value.
	 *
	 * @param valuePointer     the value pointer.
	 * @param nextValuePointer the next value pointer to set.
	 * @param spaceAllocator   the space allocator.
	 */
	static void helpSetNextValuePointer(long valuePointer, long nextValuePointer, Allocator spaceAllocator) {
		Chunk chunk = spaceAllocator.getChunkById(SpaceUtils.getChunkIdByAddress(valuePointer));
		int offsetInChunk = SpaceUtils.getChunkOffsetByAddress(valuePointer);
		MemorySegment segment = chunk.getMemorySegment(offsetInChunk);
		int offsetInByteBuffer = chunk.getOffsetInSegment(offsetInChunk);

		putNextValuePointer(segment, offsetInByteBuffer, nextValuePointer);
	}

	/**
	 * Build the level index for the given node.
	 *
	 * @param node             the node.
	 * @param level            level of the node.
	 * @param keySegment       memory segment of the key in the node.
	 * @param keyOffset        offset of the key in memory segment.
	 * @param levelIndexHeader the head level index.
	 * @param spaceAllocator   the space allocator.
	 */
	static void buildLevelIndex(long node, int level, MemorySegment keySegment, int keyOffset, LevelIndexHeader levelIndexHeader, Allocator spaceAllocator) {
		int currLevel = level;
		long prevNode = findPredecessor(keySegment, keyOffset, currLevel, levelIndexHeader, spaceAllocator);
		long currentNode = helpGetNextNode(prevNode, currLevel, levelIndexHeader, spaceAllocator);

		for (; ; ) {
			if (currentNode != NIL_NODE) {
				int c = compareSegmentAndNode(keySegment, keyOffset, currentNode, spaceAllocator);
				if (c > 0) {
					prevNode = currentNode;
					currentNode = helpGetNextNode(currentNode, currLevel, levelIndexHeader, spaceAllocator);
					continue;
				}
			}

			helpSetPrevAndNextNode(node, prevNode, currentNode, currLevel, spaceAllocator);
			helpSetNextNode(prevNode, node, currLevel, levelIndexHeader, spaceAllocator);
			helpSetPrevNode(currentNode, node, currLevel, spaceAllocator);

			currLevel--;
			if (currLevel == 0) {
				break;
			}

			currentNode = helpGetNextNode(prevNode, currLevel, levelIndexHeader, spaceAllocator);
		}
	}

	/**
	 * Remove the level index for the node from the skip list.
	 *
	 * @param node             the node.
	 * @param spaceAllocator   the space allocator.
	 * @param levelIndexHeader the head level index.
	 */
	static void removeLevelIndex(long node, Allocator spaceAllocator, LevelIndexHeader levelIndexHeader) {
		Chunk chunk = spaceAllocator.getChunkById(SpaceUtils.getChunkIdByAddress(node));
		int offsetInChunk = SpaceUtils.getChunkOffsetByAddress(node);
		MemorySegment segment = chunk.getMemorySegment(offsetInChunk);
		int offsetInByteBuffer = chunk.getOffsetInSegment(offsetInChunk);

		int level = getLevel(segment, offsetInByteBuffer);

		for (int i = 1; i <= level; i++) {
			long prevNode = getPrevIndexNode(segment, offsetInByteBuffer, level, i);
			long nextNode = getNextIndexNode(segment, offsetInByteBuffer, i);
			helpSetNextNode(prevNode, nextNode, i, levelIndexHeader, spaceAllocator);
			helpSetPrevNode(nextNode, prevNode, i, spaceAllocator);
		}
	}

	/**
	 * Free the space of the linked values, and the head value
	 * is pointed by the given pointer.
	 *
	 * @param valuePointer   the pointer of the value to start removing.
	 * @param spaceAllocator the space allocator.
	 */
	static void removeAllValues(long valuePointer, Allocator spaceAllocator) {
		long nextValuePointer;
		while (valuePointer != NIL_VALUE_POINTER) {
			nextValuePointer = helpGetNextValuePointer(valuePointer, spaceAllocator);
			spaceAllocator.free(valuePointer);
			valuePointer = nextValuePointer;
		}
	}

	/**
	 * Returns the value pointer of the node.
	 *
	 * @param node           the node.
	 * @param spaceAllocator the space allocator.
	 */
	static long helpGetValuePointer(long node, Allocator spaceAllocator) {
		Chunk chunk = spaceAllocator.getChunkById(SpaceUtils.getChunkIdByAddress(node));
		int offsetInChunk = SpaceUtils.getChunkOffsetByAddress(node);
		MemorySegment segment = chunk.getMemorySegment(offsetInChunk);
		int offsetInByteBuffer = chunk.getOffsetInSegment(offsetInChunk);

		return getValuePointer(segment, offsetInByteBuffer);
	}

	/**
	 * Returns the version of the value.
	 *
	 * @param valuePointer   the pointer to the value.
	 * @param spaceAllocator the space allocator.
	 */
	static int helpGetValueVersion(long valuePointer, Allocator spaceAllocator) {
		Chunk chunk = spaceAllocator.getChunkById(SpaceUtils.getChunkIdByAddress(valuePointer));
		int offsetInChunk = SpaceUtils.getChunkOffsetByAddress(valuePointer);
		MemorySegment segment = chunk.getMemorySegment(offsetInChunk);
		int offsetInByteBuffer = chunk.getOffsetInSegment(offsetInChunk);

		return getValueVersion(segment, offsetInByteBuffer);
	}

	/**
	 * Returns the length of the value.
	 *
	 * @param valuePointer   the pointer to the value.
	 * @param spaceAllocator the space allocator.
	 */
	static int helpGetValueLen(long valuePointer, Allocator spaceAllocator) {
		Chunk chunk = spaceAllocator.getChunkById(SpaceUtils.getChunkIdByAddress(valuePointer));
		int offsetInChunk = SpaceUtils.getChunkOffsetByAddress(valuePointer);
		MemorySegment segment = chunk.getMemorySegment(offsetInChunk);
		int offsetInByteBuffer = chunk.getOffsetInSegment(offsetInChunk);

		return getValueLen(segment, offsetInByteBuffer);
	}

	/**
	 * Return of the newest version of value for the node.
	 *
	 * @param node           the node.
	 * @param spaceAllocator the space allocator.
	 */
	static int helpGetNodeLatestVersion(long node, Allocator spaceAllocator) {
		Chunk chunk = spaceAllocator.getChunkById(SpaceUtils.getChunkIdByAddress(node));
		int offsetInChunk = SpaceUtils.getChunkOffsetByAddress(node);
		MemorySegment segment = chunk.getMemorySegment(offsetInChunk);
		int offsetInByteBuffer = chunk.getOffsetInSegment(offsetInChunk);
		long valuePointer = getValuePointer(segment, offsetInByteBuffer);

		return helpGetValueVersion(valuePointer, spaceAllocator);
	}

}
