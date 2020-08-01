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
import org.apache.flink.util.Preconditions;

import static org.apache.flink.runtime.state.heap.SkipListUtils.DEFAULT_LEVEL;
import static org.apache.flink.runtime.state.heap.SkipListUtils.MAX_LEVEL;
import static org.apache.flink.runtime.state.heap.SkipListUtils.NIL_NODE;

/**
 * Implementation of {@link LevelIndexHeader} which stores index on heap.
 */
public class OnHeapLevelIndexHeader implements LevelIndexHeader {

	/**
	 * The level index array where each position stores the next node id of the level.
	 */
	private volatile long[] levelIndex;

	/**
	 * The topmost level currently.
	 */
	private volatile int topLevel;

	/**
	 * Next node at level 0.
	 */
	private volatile long nextNode;

	OnHeapLevelIndexHeader() {
		this(DEFAULT_LEVEL);
	}

	private OnHeapLevelIndexHeader(int maxLevel) {
		Preconditions.checkArgument(maxLevel >= 1 && maxLevel <= MAX_LEVEL,
			"maxLevel(" + maxLevel + ") must be non-negative and no more than " + MAX_LEVEL);
		this.topLevel = 1;
		this.nextNode = NIL_NODE;
		this.levelIndex = new long[maxLevel];
		initLevelIndex(levelIndex);
	}

	private void initLevelIndex(long[] levelIndex) {
		for (int i = 0; i < levelIndex.length; i++) {
			levelIndex[i] = NIL_NODE;
		}
	}

	@Override
	public int getLevel() {
		return topLevel;
	}

	@Override
	public void updateLevel(int level) {
		Preconditions.checkArgument(level >= 0 && level <= MAX_LEVEL,
			"level(" + level + ") must be non-negative and no more than " + MAX_LEVEL);
		Preconditions.checkArgument(level <= this.topLevel + 1,
			"top level " + topLevel + " must be updated level by level, but new level is " + level);

		if (levelIndex.length < level) {
			long[] newLevelIndex = new long[this.levelIndex.length * 2];
			initLevelIndex(newLevelIndex);
			System.arraycopy(this.levelIndex, 0, newLevelIndex, 0, this.levelIndex.length);
			this.levelIndex = newLevelIndex;
		}

		if (topLevel < level) {
			topLevel = level;
		}
	}

	@Override
	public long getNextNode(int level) {
		Preconditions.checkArgument(level >= 0 && level <= topLevel,
			"invalid level " + level + " current top level is " + topLevel);

		if (level == 0) {
			return nextNode;
		}
		return levelIndex[level - 1];
	}

	@Override
	public void updateNextNode(int level, long node) {
		Preconditions.checkArgument(level >= 0 && level <= topLevel,
			"invalid level " + level + " current top level is " + topLevel);

		if (level == 0) {
			nextNode = node;
		} else {
			levelIndex[level - 1] = node;
		}
	}

	@VisibleForTesting
	long[] getLevelIndex() {
		return levelIndex;
	}
}
