/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Collection;
import java.util.List;
import java.util.function.BiFunction;

/**
 * Utils for RocksDB Incremental Checkpoint.
 */
public class RocksDBIncrementalCheckpointUtils {

	/**
	 * The threshold of the overlap fraction of the handle's key-group range with target key-group range to
	 * be an initial handle.
	 */
	private static final double OVERLAP_FRACTION_THRESHOLD = 0.75;

	/**
	 * Evaluates state handle's "score" regarding to the target range when choosing
	 * the best state handle to init the initial db for recovery, if the overlap fraction
	 * is less than {@link #OVERLAP_FRACTION_THRESHOLD}, then just return -1 to mean the handle
	 * has not chance to be the initial handle.
	 */
	private static final BiFunction<KeyedStateHandle, KeyGroupRange, Double> STATE_HANDLE_EVALUATOR = (stateHandle, targetKeyGroupRange) -> {
		final KeyGroupRange handleKeyGroupRange = stateHandle.getKeyGroupRange();
		final KeyGroupRange intersectGroup = handleKeyGroupRange.getIntersection(targetKeyGroupRange);

		final double overlapFraction = (double) intersectGroup.getNumberOfKeyGroups() / handleKeyGroupRange.getNumberOfKeyGroups();

		if (overlapFraction < OVERLAP_FRACTION_THRESHOLD) {
			return -1.0;
		}

		return intersectGroup.getNumberOfKeyGroups() * overlapFraction * overlapFraction;
	};

	/**
	 * The method to clip the db instance according to the target key group range using
	 * the {@link RocksDB#delete(ColumnFamilyHandle, byte[])}.
	 *
	 * @param db the RocksDB instance to be clipped.
	 * @param columnFamilyHandles the column families in the db instance.
	 * @param targetKeyGroupRange the target key group range.
	 * @param currentKeyGroupRange the key group range of the db instance.
	 * @param keyGroupPrefixBytes Number of bytes required to prefix the key groups.
	 */
	public static void clipDBWithKeyGroupRange(
		@Nonnull RocksDB db,
		@Nonnull List<ColumnFamilyHandle> columnFamilyHandles,
		@Nonnull KeyGroupRange targetKeyGroupRange,
		@Nonnull KeyGroupRange currentKeyGroupRange,
		@Nonnegative int keyGroupPrefixBytes,
		@Nonnegative long writeBatchSize) throws RocksDBException {

		final byte[] beginKeyGroupBytes = new byte[keyGroupPrefixBytes];
		final byte[] endKeyGroupBytes = new byte[keyGroupPrefixBytes];

		if (currentKeyGroupRange.getStartKeyGroup() < targetKeyGroupRange.getStartKeyGroup()) {
			RocksDBKeySerializationUtils.serializeKeyGroup(
				currentKeyGroupRange.getStartKeyGroup(), beginKeyGroupBytes);
			RocksDBKeySerializationUtils.serializeKeyGroup(
				targetKeyGroupRange.getStartKeyGroup(), endKeyGroupBytes);
			deleteRange(db, columnFamilyHandles, beginKeyGroupBytes, endKeyGroupBytes, writeBatchSize);
		}

		if (currentKeyGroupRange.getEndKeyGroup() > targetKeyGroupRange.getEndKeyGroup()) {
			RocksDBKeySerializationUtils.serializeKeyGroup(
				targetKeyGroupRange.getEndKeyGroup() + 1, beginKeyGroupBytes);
			RocksDBKeySerializationUtils.serializeKeyGroup(
				currentKeyGroupRange.getEndKeyGroup() + 1, endKeyGroupBytes);
			deleteRange(db, columnFamilyHandles, beginKeyGroupBytes, endKeyGroupBytes, writeBatchSize);
		}
	}

	/**
	 * Delete the record falls into [beginKeyBytes, endKeyBytes) of the db.
	 *
	 * @param db the target need to be clipped.
	 * @param columnFamilyHandles the column family need to be clipped.
	 * @param beginKeyBytes the begin key bytes
	 * @param endKeyBytes the end key bytes
	 */
	private static void deleteRange(
		RocksDB db,
		List<ColumnFamilyHandle> columnFamilyHandles,
		byte[] beginKeyBytes,
		byte[] endKeyBytes,
		@Nonnegative long writeBatchSize) throws RocksDBException {

		for (ColumnFamilyHandle columnFamilyHandle : columnFamilyHandles) {
			try (RocksIteratorWrapper iteratorWrapper = RocksDBOperationUtils.getRocksIterator(db, columnFamilyHandle);
				RocksDBWriteBatchWrapper writeBatchWrapper = new RocksDBWriteBatchWrapper(db, writeBatchSize)) {

				iteratorWrapper.seek(beginKeyBytes);

				while (iteratorWrapper.isValid()) {
					final byte[] currentKey = iteratorWrapper.key();
					if (beforeThePrefixBytes(currentKey, endKeyBytes)) {
						writeBatchWrapper.remove(columnFamilyHandle, currentKey);
					} else {
						break;
					}
					iteratorWrapper.next();
				}
			}
		}
	}

	/**
	 * check whether the bytes is before prefixBytes in the character order.
	 */
	public static boolean beforeThePrefixBytes(@Nonnull byte[] bytes, @Nonnull byte[] prefixBytes) {
		final int prefixLength = prefixBytes.length;
		for (int i = 0; i < prefixLength; ++i) {
			int r = (char) prefixBytes[i] - (char) bytes[i];
			if (r != 0) {
				return r > 0;
			}
		}
		return false;
	}

	/**
	 * Choose the best state handle according to the {@link #STATE_HANDLE_EVALUATOR}
	 * to init the initial db.
	 *
	 * @param restoreStateHandles The candidate state handles.
	 * @param targetKeyGroupRange The target key group range.
	 * @return The best candidate or null if no candidate was a good fit.
	 */
	@Nullable
	public static KeyedStateHandle chooseTheBestStateHandleForInitial(
		@Nonnull Collection<KeyedStateHandle> restoreStateHandles,
		@Nonnull KeyGroupRange targetKeyGroupRange) {

		KeyedStateHandle bestStateHandle = null;
		double bestScore = 0;
		for (KeyedStateHandle rawStateHandle : restoreStateHandles) {
			double handleScore = STATE_HANDLE_EVALUATOR.apply(rawStateHandle, targetKeyGroupRange);
			if (handleScore > bestScore) {
				bestStateHandle = rawStateHandle;
				bestScore = handleScore;
			}
		}

		return bestStateHandle;
	}
}
