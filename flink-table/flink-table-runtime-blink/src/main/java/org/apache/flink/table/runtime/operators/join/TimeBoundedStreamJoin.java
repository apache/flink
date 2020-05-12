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

package org.apache.flink.table.runtime.operators.join;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * A CoProcessFunction to execute time-bounded stream inner-join.
 * Two kinds of time criteria:
 * "L.time between R.time + X and R.time + Y" or "R.time between L.time - Y and L.time - X"
 * X and Y might be negative or positive and X <= Y.
 */
abstract class TimeBoundedStreamJoin extends KeyedCoProcessFunction<RowData, RowData, RowData, RowData> {
	private static final Logger LOGGER = LoggerFactory.getLogger(TimeBoundedStreamJoin.class);
	private final FlinkJoinType joinType;
	protected final long leftRelativeSize;
	protected final long rightRelativeSize;

	// Minimum interval by which state is cleaned up
	private final long minCleanUpInterval;
	protected final long allowedLateness;
	private final RowDataTypeInfo leftType;
	private final RowDataTypeInfo rightType;
	private GeneratedFunction<FlatJoinFunction<RowData, RowData, RowData>> genJoinFunc;
	private transient OuterJoinPaddingUtil paddingUtil;

	private transient EmitAwareCollector joinCollector;

	// the join function for other conditions
	private transient FlatJoinFunction<RowData, RowData, RowData> joinFunction;

	// cache to store rows form the left stream
	private transient MapState<Long, List<Tuple2<RowData, Boolean>>> leftCache;
	// cache to store rows from the right stream
	private transient MapState<Long, List<Tuple2<RowData, Boolean>>> rightCache;

	// state to record the timer on the left stream. 0 means no timer set
	private transient ValueState<Long> leftTimerState;
	// state to record the timer on the right stream. 0 means no timer set
	private transient ValueState<Long> rightTimerState;

	// Points in time until which the respective cache has been cleaned.
	private long leftExpirationTime = 0L;
	private long rightExpirationTime = 0L;

	// Current time on the respective input stream.
	protected long leftOperatorTime = 0L;
	protected long rightOperatorTime = 0L;

	TimeBoundedStreamJoin(
			FlinkJoinType joinType,
			long leftLowerBound,
			long leftUpperBound,
			long allowedLateness,
			RowDataTypeInfo leftType,
			RowDataTypeInfo rightType,
			GeneratedFunction<FlatJoinFunction<RowData, RowData, RowData>> genJoinFunc) {
		this.joinType = joinType;
		this.leftRelativeSize = -leftLowerBound;
		this.rightRelativeSize = leftUpperBound;
		minCleanUpInterval = (leftRelativeSize + rightRelativeSize) / 2;
		if (allowedLateness < 0) {
			throw new IllegalArgumentException("The allowed lateness must be non-negative.");
		}
		this.allowedLateness = allowedLateness;
		this.leftType = leftType;
		this.rightType = rightType;
		this.genJoinFunc = genJoinFunc;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		LOGGER.debug("Instantiating JoinFunction: {} \n\n Code:\n{}", genJoinFunc.getClassName(),
				genJoinFunc.getCode());
		joinFunction = genJoinFunc.newInstance(getRuntimeContext().getUserCodeClassLoader());
		genJoinFunc = null;

		joinCollector = new EmitAwareCollector();

		// Initialize the data caches.
		ListTypeInfo<Tuple2<RowData, Boolean>> leftRowListTypeInfo = new ListTypeInfo<>(
				new TupleTypeInfo<>(leftType, BasicTypeInfo.BOOLEAN_TYPE_INFO));
		MapStateDescriptor<Long, List<Tuple2<RowData, Boolean>>> leftMapStateDescriptor = new MapStateDescriptor<>(
				"WindowJoinLeftCache",
				BasicTypeInfo.LONG_TYPE_INFO,
				leftRowListTypeInfo);
		leftCache = getRuntimeContext().getMapState(leftMapStateDescriptor);

		ListTypeInfo<Tuple2<RowData, Boolean>> rightRowListTypeInfo = new ListTypeInfo<>(
				new TupleTypeInfo<>(rightType, BasicTypeInfo.BOOLEAN_TYPE_INFO));
		MapStateDescriptor<Long, List<Tuple2<RowData, Boolean>>> rightMapStateDescriptor = new MapStateDescriptor<>(
				"WindowJoinRightCache",
				BasicTypeInfo.LONG_TYPE_INFO,
				rightRowListTypeInfo);
		rightCache = getRuntimeContext().getMapState(rightMapStateDescriptor);

		// Initialize the timer states.
		ValueStateDescriptor<Long> leftValueStateDescriptor = new ValueStateDescriptor<>(
				"WindowJoinLeftTimerState",
				Long.class);
		leftTimerState = getRuntimeContext().getState(leftValueStateDescriptor);

		ValueStateDescriptor<Long> rightValueStateDescriptor = new ValueStateDescriptor<>(
				"WindowJoinRightTimerState",
				Long.class);
		rightTimerState = getRuntimeContext().getState(rightValueStateDescriptor);

		paddingUtil = new OuterJoinPaddingUtil(leftType.getArity(), rightType.getArity());
	}

	@Override
	public void processElement1(RowData leftRow, Context ctx, Collector<RowData> out) throws Exception {
		joinCollector.setInnerCollector(out);
		updateOperatorTime(ctx);

		long timeForLeftRow = getTimeForLeftStream(ctx, leftRow);
		long rightQualifiedLowerBound = timeForLeftRow - rightRelativeSize;
		long rightQualifiedUpperBound = timeForLeftRow + leftRelativeSize;
		boolean emitted = false;

		// Check if we need to join the current row against cached rows of the right input.
		// The condition here should be rightMinimumTime < rightQualifiedUpperBound.
		// We use rightExpirationTime as an approximation of the rightMinimumTime here,
		// since rightExpirationTime <= rightMinimumTime is always true.
		if (rightExpirationTime < rightQualifiedUpperBound) {
			// Upper bound of current join window has not passed the cache expiration time yet.
			// There might be qualifying rows in the cache that the current row needs to be joined with.
			rightExpirationTime = calExpirationTime(leftOperatorTime, rightRelativeSize);
			// Join the leftRow with rows from the right cache.
			Iterator<Map.Entry<Long, List<Tuple2<RowData, Boolean>>>> rightIterator = rightCache.iterator();
			while (rightIterator.hasNext()) {
				Map.Entry<Long, List<Tuple2<RowData, Boolean>>> rightEntry = rightIterator.next();
				Long rightTime = rightEntry.getKey();
				if (rightTime >= rightQualifiedLowerBound && rightTime <= rightQualifiedUpperBound) {
					List<Tuple2<RowData, Boolean>> rightRows = rightEntry.getValue();
					boolean entryUpdated = false;
					for (Tuple2<RowData, Boolean> tuple : rightRows) {
						joinCollector.reset();
						joinFunction.join(leftRow, tuple.f0, joinCollector);
						emitted = emitted || joinCollector.isEmitted();
						if (joinType.isRightOuter()) {
							if (!tuple.f1 && joinCollector.isEmitted()) {
								// Mark the right row as being successfully joined and emitted.
								tuple.f1 = true;
								entryUpdated = true;
							}
						}
					}
					if (entryUpdated) {
						// Write back the edited entry (mark emitted) for the right cache.
						rightEntry.setValue(rightRows);
					}
				}
				// Clean up the expired right cache row, clean the cache while join
				if (rightTime <= rightExpirationTime) {
					if (joinType.isRightOuter()) {
						List<Tuple2<RowData, Boolean>> rightRows = rightEntry.getValue();
						rightRows.forEach((Tuple2<RowData, Boolean> tuple) -> {
							if (!tuple.f1) {
								// Emit a null padding result if the right row has never been successfully joined.
								joinCollector.collect(paddingUtil.padRight(tuple.f0));
							}
						});
					}
					// eager remove
					rightIterator.remove();
				} // We could do the short-cutting optimization here once we get a state with ordered keys.
			}
		}
		// Check if we need to cache the current row.
		if (rightOperatorTime < rightQualifiedUpperBound) {
			// Operator time of right stream has not exceeded the upper window bound of the current
			// row. Put it into the left cache, since later coming records from the right stream are
			// expected to be joined with it.
			List<Tuple2<RowData, Boolean>> leftRowList = leftCache.get(timeForLeftRow);
			if (leftRowList == null) {
				leftRowList = new ArrayList<>(1);
			}
			leftRowList.add(Tuple2.of(leftRow, emitted));
			leftCache.put(timeForLeftRow, leftRowList);
			if (rightTimerState.value() == null) {
				// Register a timer on the RIGHT stream to remove rows.
				registerCleanUpTimer(ctx, timeForLeftRow, true);
			}
		} else if (!emitted && joinType.isLeftOuter()) {
			// Emit a null padding result if the left row is not cached and successfully joined.
			joinCollector.collect(paddingUtil.padLeft(leftRow));
		}
	}

	@Override
	public void processElement2(RowData rightRow, Context ctx, Collector<RowData> out) throws Exception {
		joinCollector.setInnerCollector(out);
		updateOperatorTime(ctx);
		long timeForRightRow = getTimeForRightStream(ctx, rightRow);
		long leftQualifiedLowerBound = timeForRightRow - leftRelativeSize;
		long leftQualifiedUpperBound = timeForRightRow + rightRelativeSize;
		boolean emitted = false;

		// Check if we need to join the current row against cached rows of the left input.
		// The condition here should be leftMinimumTime < leftQualifiedUpperBound.
		// We use leftExpirationTime as an approximation of the leftMinimumTime here,
		// since leftExpirationTime <= leftMinimumTime is always true.
		if (leftExpirationTime < leftQualifiedUpperBound) {
			leftExpirationTime = calExpirationTime(rightOperatorTime, leftRelativeSize);
			// Join the rightRow with rows from the left cache.
			Iterator<Map.Entry<Long, List<Tuple2<RowData, Boolean>>>> leftIterator = leftCache.iterator();
			while (leftIterator.hasNext()) {
				Map.Entry<Long, List<Tuple2<RowData, Boolean>>> leftEntry = leftIterator.next();
				Long leftTime = leftEntry.getKey();
				if (leftTime >= leftQualifiedLowerBound && leftTime <= leftQualifiedUpperBound) {
					List<Tuple2<RowData, Boolean>> leftRows = leftEntry.getValue();
					boolean entryUpdated = false;
					for (Tuple2<RowData, Boolean> tuple : leftRows) {
						joinCollector.reset();
						joinFunction.join(tuple.f0, rightRow, joinCollector);
						emitted = emitted || joinCollector.isEmitted();
						if (joinType.isLeftOuter()) {
							if (!tuple.f1 && joinCollector.isEmitted()) {
								// Mark the left row as being successfully joined and emitted.
								tuple.f1 = true;
								entryUpdated = true;
							}
						}
					}
					if (entryUpdated) {
						// Write back the edited entry (mark emitted) for the right cache.
						leftEntry.setValue(leftRows);
					}
				}

				if (leftTime <= leftExpirationTime) {
					if (joinType.isLeftOuter()) {
						List<Tuple2<RowData, Boolean>> leftRows = leftEntry.getValue();
						leftRows.forEach((Tuple2<RowData, Boolean> tuple) -> {
							if (!tuple.f1) {
								// Emit a null padding result if the left row has never been successfully joined.
								joinCollector.collect(paddingUtil.padLeft(tuple.f0));
							}
						});
					}
					// eager remove
					leftIterator.remove();
				} // We could do the short-cutting optimization here once we get a state with ordered keys.
			}
		}
		// Check if we need to cache the current row.
		if (leftOperatorTime < leftQualifiedUpperBound) {
			// Operator time of left stream has not exceeded the upper window bound of the current
			// row. Put it into the right cache, since later coming records from the left stream are
			// expected to be joined with it.
			List<Tuple2<RowData, Boolean>> rightRowList = rightCache.get(timeForRightRow);
			if (null == rightRowList) {
				rightRowList = new ArrayList<>(1);
			}
			rightRowList.add(Tuple2.of(rightRow, emitted));
			rightCache.put(timeForRightRow, rightRowList);
			if (leftTimerState.value() == null) {
				// Register a timer on the LEFT stream to remove rows.
				registerCleanUpTimer(ctx, timeForRightRow, false);
			}
		} else if (!emitted && joinType.isRightOuter()) {
			// Emit a null padding result if the right row is not cached and successfully joined.
			joinCollector.collect(paddingUtil.padRight(rightRow));
		}
	}

	@Override
	public void onTimer(long timestamp, OnTimerContext ctx, Collector<RowData> out) throws Exception {
		joinCollector.setInnerCollector(out);
		updateOperatorTime(ctx);
		// In the future, we should separate the left and right watermarks. Otherwise, the
		// registered timer of the faster stream will be delayed, even if the watermarks have
		// already been emitted by the source.
		Long leftCleanUpTime = leftTimerState.value();
		if (leftCleanUpTime != null && timestamp == leftCleanUpTime) {
			rightExpirationTime = calExpirationTime(leftOperatorTime, rightRelativeSize);
			removeExpiredRows(
					joinCollector,
					rightExpirationTime,
					rightCache,
					leftTimerState,
					ctx,
					false
			);
		}

		Long rightCleanUpTime = rightTimerState.value();
		if (rightCleanUpTime != null && timestamp == rightCleanUpTime) {
			leftExpirationTime = calExpirationTime(rightOperatorTime, leftRelativeSize);
			removeExpiredRows(
					joinCollector,
					leftExpirationTime,
					leftCache,
					rightTimerState,
					ctx,
					true
			);
		}
	}

	/**
	 * Calculate the expiration time with the given operator time and relative window size.
	 *
	 * @param operatorTime the operator time
	 * @param relativeSize the relative window size
	 * @return the expiration time for cached rows
	 */
	private long calExpirationTime(long operatorTime, long relativeSize) {
		if (operatorTime < Long.MAX_VALUE) {
			return operatorTime - relativeSize - allowedLateness - 1;
		} else {
			// When operatorTime = Long.MaxValue, it means the stream has reached the end.
			return Long.MAX_VALUE;
		}
	}

	/**
	 * Register a timer for cleaning up rows in a specified time.
	 *
	 * @param ctx the context to register timer
	 * @param rowTime time for the input row
	 * @param leftRow whether this row comes from the left stream
	 */
	private void registerCleanUpTimer(Context ctx, long rowTime, boolean leftRow) throws IOException {
		if (leftRow) {
			long cleanUpTime = rowTime + leftRelativeSize + minCleanUpInterval + allowedLateness + 1;
			registerTimer(ctx, cleanUpTime);
			rightTimerState.update(cleanUpTime);
		} else {
			long cleanUpTime = rowTime + rightRelativeSize + minCleanUpInterval + allowedLateness + 1;
			registerTimer(ctx, cleanUpTime);
			leftTimerState.update(cleanUpTime);
		}
	}

	/**
	 * Remove the expired rows. Register a new timer if the cache still holds valid rows
	 * after the cleaning up.
	 *
	 * @param collector the collector to emit results
	 * @param expirationTime the expiration time for this cache
	 * @param rowCache the row cache
	 * @param timerState timer state for the opposite stream
	 * @param ctx the context to register the cleanup timer
	 * @param removeLeft whether to remove the left rows
	 */
	private void removeExpiredRows(
			Collector<RowData> collector,
			long expirationTime,
			MapState<Long, List<Tuple2<RowData, Boolean>>> rowCache,
			ValueState<Long> timerState,
			OnTimerContext ctx,
			boolean removeLeft) throws Exception {
		Iterator<Map.Entry<Long, List<Tuple2<RowData, Boolean>>>> iterator = rowCache.iterator();

		long earliestTimestamp = -1L;

		// We remove all expired keys and do not leave the loop early.
		// Hence, we do a full pass over the state.
		while (iterator.hasNext()) {
			Map.Entry<Long, List<Tuple2<RowData, Boolean>>> entry = iterator.next();
			Long rowTime = entry.getKey();
			if (rowTime <= expirationTime) {
				if (removeLeft && joinType.isLeftOuter()) {
					List<Tuple2<RowData, Boolean>> rows = entry.getValue();
					rows.forEach((Tuple2<RowData, Boolean> tuple) -> {
						if (!tuple.f1) {
							// Emit a null padding result if the row has never been successfully joined.
							collector.collect(paddingUtil.padLeft(tuple.f0));
						}
					});
				} else if (!removeLeft && joinType.isRightOuter()) {
					List<Tuple2<RowData, Boolean>> rows = entry.getValue();
					rows.forEach((Tuple2<RowData, Boolean> tuple) -> {
						if (!tuple.f1) {
							// Emit a null padding result if the row has never been successfully joined.
							collector.collect(paddingUtil.padRight(tuple.f0));
						}
					});
				}
				iterator.remove();
			} else {
				// We find the earliest timestamp that is still valid.
				if (rowTime < earliestTimestamp || earliestTimestamp < 0) {
					earliestTimestamp = rowTime;
				}
			}
		}

		if (earliestTimestamp > 0) {
			// There are rows left in the cache. Register a timer to expire them later.
			registerCleanUpTimer(ctx, earliestTimestamp, removeLeft);
		} else {
			// No rows left in the cache. Clear the states and the timerState will be 0.
			timerState.clear();
			rowCache.clear();
		}

	}

	/**
	 * Update the operator time of the two streams.
	 * Must be the first call in all processing methods (i.e., processElement(), onTimer()).
	 *
	 * @param ctx the context to acquire watermarks
	 */
	abstract void updateOperatorTime(Context ctx);

	/**
	 * Return the time for the target row from the left stream.
	 * Requires that [[updateOperatorTime()]] has been called before.
	 *
	 * @param ctx the runtime context
	 * @param row the target row
	 * @return time for the target row
	 */
	abstract long getTimeForLeftStream(Context ctx, RowData row);

	/**
	 * Return the time for the target row from the right stream.
	 * Requires that [[updateOperatorTime()]] has been called before.
	 *
	 * @param ctx the runtime context
	 * @param row the target row
	 * @return time for the target row
	 */
	abstract long getTimeForRightStream(Context ctx, RowData row);

	/**
	 * Register a proctime or rowtime timer.
	 *
	 * @param ctx the context to register the timer
	 * @param cleanupTime timestamp for the timer
	 */
	abstract void registerTimer(Context ctx, long cleanupTime);

}
