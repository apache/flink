/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copysecond ownership.
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

package org.apache.flink.table.runtime.join.stream.bundle;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.state.keyed.KeyedValueState;
import org.apache.flink.streaming.api.bundle.CoBundleTrigger;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.table.codegen.GeneratedJoinConditionFunction;
import org.apache.flink.table.codegen.GeneratedProjection;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.table.runtime.join.stream.state.JoinKeyContainPrimaryKeyStateHandler;
import org.apache.flink.table.runtime.join.stream.state.JoinStateHandler;
import org.apache.flink.table.runtime.join.stream.state.match.JoinMatchStateHandler;
import org.apache.flink.table.runtime.join.stream.state.match.OnlyEqualityConditionMatchStateHandler;
import org.apache.flink.table.typeutils.BaseRowTypeInfo;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.table.dataformat.util.BaseRowUtil.ACCUMULATE_MSG;
import static org.apache.flink.table.dataformat.util.BaseRowUtil.RETRACT_MSG;

/**
 * Base MiniBatch operator for outer join.
 */
@Internal
abstract class MiniBatchOuterJoinStreamOperator extends MiniBatchJoinStreamOperator {

	private static final long serialVersionUID = 1L;

	protected final JoinMatchStateHandler.Type leftMatchStateType;
	protected final JoinMatchStateHandler.Type rightMatchStateType;

	protected transient JoinMatchStateHandler leftMatchStateHandler;
	protected transient JoinMatchStateHandler rightMatchStateHandler;

	protected transient BaseRow leftSideNullRow;
	protected transient BaseRow rightSideNullRow;

	public MiniBatchOuterJoinStreamOperator(
			BaseRowTypeInfo leftType,
			BaseRowTypeInfo rightType, GeneratedJoinConditionFunction condFuncCode,
			KeySelector<BaseRow, BaseRow> leftKeySelector,
			KeySelector<BaseRow, BaseRow> rightKeySelector,
			GeneratedProjection leftPkProjectCode, GeneratedProjection rightPkProjectCode,
			JoinStateHandler.Type leftJoinStateType,
			JoinStateHandler.Type rightJoinStateType, long maxRetentionTime, long minRetentionTime,
			JoinMatchStateHandler.Type leftMatchStateType,
			JoinMatchStateHandler.Type rightMatchStateType, Boolean leftIsAccRetract, Boolean rightIsAccRetract,
			boolean[] filterNullKeys, CoBundleTrigger<BaseRow, BaseRow> coBundleTrigger,
			boolean finishBundleBeforeSnapshot) {
		super(leftType, rightType, condFuncCode, leftKeySelector, rightKeySelector, leftPkProjectCode,
				rightPkProjectCode, leftJoinStateType, rightJoinStateType, maxRetentionTime,
				minRetentionTime, leftIsAccRetract, rightIsAccRetract, filterNullKeys, coBundleTrigger,
				finishBundleBeforeSnapshot);
		this.leftMatchStateType = leftMatchStateType;
		this.rightMatchStateType = rightMatchStateType;
	}

	@Override
	public void open() throws Exception {
		super.open();
		this.leftSideNullRow = new GenericRow(leftType.getArity());
		this.rightSideNullRow = new GenericRow(rightType.getArity());
		LOG.info("leftJoinStateType {}, rightJoinStateType {}, leftMatchStateType {}, rightMatchStateType {}",
				leftJoinStateType, rightJoinStateType, leftMatchStateType, rightMatchStateType);
	}

	@Override
	protected void initAllStates() throws Exception {
		super.initAllStates();

		this.leftMatchStateHandler = createMatchStateHandler(
			leftType, leftMatchStateType, leftKeyType, "LeftMatchHandler", leftPkProjectCode);
		this.rightMatchStateHandler = createMatchStateHandler(
			rightType, rightMatchStateType, rightKeyType, "RightMatchHandler", rightPkProjectCode);
	}

	@Override
	public void onProcessingTime(InternalTimer<BaseRow, Byte> timer) throws Exception {
		byte namespace = timer.getNamespace();
		if (namespace == 1) {
			//left
			if (needToCleanupState(timer.getKey(), timer.getTimestamp(), leftTimerState)) {
				leftMatchStateHandler.remove(timer.getKey());
				leftStateHandler.remove(timer.getKey());
			}
		} else {
			//right
			if (needToCleanupState(timer.getKey(), timer.getTimestamp(), rightTimerState)) {
				rightMatchStateHandler.remove(timer.getKey());
				rightStateHandler.remove(timer.getKey());
			}
		}
	}

	public void processSingleSideBundles(Map<BaseRow, List<BaseRow>> inputSide,
			Map<BaseRow, List<BaseRow>> otherSide,
			JoinStateHandler.Type inputSideJoinStateType,
			JoinStateHandler.Type otherSideJoinStateType,
			JoinStateHandler inputSideStateHandler,
			JoinStateHandler otherSideStateHandler,
			JoinMatchStateHandler inputSideMatchStateHandler,
			JoinMatchStateHandler otherSideMatchStateHandler,
			KeyedValueState<BaseRow, Long> timerState,
			Boolean inputSideIsLeft,
			boolean inputIsOuter, boolean otherSideIsOuter,
			Collector<BaseRow> out) throws Exception {

		// batch get values for all keys if other side is valuestate
		if (otherSideJoinStateType == JoinStateHandler.Type.JOIN_KEY_CONTAIN_PRIMARY_KEY) {
			Set<BaseRow> keySet = new HashSet<>();
			for (BaseRow stateKey: inputSide.keySet()) {
				keySet.add(stateKey);
			}
			otherSideStateHandler.batchGet(keySet);
		}

		Map<BaseRow, BaseRow> putMap = new HashMap<>();
		Set<BaseRow> deleteSet = new HashSet<>();
		Boolean isAccRetract = inputSideIsLeft ? leftIsAccRetract : rightIsAccRetract;
		for (Map.Entry<BaseRow, List<BaseRow>> entry: inputSide.entrySet()) {
			List<Tuple2<BaseRow, Long>> reducedList = reduceCurrentList(entry.getValue(),
					inputSideStateHandler, isAccRetract);
			joinCurrentList(
					entry.getKey(),
					reducedList,
					inputSideStateHandler,
					otherSideStateHandler,
					inputSideMatchStateHandler,
					otherSideMatchStateHandler,
					inputSideIsLeft, inputIsOuter, otherSideIsOuter,
					timerState,
					stateCleaningEnabled);

			if (inputSideJoinStateType == JoinStateHandler.Type.JOIN_KEY_CONTAIN_PRIMARY_KEY) {
				// update putMap and deleteSet
				Tuple2<BaseRow, Long> lastTuple = null;
				for (Tuple2<BaseRow, Long> tuple2: reducedList) {
					lastTuple = tuple2;
				}
				if (lastTuple != null) {
					if (lastTuple.f1 < 0) {
						deleteSet.add(entry.getKey());
					} else {
						putMap.put(entry.getKey(), lastTuple.f0);
					}
				}
			}
		}

		if (inputSideJoinStateType == JoinStateHandler.Type.JOIN_KEY_CONTAIN_PRIMARY_KEY) {
			inputSideStateHandler.putAll(putMap);
			inputSideStateHandler.removeAll(deleteSet);
		}
	}

	/**
	 * 1. batch update current side state
	 * 2. loop other side state
	 *     2.1 get other side old JoinCnt
	 *     2.2 loop list calc delta matchCnt
	 *     2.3 get other side new JoinCnt
	 *     2.4 JoinCnt from 0 to n. retract null
	 *     2.5 loop list to output joined Row
	 *     2.6 JoinCnt from n to 0. output null
	 *     2.7 update other side joinCnt
	 * 3. loop list output null if matchCnt is 0
	 * 4. update current side joinCnt
	 */
	private void joinCurrentList(
			BaseRow currentJoinKey,
			List<Tuple2<BaseRow, Long>> inputList,
			JoinStateHandler inputSideStateHandler,
			JoinStateHandler otherSideStateHandler,
			JoinMatchStateHandler inputSideMatchStateHandler,
			JoinMatchStateHandler otherSideMatchStateHandler,
			boolean inputIsLeft, boolean inputIsOuter, boolean otherSideIsOuter,
			KeyedValueState<BaseRow, Long> timerState,
			boolean cleaningBasedTimer) throws Exception {

		inputSideStateHandler.setCurrentJoinKey(currentJoinKey);
		long currentTime = internalTimerService.currentProcessingTime();
		registerProcessingCleanupTimer(inputSideStateHandler.getCurrentJoinKey(), currentTime, inputIsLeft, timerState);

		Iterator<Tuple3<BaseRow, Long, Long>> iterator = null;
		if (otherSideStateHandler instanceof JoinKeyContainPrimaryKeyStateHandler) {
			iterator = ((JoinKeyContainPrimaryKeyStateHandler) otherSideStateHandler).getRecordsFromCache(currentJoinKey);
		} else {
			iterator = otherSideStateHandler.getRecords(currentJoinKey);
		}

		long [] updateStatus = inputSideStateHandler.batchUpdate(currentJoinKey, inputList, currentTime + maxRetentionTime);
		long[] inputSideJoinCnt = new long[inputList.size()];
		long otherSideOldJoinCnt = 0;
		long otherSideNewJoinCnt = 0;

		// for OnlyEqualityConditionMatchStateHandler, we get old join cnt once
		if (otherSideMatchStateHandler instanceof OnlyEqualityConditionMatchStateHandler) {
			otherSideMatchStateHandler.extractCurrentRowMatchJoinCount(currentJoinKey, null, 0);
		}
		while (iterator.hasNext()) {
			Tuple3<BaseRow, Long, Long> tuple3 = iterator.next();
			BaseRow matchRow = tuple3.f0;
			long matchRowCount = tuple3.f1;

			if (!(otherSideMatchStateHandler instanceof OnlyEqualityConditionMatchStateHandler)) {
				otherSideMatchStateHandler.extractCurrentRowMatchJoinCount(currentJoinKey, matchRow, 0);
			}
			otherSideOldJoinCnt = otherSideMatchStateHandler.getCurrentRowMatchJoinCnt();

			int idx = 0;
			long matchCnt = 0;
			for (Tuple2<BaseRow, Long> tuple2: inputList) {
				if (inputIsLeft) {
					if (applyCondition(tuple2.f0, matchRow,
						inputSideStateHandler.getCurrentJoinKey())) {
						matchCnt += tuple2.f1;
						inputSideJoinCnt[idx] += matchRowCount;
					}
				} else {
					if (applyCondition(matchRow, tuple2.f0,
						inputSideStateHandler.getCurrentJoinKey())) {
						matchCnt += tuple2.f1;
						inputSideJoinCnt[idx] += matchRowCount;
					}
				}
				idx++;
			}
			otherSideNewJoinCnt = otherSideOldJoinCnt + matchCnt;
			if (otherSideIsOuter && otherSideOldJoinCnt == 0 && otherSideNewJoinCnt > 0) {
				joinedRow.setHeader(RETRACT_MSG);
				if (inputIsLeft) {
					collectResult(joinedRow.replace(leftSideNullRow, matchRow), matchRowCount);
				} else {
					collectResult(joinedRow.replace(matchRow, rightSideNullRow), matchRowCount);
				}
			}
			for (Tuple2<BaseRow, Long> tuple2: inputList) {
				joinedRow.setHeader(tuple2.f1 < 0 ? RETRACT_MSG : ACCUMULATE_MSG);
				if (inputIsLeft) {
					if (applyCondition(tuple2.f0, matchRow,
						inputSideStateHandler.getCurrentJoinKey())) {
						collectResult(joinedRow.replace(tuple2.f0, matchRow), tuple2.f1 * matchRowCount);
					}
				} else {
					if (applyCondition(matchRow, tuple2.f0,
						inputSideStateHandler.getCurrentJoinKey())) {
						collectResult(joinedRow.replace(matchRow, tuple2.f0), tuple2.f1 * matchRowCount);
					}
				}
			}
			if (otherSideIsOuter && otherSideOldJoinCnt > 0 && otherSideNewJoinCnt <= 0) {
				joinedRow.setHeader(ACCUMULATE_MSG);
				if (inputIsLeft) {
					collectResult(joinedRow.replace(leftSideNullRow, matchRow), matchRowCount);
				} else {
					collectResult(joinedRow.replace(matchRow, rightSideNullRow), matchRowCount);
				}
			}

			if (!(otherSideMatchStateHandler instanceof OnlyEqualityConditionMatchStateHandler)) {
				otherSideMatchStateHandler.updateRowMatchJoinCnt(currentJoinKey, matchRow, otherSideNewJoinCnt);
			}

			if (tuple3.f2 < currentTime && stateCleaningEnabled) {
				//expire
				iterator.remove();
				otherSideMatchStateHandler.remove(currentJoinKey, matchRow);
			}
		}

		// for OnlyEqualityConditionMatchStateHandler, we update new join cnt once
		if (otherSideMatchStateHandler instanceof OnlyEqualityConditionMatchStateHandler &&
				otherSideNewJoinCnt != otherSideOldJoinCnt) {
			otherSideMatchStateHandler.updateRowMatchJoinCnt(currentJoinKey, null, otherSideNewJoinCnt);
		}

		if (inputIsOuter) {
			int idx = 0;
			Set<BaseRow> deleteSet = new HashSet<>();
			Map<BaseRow, Long> addMap = new HashMap();
			for (Tuple2<BaseRow, Long> tuple2: inputList) {
				if (inputSideJoinCnt[idx] == 0) {
					joinedRow.setHeader(tuple2.f1 < 0 ? RETRACT_MSG : ACCUMULATE_MSG);
					if (inputIsLeft) {
						collectResult(joinedRow.replace(tuple2.f0, rightSideNullRow), tuple2.f1);
					} else {
						collectResult(joinedRow.replace(leftSideNullRow, tuple2.f0), tuple2.f1);
					}
				}
				if (updateStatus[idx] == -1) {
					deleteSet.add(tuple2.f0);
				} else if (updateStatus[idx] == 1) {
					addMap.put(tuple2.f0, inputSideJoinCnt[idx]);
				}
				idx++;
			}
			// batch update and delete
			inputSideMatchStateHandler.removeAll(currentJoinKey, deleteSet);
			inputSideMatchStateHandler.addAll(currentJoinKey, addMap);
		}
	}
}
