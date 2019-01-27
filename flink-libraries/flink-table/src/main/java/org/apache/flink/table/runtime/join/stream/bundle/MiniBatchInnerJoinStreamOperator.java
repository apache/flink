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
import org.apache.flink.streaming.api.operators.TwoInputSelection;
import org.apache.flink.table.codegen.GeneratedJoinConditionFunction;
import org.apache.flink.table.codegen.GeneratedProjection;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.runtime.join.stream.state.JoinKeyContainPrimaryKeyStateHandler;
import org.apache.flink.table.runtime.join.stream.state.JoinStateHandler;
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
 * Inner MiniBatch Join operator.
 */
@Internal
public class MiniBatchInnerJoinStreamOperator extends MiniBatchJoinStreamOperator {

	private static final long serialVersionUID = 1L;

	public MiniBatchInnerJoinStreamOperator(
			BaseRowTypeInfo leftType,
			BaseRowTypeInfo rightType, GeneratedJoinConditionFunction condFuncCode,
			KeySelector<BaseRow, BaseRow> leftKeySelector,
			KeySelector<BaseRow, BaseRow> rightKeySelector,
			GeneratedProjection leftPkProjectCode, GeneratedProjection rightPkProjectCode,
			JoinStateHandler.Type leftJoinStateType,
			JoinStateHandler.Type rightJoinStateType, long maxRetentionTime, long minRetentionTime,
			Boolean leftIsAccRetract, Boolean rightIsAccRetract,
			boolean[] filterNullKeys, CoBundleTrigger<BaseRow, BaseRow> coBundleTrigger,
			boolean finishBundleBeforeSnapshot) {
		super(leftType, rightType, condFuncCode, leftKeySelector, rightKeySelector, leftPkProjectCode,
				rightPkProjectCode, leftJoinStateType, rightJoinStateType, maxRetentionTime, minRetentionTime,
				leftIsAccRetract, rightIsAccRetract, filterNullKeys, coBundleTrigger, finishBundleBeforeSnapshot);
	}

	@Override
	public void open() throws Exception {
		super.open();
		LOG.info("Init MiniBatchInnerJoinStreamOperator");
		LOG.info("leftJoinStateType {}, rightJoinStateType {}", leftJoinStateType, rightJoinStateType);
	}

	public void processSingleSideBundles(Map<BaseRow, List<BaseRow>> inputSide,
			JoinStateHandler.Type inputSideJoinStateType,
			JoinStateHandler.Type otherSideJoinStateType,
			JoinStateHandler inputSideStateHandler,
			JoinStateHandler otherSideStateHandler,
			KeyedValueState<BaseRow, Long> timerState,
			Boolean inputSideIsLeft,
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
					inputSideIsLeft,
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

	@Override
	public void processBundles(
			Map<BaseRow, List<BaseRow>> left,
			Map<BaseRow, List<BaseRow>> right,
			Collector<BaseRow> out) throws Exception {

		processSingleSideBundles(right, rightJoinStateType, leftJoinStateType,
				rightStateHandler, leftStateHandler, rightTimerState, false, out);
		processSingleSideBundles(left, leftJoinStateType, rightJoinStateType,
				leftStateHandler, rightStateHandler, leftTimerState, true, out);
	}

	@Override
	public void onProcessingTime(InternalTimer<BaseRow, Byte> timer) throws Exception {
		byte namespace = timer.getNamespace();
		if (namespace == 1) {
			//left
			if (needToCleanupState(timer.getKey(), timer.getTimestamp(), leftTimerState)) {
				leftStateHandler.remove(timer.getKey());
			}

		} else {
			//right
			if (needToCleanupState(timer.getKey(), timer.getTimestamp(), rightTimerState)) {
				rightStateHandler.remove(timer.getKey());
			}
		}
	}

	private void joinCurrentList(
			BaseRow currentJoinKey,
			List<Tuple2<BaseRow, Long>> inputList,
			JoinStateHandler inputStateHandler,
			JoinStateHandler otherStateHandler,
			boolean inputIsLeft, KeyedValueState<BaseRow, Long> timerState,
			boolean cleaningBasedTimer) throws Exception {

		inputStateHandler.setCurrentJoinKey(currentJoinKey);
		long currentTime = internalTimerService.currentProcessingTime();
		inputStateHandler.batchUpdate(currentJoinKey, inputList, currentTime + maxRetentionTime);
		registerProcessingCleanupTimer(inputStateHandler.getCurrentJoinKey(), currentTime, inputIsLeft, timerState);

		Iterator<Tuple3<BaseRow, Long, Long>> iterator = null;
		if (otherStateHandler instanceof JoinKeyContainPrimaryKeyStateHandler) {
			iterator = ((JoinKeyContainPrimaryKeyStateHandler) otherStateHandler).getRecordsFromCache(currentJoinKey);
		} else {
			iterator = otherStateHandler.getRecords(currentJoinKey);
		}

		// loop other side state
		while (iterator.hasNext()) {
			Tuple3<BaseRow, Long, Long> tuple3 = iterator.next();
			BaseRow otherRow = tuple3.f0;
			Long count = tuple3.f1;
			if (inputIsLeft) {
				for (Tuple2<BaseRow, Long> tuple2: inputList) {
					BaseRow input = tuple2.f0;
					byte header = tuple2.f1 < 0 ? RETRACT_MSG : ACCUMULATE_MSG;
					joinedRow.setHeader(header);
					if (applyCondition(input, otherRow,
						inputStateHandler.getCurrentJoinKey())) {
						collectResult(joinedRow.replace(input, otherRow), count * tuple2.f1);
					}
				}
			} else {
				for (Tuple2<BaseRow, Long> tuple2: inputList) {
					BaseRow input = tuple2.f0;
					byte header = tuple2.f1 < 0 ? RETRACT_MSG : ACCUMULATE_MSG;
					joinedRow.setHeader(header);
					if (applyCondition(otherRow, input,
						inputStateHandler.getCurrentJoinKey())) {
						collectResult(joinedRow.replace(otherRow, input), count * tuple2.f1);
					}
				}
			}
			if (tuple3.f2 <= currentTime && stateCleaningEnabled) {
				//expire
				iterator.remove();
			}
		}
	}

	@Override
	public TwoInputSelection firstInputSelection() {
		return TwoInputSelection.ANY;
	}
}
