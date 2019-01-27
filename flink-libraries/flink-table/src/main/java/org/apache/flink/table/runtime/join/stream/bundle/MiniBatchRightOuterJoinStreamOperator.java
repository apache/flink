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

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.bundle.CoBundleTrigger;
import org.apache.flink.streaming.api.operators.TwoInputSelection;
import org.apache.flink.table.codegen.GeneratedJoinConditionFunction;
import org.apache.flink.table.codegen.GeneratedProjection;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.runtime.join.stream.state.JoinStateHandler;
import org.apache.flink.table.runtime.join.stream.state.match.JoinMatchStateHandler;
import org.apache.flink.table.typeutils.BaseRowTypeInfo;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

/**
 * MiniBatch Right outer join operator.
 */
public class MiniBatchRightOuterJoinStreamOperator extends MiniBatchOuterJoinStreamOperator {

	public MiniBatchRightOuterJoinStreamOperator(
			BaseRowTypeInfo leftType,
			BaseRowTypeInfo rightType, GeneratedJoinConditionFunction condFuncCode,
			KeySelector<BaseRow, BaseRow> leftKeySelector,
			KeySelector<BaseRow, BaseRow> rightKeySelector,
			GeneratedProjection leftPkProjectCode, GeneratedProjection rightPkProjectCode,
			JoinStateHandler.Type leftJoinTableType,
			JoinStateHandler.Type rightJoinTableType, long maxRetentionTime, long minRetentionTime,
			JoinMatchStateHandler.Type leftMatchStateType,
			JoinMatchStateHandler.Type rightMatchStateType, Boolean leftIsAccRetract, Boolean rightIsAccRetract,
			boolean[] filterNullKeys, CoBundleTrigger<BaseRow, BaseRow> coBundleTrigger,
			boolean finishBundleBeforeSnapshot) {
		super(leftType, rightType, condFuncCode, leftKeySelector, rightKeySelector, leftPkProjectCode,
				rightPkProjectCode, leftJoinTableType, rightJoinTableType, maxRetentionTime, minRetentionTime,
				leftMatchStateType, rightMatchStateType, leftIsAccRetract, rightIsAccRetract,
				filterNullKeys, coBundleTrigger, finishBundleBeforeSnapshot);
	}

	@Override
	public void open() throws Exception {
		super.open();
		LOG.info("Init MiniBatchRightOuterJoinStreamOperator.");
	}

	@Override
	public void processBundles(
			Map<BaseRow, List<BaseRow>> left,
			Map<BaseRow, List<BaseRow>> right,
			Collector<BaseRow> out) throws Exception {

		// more efficient to process left first for right out join, i.e, some retractions can be avoided

		// process left
		processSingleSideBundles(left, right, leftJoinStateType, rightJoinStateType, leftStateHandler,
			rightStateHandler, leftMatchStateHandler, rightMatchStateHandler, leftTimerState, true, false, true, out);

		// process right
		processSingleSideBundles(right, left, rightJoinStateType, leftJoinStateType, rightStateHandler,
				leftStateHandler, rightMatchStateHandler, leftMatchStateHandler, rightTimerState, false, true, false, out);
	}

	@Override
	public TwoInputSelection firstInputSelection() {
		return TwoInputSelection.ANY;
	}
}
