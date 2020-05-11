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
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;

/**
 * The function to execute processing time bounded stream inner-join.
 */
public final class ProcTimeBoundedStreamJoin extends TimeBoundedStreamJoin {

	private static final long serialVersionUID = 9204647938032023101L;

	public ProcTimeBoundedStreamJoin(
			FlinkJoinType joinType,
			long leftLowerBound,
			long leftUpperBound,
			RowDataTypeInfo leftType,
			RowDataTypeInfo rightType,
			GeneratedFunction<FlatJoinFunction<RowData, RowData, RowData>> genJoinFunc) {
		super(joinType, leftLowerBound, leftUpperBound, 0L, leftType, rightType, genJoinFunc);
	}

	@Override
	void updateOperatorTime(Context ctx) {
		leftOperatorTime = ctx.timerService().currentProcessingTime();
		rightOperatorTime = leftOperatorTime;
	}

	@Override
	long getTimeForLeftStream(Context ctx, RowData row) {
		return leftOperatorTime;
	}

	@Override
	long getTimeForRightStream(Context ctx, RowData row) {
		return rightOperatorTime;
	}

	@Override
	void registerTimer(Context ctx, long cleanupTime) {
		ctx.timerService().registerProcessingTimeTimer(cleanupTime);
	}
}
