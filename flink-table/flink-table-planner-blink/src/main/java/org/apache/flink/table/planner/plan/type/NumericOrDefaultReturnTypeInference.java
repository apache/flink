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

package org.apache.flink.table.planner.plan.type;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * Determine the return type of functions with numeric arguments.
 * The return type is the type of the argument with the largest range.
 * We start to consider the arguments from the `startTypeIdx`-th one.
 * If one of the arguments is not of numeric type,
 * we return the type of the `defaultTypeIdx`-th argument instead.
 */
public class NumericOrDefaultReturnTypeInference implements SqlReturnTypeInference {
	// Default argument whose type is returned
	// when one of the arguments from the `startTypeIdx`-th isn't of numeric type.
	private int defaultTypeIdx;
	// We check from the `startTypeIdx`-th argument that
	// if all the following arguments are of numeric type.
	// Previous arguments are ignored.
	private int startTypeIdx;

	public NumericOrDefaultReturnTypeInference(int defaultTypeIdx) {
		this(defaultTypeIdx, 0);
	}

	public NumericOrDefaultReturnTypeInference(int defaultTypeIdx, int startTypeIdx) {
		this.defaultTypeIdx = defaultTypeIdx;
		this.startTypeIdx = startTypeIdx;
	}

	@Override
	public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
		int nOperands = opBinding.getOperandCount();
		List<RelDataType> types = new ArrayList<>();
		for (int i = startTypeIdx; i < nOperands; i++) {
			RelDataType type = opBinding.getOperandType(i);
			if (SqlTypeUtil.isNumeric(type)) {
				types.add(type);
			} else {
				return opBinding.getOperandType(defaultTypeIdx);
			}
		}
		return opBinding.getTypeFactory().leastRestrictive(types);
	}
}
