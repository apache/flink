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

import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Parameter type-checking strategy where all operand types except first one must be numeric type.
 */
public class NumericExceptFirstOperandChecker implements SqlOperandTypeChecker {

	private int nOperands;

	public NumericExceptFirstOperandChecker(int nOperands) {
		this.nOperands = nOperands;
	}

	@Override
	public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
		for (int i = 1; i < callBinding.getOperandCount(); i++) {
			if (!SqlTypeUtil.isNumeric(callBinding.getOperandType(i))) {
				if (!throwOnFailure) {
					return false;
				}
				throw callBinding.newValidationSignatureError();
			}
		}
		return true;
	}

	@Override
	public SqlOperandCountRange getOperandCountRange() {
		if (nOperands == -1) {
			return SqlOperandCountRanges.any();
		} else {
			return SqlOperandCountRanges.of(nOperands);
		}
	}

	@Override
	public String getAllowedSignatures(SqlOperator op, String opName) {
		final String anyType = "ANY_TYPE";
		final String numericType = "NUMERIC_TYPE";

		if (nOperands == -1) {
			return SqlUtil.getAliasedSignature(op, opName,
				Arrays.asList(anyType, numericType, "..."));
		} else {
			List<String> types = new ArrayList<>();
			types.add(anyType);
			types.addAll(Collections.nCopies(nOperands - 1, numericType));
			return SqlUtil.getAliasedSignature(op, opName, types);
		}
	}

	@Override
	public Consistency getConsistency() {
		return Consistency.NONE;
	}

	@Override
	public boolean isOptional(int i) {
		return false;
	}
}
