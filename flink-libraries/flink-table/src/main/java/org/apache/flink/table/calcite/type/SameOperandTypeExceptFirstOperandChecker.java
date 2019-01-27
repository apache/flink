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

package org.apache.flink.table.calcite.type;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.type.SameOperandTypeChecker;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Parameter type-checking strategy where all operand types except first one must be the same.
 */
public class SameOperandTypeExceptFirstOperandChecker extends SameOperandTypeChecker {

	private final String firstOperandTypeName;

	public SameOperandTypeExceptFirstOperandChecker(int nOperands, String firstOperandTypeName) {
		super(nOperands);
		this.firstOperandTypeName = firstOperandTypeName;
	}

	//~ Methods ----------------------------------------------------------------

	protected boolean checkOperandTypesImpl(
		SqlOperatorBinding operatorBinding,
		boolean throwOnFailure,
		SqlCallBinding callBinding) {
		int nOperandsActual = nOperands;
		if (nOperandsActual == -1) {
			nOperandsActual = operatorBinding.getOperandCount();
		}
		assert !(throwOnFailure && (callBinding == null));
		RelDataType[] types = new RelDataType[nOperandsActual];
		final List<Integer> operandList =
			getOperandList(operatorBinding.getOperandCount());
		for (int i : operandList) {
			if (operatorBinding.isOperandNull(i, false)) {
				if (throwOnFailure) {
					throw callBinding.getValidator().newValidationError(
						callBinding.operand(i), RESOURCE.nullIllegal());
				} else {
					return false;
				}
			}
			types[i] = operatorBinding.getOperandType(i);
		}
		int prev = -1;
		for (int i : operandList) {
			if (prev >= 1) {
				if (!types[i].getSqlTypeName().equals(types[prev].getSqlTypeName())) {
					if (!throwOnFailure) {
						return false;
					}

					// REVIEW jvs 5-June-2005: Why don't we use
					// newValidationSignatureError() here?  It gives more
					// specific diagnostics.
					throw callBinding.newValidationError(
						RESOURCE.needSameTypeParameter());
				}
			}
			prev = i;
		}
		return true;
	}

	public String getAllowedSignatures(SqlOperator op, String opName) {
		final String typeName = getTypeName();
		if (nOperands == -1) {
			return SqlUtil.getAliasedSignature(op, opName,
				Arrays.asList(typeName, typeName, "..."));
		} else {
			List<String> types = new ArrayList<>();
			types.add(firstOperandTypeName);
			types.addAll(Collections.nCopies(nOperands - 1, typeName));
			return SqlUtil.getAliasedSignature(op, opName, types);
		}
	}
}
