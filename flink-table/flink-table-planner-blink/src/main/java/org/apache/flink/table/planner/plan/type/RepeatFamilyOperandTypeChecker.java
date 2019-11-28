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

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.Arrays;

import static org.apache.calcite.util.Static.RESOURCE;


/**
 * Parameter type-checking strategy where there must be more than one operands,
 * and all operands must have a same specific SqlTypeFamily type.
 */
public class RepeatFamilyOperandTypeChecker implements SqlOperandTypeChecker {

	protected final SqlTypeFamily family;

	public RepeatFamilyOperandTypeChecker(SqlTypeFamily family) {
		this.family = family;
	}

	public boolean checkSingleOperandType(
		SqlCallBinding callBinding,
		SqlNode node,
		boolean throwOnFailure) {

		if (SqlUtil.isNullLiteral(node, false)) {
			if (throwOnFailure) {
				throw callBinding.getValidator().newValidationError(node,
					RESOURCE.nullIllegal());
			} else {
				return false;
			}
		}

		RelDataType type = callBinding.getValidator().deriveType(
			callBinding.getScope(),
			node);
		SqlTypeName typeName = type.getSqlTypeName();

		// Pass type checking for operators if it's of type 'ANY'.
		if (typeName.getFamily() == SqlTypeFamily.ANY) {
			return true;
		}

		if (!family.getTypeNames().contains(typeName)) {
			if (throwOnFailure) {
				throw callBinding.newValidationSignatureError();
			}
			return false;
		}
		return true;
	}

	public boolean checkOperandTypes(
		SqlCallBinding callBinding,
		boolean throwOnFailure) {

		for (Ord<SqlNode> op : Ord.zip(callBinding.operands())) {
			if (!checkSingleOperandType(
				callBinding,
				op.e,
				false)) {
				// TODO: check type coercion when we support implicit type conversion
				// recheck to validate.
				for (Ord<SqlNode> op1 : Ord.zip(callBinding.operands())) {
					if (!checkSingleOperandType(
						callBinding,
						op1.e,
						throwOnFailure)) {
						return false;
					}
				}
				return false;
			}
		}
		return true;
	}

	public SqlOperandCountRange getOperandCountRange() {
		return SqlOperandCountRanges.from(1);
	}

	public String getAllowedSignatures(SqlOperator op, String opName) {
		return SqlUtil.getAliasedSignature(op, opName, Arrays.asList(family.toString(), "..."));
	}

	public Consistency getConsistency() {
		return Consistency.NONE;
	}

	public boolean isOptional(int i) {
		return false;
	}
}

// End RepeatFamilyOperandTypeChecker.java
