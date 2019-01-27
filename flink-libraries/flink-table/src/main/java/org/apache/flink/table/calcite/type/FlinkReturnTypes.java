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

import org.apache.flink.table.api.types.DecimalType;
import org.apache.flink.table.dataformat.Decimal;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeTransforms;

import java.math.BigDecimal;

/**
 * Type inference in Flink.
 */
public class FlinkReturnTypes {

	/**
	 * ROUND(num [,len]) type inference.
	 */
	public static final SqlReturnTypeInference ROUND_FUNCTION = new SqlReturnTypeInference() {
		@Override
		public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
			final RelDataType numType = opBinding.getOperandType(0);
			if (numType.getSqlTypeName() != SqlTypeName.DECIMAL) {
				return numType;
			}
			final BigDecimal lenVal;
			if (opBinding.getOperandCount() == 1) {
				lenVal = BigDecimal.ZERO;
			} else if (opBinding.getOperandCount() == 2) {
				lenVal = getArg1Literal(opBinding); // may return null
			} else {
				throw new AssertionError();
			}
			if (lenVal == null) {
				return numType; //
			}
			// ROUND( decimal(p,s), r )
			final int p = numType.getPrecision();
			final int s = numType.getScale();
			final int r = lenVal.intValueExact();
			DecimalType dt = Decimal.inferRoundType(p, s, r);
			return opBinding.getTypeFactory().createSqlType(
				SqlTypeName.DECIMAL, dt.precision(), dt.scale());
		}

		private BigDecimal getArg1Literal(SqlOperatorBinding opBinding) {
			try {
				return opBinding.getOperandLiteralValue(1, BigDecimal.class);
			} catch (Throwable e) {
				return null;
			}
		}
	};

	public static final SqlReturnTypeInference ROUND_FUNCTION_NULLABLE =
		ReturnTypes.cascade(ROUND_FUNCTION, SqlTypeTransforms.TO_NULLABLE);

	public static final SqlReturnTypeInference NUMERIC_FROM_ARG1_DEFAULT1 =
		new NumericOrDefaultReturnTypeInference(1, 1);

	public static final SqlReturnTypeInference NUMERIC_FROM_ARG1_DEFAULT1_NULLABLE =
		ReturnTypes.cascade(NUMERIC_FROM_ARG1_DEFAULT1, SqlTypeTransforms.TO_NULLABLE);
}
