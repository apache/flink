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

package org.apache.flink.table.functions.aggfunctions;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.UnresolvedReferenceExpression;
import org.apache.flink.table.type.DecimalType;
import org.apache.flink.table.type.InternalType;
import org.apache.flink.table.type.InternalTypes;
import org.apache.flink.table.type.TypeConverters;
import org.apache.flink.table.typeutils.DecimalTypeInfo;

import java.math.BigDecimal;

import static org.apache.flink.table.expressions.ExpressionBuilder.div;
import static org.apache.flink.table.expressions.ExpressionBuilder.equalTo;
import static org.apache.flink.table.expressions.ExpressionBuilder.ifThenElse;
import static org.apache.flink.table.expressions.ExpressionBuilder.isNull;
import static org.apache.flink.table.expressions.ExpressionBuilder.literal;
import static org.apache.flink.table.expressions.ExpressionBuilder.minus;
import static org.apache.flink.table.expressions.ExpressionBuilder.nullOf;
import static org.apache.flink.table.expressions.ExpressionBuilder.plus;

/**
 * built-in avg aggregate function.
 */
public abstract class AvgAggFunction extends DeclarativeAggregateFunction {

	private UnresolvedReferenceExpression sum = new UnresolvedReferenceExpression("sum");
	private UnresolvedReferenceExpression count = new UnresolvedReferenceExpression("count");

	public abstract TypeInformation getSumType();

	@Override
	public int operandCount() {
		return 1;
	}

	@Override
	public UnresolvedReferenceExpression[] aggBufferAttributes() {
		return new UnresolvedReferenceExpression[] {
				sum,
				count};
	}

	@Override
	public InternalType[] getAggBufferTypes() {
		return new InternalType[] {
				TypeConverters.createInternalTypeFromTypeInfo(getSumType()),
				InternalTypes.LONG
		};
	}

	@Override
	public Expression[] initialValuesExpressions() {
		return new Expression[] {
				/* sum = */ literal(0L, getSumType()),
				/* count = */ literal(0L)};
	}

	@Override
	public Expression[] accumulateExpressions() {
		return new Expression[] {
				/* sum = */ ifThenElse(isNull(operand(0)), sum, plus(sum, operand(0))),
				/* count = */ ifThenElse(isNull(operand(0)), count, plus(count, literal(1L))),
		};
	}

	@Override
	public Expression[] retractExpressions() {
		return new Expression[] {
				/* sum = */ ifThenElse(isNull(operand(0)), sum, minus(sum, operand(0))),
				/* count = */ ifThenElse(isNull(operand(0)), count, minus(count, literal(1L))),
		};
	}

	@Override
	public Expression[] mergeExpressions() {
		return new Expression[] {
				/* sum = */ plus(sum, mergeOperand(sum)),
				/* count = */ plus(count, mergeOperand(count))
		};
	}

	/**
	 * If all input are nulls, count will be 0 and we will get null after the division.
	 */
	@Override
	public Expression getValueExpression() {
		return ifThenElse(equalTo(count, literal(0L)), nullOf(getResultType()), div(sum, count));
	}

	/**
	 * Built-in Int Avg aggregate function for integral arguments,
	 * including BYTE, SHORT, INT, LONG.
	 * The result type is DOUBLE.
	 */
	public static class IntegralAvgAggFunction extends AvgAggFunction {

		@Override
		public TypeInformation getResultType() {
			return Types.DOUBLE;
		}

		@Override
		public TypeInformation getSumType() {
			return Types.LONG;
		}
	}

	/**
	 * Built-in Double Avg aggregate function.
	 */
	public static class DoubleAvgAggFunction extends AvgAggFunction {

		@Override
		public TypeInformation getResultType() {
			return Types.DOUBLE;
		}

		@Override
		public TypeInformation getSumType() {
			return Types.DOUBLE;
		}

		@Override
		public Expression[] initialValuesExpressions() {
			return new Expression[] {literal(0D), literal(0L)};
		}
	}

	/**
	 * Built-in Decimal Avg aggregate function.
	 */
	public static class DecimalAvgAggFunction extends AvgAggFunction {

		private final DecimalTypeInfo type;

		public DecimalAvgAggFunction(DecimalTypeInfo type) {
			this.type = type;
		}

		@Override
		public TypeInformation getResultType() {
			return DecimalType.inferAggAvgType(type.scale()).toTypeInfo();
		}

		@Override
		public TypeInformation getSumType() {
			return DecimalType.inferAggSumType(type.scale()).toTypeInfo();
		}

		@Override
		public Expression[] initialValuesExpressions() {
			return new Expression[] {literal(new BigDecimal(0), getSumType()), literal(0L)};
		}
	}
}
