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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.calcite.FlinkTypeSystem;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.UnresolvedReferenceExpression;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DecimalType;

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

	public abstract DataType getSumType();

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
	public DataType[] getAggBufferTypes() {
		return new DataType[] {
				getSumType(),
				DataTypes.BIGINT()
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
		public DataType getResultType() {
			return DataTypes.DOUBLE();
		}

		@Override
		public DataType getSumType() {
			return DataTypes.BIGINT();
		}
	}

	/**
	 * Built-in Double Avg aggregate function.
	 */
	public static class DoubleAvgAggFunction extends AvgAggFunction {

		@Override
		public DataType getResultType() {
			return DataTypes.DOUBLE();
		}

		@Override
		public DataType getSumType() {
			return DataTypes.DOUBLE();
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

		private final DecimalType type;

		public DecimalAvgAggFunction(DecimalType type) {
			this.type = type;
		}

		@Override
		public DataType getResultType() {
			DecimalType t = FlinkTypeSystem.inferAggAvgType(type.getScale());
			return DataTypes.DECIMAL(t.getPrecision(), t.getScale());
		}

		@Override
		public DataType getSumType() {
			DecimalType t = FlinkTypeSystem.inferAggSumType(type.getScale());
			return DataTypes.DECIMAL(t.getPrecision(), t.getScale());
		}

		@Override
		public Expression[] initialValuesExpressions() {
			return new Expression[] {
				literal(
					BigDecimal.ZERO,
					getSumType()),
				literal(0L)
			};
		}
	}
}
