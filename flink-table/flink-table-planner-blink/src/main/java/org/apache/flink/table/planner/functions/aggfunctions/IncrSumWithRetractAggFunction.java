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

package org.apache.flink.table.planner.functions.aggfunctions;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.UnresolvedReferenceExpression;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DecimalType;

import static org.apache.flink.table.expressions.ApiExpressionUtils.unresolvedRef;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.equalTo;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.ifThenElse;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.isNull;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.lessThan;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.literal;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.minus;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.nullOf;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.or;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.plus;

/**
 * built-in IncrSum with retract aggregate function,
 * negative number is discarded to ensure the monotonicity.
 */
public abstract class IncrSumWithRetractAggFunction extends DeclarativeAggregateFunction {
	private UnresolvedReferenceExpression sum = unresolvedRef("sum");
	private UnresolvedReferenceExpression count = unresolvedRef("count");

	@Override
	public int operandCount() {
		return 1;
	}

	@Override
	public UnresolvedReferenceExpression[] aggBufferAttributes() {
		return new UnresolvedReferenceExpression[0];
	}

	@Override
	public DataType[] getAggBufferTypes() {
		return new DataType[] {
				getResultType(),
				DataTypes.BIGINT() };
	}

	@Override
	public Expression[] initialValuesExpressions() {
		return new Expression[] {
				/* sum = */ nullOf(getResultType()),
				/* count = */ literal(0L)
		};
	}

	@Override
	public Expression[] accumulateExpressions() {
		return new Expression[] {
				/* sum = */
				ifThenElse(or(isNull(operand(0)), lessThan(operand(0), zeroLiteral())), sum,
						ifThenElse(isNull(sum), operand(0), plus(sum, operand(0)))),
				/* count = */
				ifThenElse(or(isNull(operand(0)), lessThan(operand(0), literal(0L))), count,
						plus(count, literal(1L)))
		};
	}

	@Override
	public Expression[] retractExpressions() {
		return new Expression[] {
				/* sum = */
				ifThenElse(or(isNull(operand(0)), lessThan(operand(0), zeroLiteral())), sum,
						ifThenElse(isNull(sum), minus(zeroLiteral(), operand(0)), minus(sum, operand(0)))),
				/* count = */
				ifThenElse(isNull(operand(0)), count, minus(count, literal(1L)))
		};
	}

	@Override
	public Expression[] mergeExpressions() {
		return new Expression[] {
				/* sum = */
				ifThenElse(isNull(mergeOperand(sum)), sum,
						ifThenElse(isNull(sum), mergeOperand(sum), plus(sum, mergeOperand(sum)))),
				/* count = */
				plus(count, mergeOperand(count))
		};
	}

	@Override
	public Expression getValueExpression() {
		return ifThenElse(equalTo(count, literal(0L)), nullOf(getResultType()), sum);
	}

	protected abstract Expression zeroLiteral();

	/**
	 * Built-in IncrInt Sum with retract aggregate function.
	 */
	public static class IntIncrSumWithRetractAggFunction extends IncrSumWithRetractAggFunction {

		@Override
		public DataType getResultType() {
			return DataTypes.INT();
		}

		@Override
		protected Expression zeroLiteral() {
			return literal(0);
		}
	}

	/**
	 * Built-in Byte IncrSum with retract aggregate function.
	 */
	public static class ByteIncrSumWithRetractAggFunction extends IncrSumWithRetractAggFunction {
		@Override
		public DataType getResultType() {
			return DataTypes.TINYINT();
		}

		@Override
		protected Expression zeroLiteral() {
			return literal((byte) 0);
		}
	}

	/**
	 * Built-in Short IncrSum with retract aggregate function.
	 */
	public static class ShortIncrSumWithRetractAggFunction extends IncrSumWithRetractAggFunction {
		@Override
		public DataType getResultType() {
			return DataTypes.SMALLINT();
		}

		@Override
		protected Expression zeroLiteral() {
			return literal((short) 0);
		}
	}

	/**
	 * Built-in Long IncrSum with retract aggregate function.
	 */
	public static class LongIncrSumWithRetractAggFunction extends IncrSumWithRetractAggFunction {
		@Override
		public DataType getResultType() {
			return DataTypes.BIGINT();
		}

		@Override
		protected Expression zeroLiteral() {
			return literal(0L);
		}
	}

	/**
	 * Built-in Float IncrSum with retract aggregate function.
	 */
	public static class FloatIncrSumWithRetractAggFunction extends IncrSumWithRetractAggFunction {
		@Override
		public DataType getResultType() {
			return DataTypes.FLOAT();
		}

		@Override
		protected Expression zeroLiteral() {
			return literal(0F);
		}
	}

	/**
	 * Built-in Double IncrSum with retract aggregate function.
	 */
	public static class DoubleIncrSumWithRetractAggFunction extends IncrSumWithRetractAggFunction {
		@Override
		public DataType getResultType() {
			return DataTypes.DOUBLE();
		}

		@Override
		protected Expression zeroLiteral() {
			return literal(0D);
		}
	}

	/**
	 * Built-in Decimal IncrSum with retract aggregate function.
	 */
	public static class DecimalIncrSumWithRetractAggFunction extends IncrSumWithRetractAggFunction {
		private DecimalType decimalType;

		public DecimalIncrSumWithRetractAggFunction(DecimalType decimalType) {
			this.decimalType = decimalType;
		}

		@Override
		public DataType getResultType() {
			DecimalType sumType = FlinkTypeSystem.inferAggSumType(decimalType.getScale());
			return DataTypes.DECIMAL(sumType.getPrecision(), sumType.getScale());
		}

		@Override
		protected Expression zeroLiteral() {
			return literal(0);
		}
	}
}
