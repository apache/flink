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

import java.math.BigDecimal;

import static org.apache.flink.table.expressions.utils.ApiExpressionUtils.unresolvedRef;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.ifThenElse;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.isNull;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.literal;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.minus;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.plus;

/**
 * built-in sum0 aggregate function.
 */
public abstract class Sum0AggFunction extends DeclarativeAggregateFunction {
	private UnresolvedReferenceExpression sum0 = unresolvedRef("sum");

	@Override
	public int operandCount() {
		return 1;
	}

	@Override
	public UnresolvedReferenceExpression[] aggBufferAttributes() {
		return new UnresolvedReferenceExpression[] { sum0 };
	}

	@Override
	public DataType[] getAggBufferTypes() {
		return new DataType[] { getResultType() };
	}

	@Override
	public Expression[] accumulateExpressions() {
		return new Expression[] {
				/* sum0 = */ ifThenElse(isNull(operand(0)), sum0, plus(sum0, operand(0)))
		};
	}

	@Override
	public Expression[] retractExpressions() {
		return new Expression[] {
				/* sum0 = */ ifThenElse(isNull(operand(0)), sum0, minus(sum0, operand(0)))
		};
	}

	@Override
	public Expression[] mergeExpressions() {
		return new Expression[] {
				/* sum0 = */ plus(sum0, mergeOperand(sum0))
		};
	}

	@Override
	public Expression getValueExpression() {
		return sum0;
	}

	/**
	 * Built-in Int Sum0 aggregate function.
	 */
	public static class IntSum0AggFunction extends Sum0AggFunction {

		@Override
		public DataType getResultType() {
			return DataTypes.INT();
		}

		@Override
		public Expression[] initialValuesExpressions() {
			return new Expression[] {
					/* sum0 = */ literal(0, getResultType())
			};
		}
	}

	/**
	 * Built-in Byte Sum0 aggregate function.
	 */
	public static class ByteSum0AggFunction extends Sum0AggFunction {
		@Override
		public DataType getResultType() {
			return DataTypes.TINYINT();
		}

		@Override
		public Expression[] initialValuesExpressions() {
			return new Expression[] {
					/* sum0 = */ literal((byte) 0, getResultType())
			};
		}
	}

	/**
	 * Built-in Short Sum0 aggregate function.
	 */
	public static class ShortSum0AggFunction extends Sum0AggFunction {
		@Override
		public DataType getResultType() {
			return DataTypes.SMALLINT();
		}

		@Override
		public Expression[] initialValuesExpressions() {
			return new Expression[] {
					/* sum0 = */ literal((short) 0, getResultType())
			};
		}
	}

	/**
	 * Built-in Long Sum0 aggregate function.
	 */
	public static class LongSum0AggFunction extends Sum0AggFunction {
		@Override
		public DataType getResultType() {
			return DataTypes.BIGINT();
		}

		@Override
		public Expression[] initialValuesExpressions() {
			return new Expression[] {
					/* sum0 = */ literal(0L, getResultType())
			};
		}
	}

	/**
	 * Built-in Float Sum0 aggregate function.
	 */
	public static class FloatSum0AggFunction extends Sum0AggFunction {
		@Override
		public DataType getResultType() {
			return DataTypes.FLOAT();
		}

		@Override
		public Expression[] initialValuesExpressions() {
			return new Expression[] {
					/* sum0 = */ literal(0.0f, getResultType())
			};
		}
	}

	/**
	 * Built-in Double Sum0 aggregate function.
	 */
	public static class DoubleSum0AggFunction extends Sum0AggFunction {
		@Override
		public DataType getResultType() {
			return DataTypes.DOUBLE();
		}

		@Override
		public Expression[] initialValuesExpressions() {
			return new Expression[] {
					/* sum0 = */ literal(0.0d, getResultType())
			};
		}
	}

	/**
	 * Built-in Decimal Sum0 aggregate function.
	 */
	public static class DecimalSum0AggFunction extends Sum0AggFunction {
		private DecimalType decimalType;

		public DecimalSum0AggFunction(DecimalType decimalType) {
			this.decimalType = decimalType;
		}

		@Override
		public DataType getResultType() {
			DecimalType sumType = FlinkTypeSystem.inferAggSumType(decimalType.getScale());
			return DataTypes.DECIMAL(sumType.getPrecision(), sumType.getScale());
		}

		@Override
		public Expression[] initialValuesExpressions() {
			return new Expression[] {
					/* sum0 = */ literal(new BigDecimal(0), getResultType())
			};
		}
	}
}
