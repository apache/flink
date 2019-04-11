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
import org.apache.flink.table.type.TypeConverters;
import org.apache.flink.table.typeutils.DecimalTypeInfo;

import java.math.BigDecimal;

import static org.apache.flink.table.expressions.ExpressionBuilder.ifThenElse;
import static org.apache.flink.table.expressions.ExpressionBuilder.isNull;
import static org.apache.flink.table.expressions.ExpressionBuilder.literal;
import static org.apache.flink.table.expressions.ExpressionBuilder.minus;
import static org.apache.flink.table.expressions.ExpressionBuilder.plus;

/**
 * built-in sum0 aggregate function.
 */
public abstract class Sum0AggFunction extends DeclarativeAggregateFunction {
	private UnresolvedReferenceExpression sum0 = new UnresolvedReferenceExpression("sum");

	@Override
	public int operandCount() {
		return 1;
	}

	@Override
	public UnresolvedReferenceExpression[] aggBufferAttributes() {
		return new UnresolvedReferenceExpression[] { sum0 };
	}

	@Override
	public InternalType[] getAggBufferTypes() {
		return new InternalType[] { TypeConverters.createInternalTypeFromTypeInfo(getResultType()) };
	}

	@Override
	public Expression[] initialValuesExpressions() {
		return new Expression[] {
				/* sum0 = */ literal(0L, getResultType())
		};
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
		public TypeInformation getResultType() {
			return Types.INT;
		}
	}

	/**
	 * Built-in Byte Sum0 aggregate function.
	 */
	public static class ByteSum0AggFunction extends Sum0AggFunction {
		@Override
		public TypeInformation getResultType() {
			return Types.BYTE;
		}
	}

	/**
	 * Built-in Short Sum0 aggregate function.
	 */
	public static class ShortSum0AggFunction extends Sum0AggFunction {
		@Override
		public TypeInformation getResultType() {
			return Types.SHORT;
		}
	}

	/**
	 * Built-in Long Sum0 aggregate function.
	 */
	public static class LongSum0AggFunction extends Sum0AggFunction {
		@Override
		public TypeInformation getResultType() {
			return Types.LONG;
		}
	}

	/**
	 * Built-in Float Sum0 aggregate function.
	 */
	public static class FloatSum0AggFunction extends Sum0AggFunction {
		@Override
		public TypeInformation getResultType() {
			return Types.FLOAT;
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
		public TypeInformation getResultType() {
			return Types.DOUBLE;
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
		private DecimalTypeInfo decimalType;

		public DecimalSum0AggFunction(DecimalTypeInfo decimalType) {
			this.decimalType = decimalType;
		}

		@Override
		public TypeInformation getResultType() {
			DecimalType sumType = DecimalType.inferAggSumType(decimalType.scale());
			return new DecimalTypeInfo(sumType.precision(), sumType.scale());
		}

		@Override
		public Expression[] initialValuesExpressions() {
			return new Expression[] {
					/* sum0 = */ literal(new BigDecimal(0), getResultType())
			};
		}
	}
}
