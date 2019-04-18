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
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.UnresolvedReferenceExpression;
import org.apache.flink.table.type.DecimalType;
import org.apache.flink.table.type.InternalType;
import org.apache.flink.table.type.TypeConverters;
import org.apache.flink.table.typeutils.DecimalTypeInfo;

import static org.apache.flink.table.expressions.ExpressionBuilder.ifThenElse;
import static org.apache.flink.table.expressions.ExpressionBuilder.isNull;
import static org.apache.flink.table.expressions.ExpressionBuilder.lessThan;
import static org.apache.flink.table.expressions.ExpressionBuilder.literal;
import static org.apache.flink.table.expressions.ExpressionBuilder.nullOf;
import static org.apache.flink.table.expressions.ExpressionBuilder.or;
import static org.apache.flink.table.expressions.ExpressionBuilder.plus;

/**
 * built-in IncrSum aggregate function,
 * negative number is discarded to ensure the monotonicity.
 */
public abstract class IncrSumAggFunction extends DeclarativeAggregateFunction {
	private UnresolvedReferenceExpression sum = new UnresolvedReferenceExpression("sum");

	@Override
	public int operandCount() {
		return 1;
	}

	@Override
	public UnresolvedReferenceExpression[] aggBufferAttributes() {
		return new UnresolvedReferenceExpression[] { sum };
	}

	@Override
	public InternalType[] getAggBufferTypes() {
		return new InternalType[] { TypeConverters.createInternalTypeFromTypeInfo(getResultType()) };
	}

	@Override
	public Expression[] initialValuesExpressions() {
		return new Expression[] {
				/* sum = */ nullOf(getResultType())
		};
	}

	@Override
	public Expression[] accumulateExpressions() {
		return new Expression[] {
				/* sum = */
				ifThenElse(or(isNull(operand(0)), lessThan(operand(0), literal(0L))), sum,
						ifThenElse(isNull(sum), operand(0), plus(sum, operand(0))))
		};
	}

	@Override
	public Expression[] retractExpressions() {
		throw new TableException("This function does not support retraction, Please choose SumWithRetractAggFunction.");
	}

	@Override
	public Expression[] mergeExpressions() {
		return new Expression[] {
				/* sum = */
				ifThenElse(isNull(mergeOperand(sum)), sum,
						ifThenElse(isNull(sum), mergeOperand(sum), plus(sum, mergeOperand(sum))))
		};
	}

	@Override
	public Expression getValueExpression() {
		return sum;
	}

	/**
	 * Built-in Int IncrSum aggregate function.
	 */
	public static class IntIncrSumAggFunction extends IncrSumAggFunction {

		@Override
		public TypeInformation getResultType() {
			return Types.INT;
		}
	}

	/**
	 * Built-in Byte IncrSum aggregate function.
	 */
	public static class ByteIncrSumAggFunction extends IncrSumAggFunction {
		@Override
		public TypeInformation getResultType() {
			return Types.BYTE;
		}
	}

	/**
	 * Built-in Short IncrSum aggregate function.
	 */
	public static class ShortIncrSumAggFunction extends IncrSumAggFunction {
		@Override
		public TypeInformation getResultType() {
			return Types.SHORT;
		}
	}

	/**
	 * Built-in Long IncrSum aggregate function.
	 */
	public static class LongIncrSumAggFunction extends IncrSumAggFunction {
		@Override
		public TypeInformation getResultType() {
			return Types.LONG;
		}
	}

	/**
	 * Built-in Float IncrSum aggregate function.
	 */
	public static class FloatIncrSumAggFunction extends IncrSumAggFunction {
		@Override
		public TypeInformation getResultType() {
			return Types.FLOAT;
		}
	}

	/**
	 * Built-in Double IncrSum aggregate function.
	 */
	public static class DoubleIncrSumAggFunction extends IncrSumAggFunction {
		@Override
		public TypeInformation getResultType() {
			return Types.DOUBLE;
		}
	}

	/**
	 * Built-in Decimal IncrSum aggregate function.
	 */
	public static class DecimalIncrSumAggFunction extends IncrSumAggFunction {
		private DecimalTypeInfo decimalType;

		public DecimalIncrSumAggFunction(DecimalTypeInfo decimalType) {
			this.decimalType = decimalType;
		}

		@Override
		public TypeInformation getResultType() {
			DecimalType sumType = DecimalType.inferAggSumType(decimalType.scale());
			return new DecimalTypeInfo(sumType.precision(), sumType.scale());
		}
	}
}
