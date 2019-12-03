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
import org.apache.flink.table.runtime.operators.over.frame.OffsetOverFrame;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;

import static org.apache.flink.table.expressions.utils.ApiExpressionUtils.unresolvedRef;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.cast;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.literal;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.typeLiteral;

/**
 * LEAD and LAG aggregate functions return the value of given expression evaluated at given offset.
 * The functions only are used by over window.
 *
 * <p>LAG(input, offset, default) - Returns the value of `input` at the `offset`th row
 * before the current row in the window. The default value of `offset` is 1 and the default
 * value of `default` is null. If the value of `input` at the `offset`th row is null,
 * null is returned. If there is no such offset row (e.g., when the offset is 1, the first
 * row of the window does not have any previous row), `default` is returned.
 *
 * <p>LEAD(input, offset, default) - Returns the value of `input` at the `offset`th row
 * after the current row in the window. The default value of `offset` is 1 and the default
 * value of `default` is null. If the value of `input` at the `offset`th row is null,
 * null is returned. If there is no such an offset row (e.g., when the offset is 1, the last
 * row of the window does not have any subsequent row), `default` is returned.
 *
 * <p>These two aggregate functions are special, and only are used by over window. So here the
 * concrete implementation is closely related to {@link OffsetOverFrame}.
 */
public abstract class LeadLagAggFunction extends DeclarativeAggregateFunction {

	private int operandCount;

	//If the length of function's args is 3, then the function has the default value.
	private boolean existDefaultValue;

	private UnresolvedReferenceExpression value = unresolvedRef("leadlag");

	public LeadLagAggFunction(int operandCount) {
		this.operandCount = operandCount;
		existDefaultValue = operandCount == 3;
	}

	@Override
	public int operandCount() {
		return operandCount;
	}

	@Override
	public UnresolvedReferenceExpression[] aggBufferAttributes() {
		return new UnresolvedReferenceExpression[] {value};
	}

	@Override
	public DataType[] getAggBufferTypes() {
		return new DataType[] {getResultType()};
	}

	@Override
	public Expression[] initialValuesExpressions() {
		return new Expression[] {literal(null, getResultType())};
	}

	@Override
	public Expression[] accumulateExpressions() {
		return new Expression[] {operand(0)};
	}

	// TODO hack, use the current input reset the buffer value.
	@Override
	public Expression[] retractExpressions() {
		return new Expression[] {existDefaultValue ? cast(operand(2),
				typeLiteral(getResultType())) : literal(null, getResultType())};
	}

	@Override
	public Expression[] mergeExpressions() {
		return new Expression[0];
	}

	@Override
	public Expression getValueExpression() {
		return value;
	}

	/**
	 * IntLeadLagAggFunction.
	 */
	public static class IntLeadLagAggFunction extends LeadLagAggFunction {

		public IntLeadLagAggFunction(int operandCount) {
			super(operandCount);
		}

		@Override
		public DataType getResultType() {
			return DataTypes.INT();
		}
	}

	/**
	 * ByteLeadLagAggFunction.
	 */
	public static class ByteLeadLagAggFunction extends LeadLagAggFunction {

		public ByteLeadLagAggFunction(int operandCount) {
			super(operandCount);
		}

		@Override
		public DataType getResultType() {
			return DataTypes.TINYINT();
		}
	}

	/**
	 * ShortLeadLagAggFunction.
	 */
	public static class ShortLeadLagAggFunction extends LeadLagAggFunction {

		public ShortLeadLagAggFunction(int operandCount) {
			super(operandCount);
		}

		@Override
		public DataType getResultType() {
			return DataTypes.SMALLINT();
		}
	}

	/**
	 * LongLeadLagAggFunction.
	 */
	public static class LongLeadLagAggFunction extends LeadLagAggFunction {

		public LongLeadLagAggFunction(int operandCount) {
			super(operandCount);
		}

		@Override
		public DataType getResultType() {
			return DataTypes.BIGINT();
		}
	}

	/**
	 * FloatLeadLagAggFunction.
	 */
	public static class FloatLeadLagAggFunction extends LeadLagAggFunction {

		public FloatLeadLagAggFunction(int operandCount) {
			super(operandCount);
		}

		@Override
		public DataType getResultType() {
			return DataTypes.FLOAT();
		}
	}

	/**
	 * DoubleLeadLagAggFunction.
	 */
	public static class DoubleLeadLagAggFunction extends LeadLagAggFunction {

		public DoubleLeadLagAggFunction(int operandCount) {
			super(operandCount);
		}

		@Override
		public DataType getResultType() {
			return DataTypes.DOUBLE();
		}
	}

	/**
	 * BooleanLeadLagAggFunction.
	 */
	public static class BooleanLeadLagAggFunction extends LeadLagAggFunction {

		public BooleanLeadLagAggFunction(int operandCount) {
			super(operandCount);
		}

		@Override
		public DataType getResultType() {
			return DataTypes.BOOLEAN();
		}
	}

	/**
	 * DecimalLeadLagAggFunction.
	 */
	public static class DecimalLeadLagAggFunction extends LeadLagAggFunction {

		private final DecimalType decimalType;

		public DecimalLeadLagAggFunction(int operandCount, DecimalType decimalType) {
			super(operandCount);
			this.decimalType = decimalType;
		}

		@Override
		public DataType getResultType() {
			return DataTypes.DECIMAL(decimalType.getPrecision(), decimalType.getScale());
		}
	}

	/**
	 * StringLeadLagAggFunction.
	 */
	public static class StringLeadLagAggFunction extends LeadLagAggFunction {

		public StringLeadLagAggFunction(int operandCount) {
			super(operandCount);
		}

		@Override
		public DataType getResultType() {
			return DataTypes.STRING();
		}
	}

	/**
	 * DateLeadLagAggFunction.
	 */
	public static class DateLeadLagAggFunction extends LeadLagAggFunction {

		public DateLeadLagAggFunction(int operandCount) {
			super(operandCount);
		}

		@Override
		public DataType getResultType() {
			return DataTypes.DATE();
		}
	}

	/**
	 * TimeLeadLagAggFunction.
	 */
	public static class TimeLeadLagAggFunction extends LeadLagAggFunction {

		public TimeLeadLagAggFunction(int operandCount) {
			super(operandCount);
		}

		@Override
		public DataType getResultType() {
			return DataTypes.TIME(TimeType.DEFAULT_PRECISION);
		}
	}

	/**
	 * TimestampLeadLagAggFunction.
	 */
	public static class TimestampLeadLagAggFunction extends LeadLagAggFunction {

		private final TimestampType type;

		public TimestampLeadLagAggFunction(int operandCount, TimestampType type) {
			super(operandCount);
			this.type = type;
		}

		@Override
		public DataType getResultType() {
			return DataTypes.TIMESTAMP(type.getPrecision());
		}
	}
}
