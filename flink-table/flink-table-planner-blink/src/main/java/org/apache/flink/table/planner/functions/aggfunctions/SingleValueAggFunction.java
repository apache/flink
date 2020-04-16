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
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.UnresolvedReferenceExpression;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;

import java.util.Arrays;

import static org.apache.flink.table.expressions.ApiExpressionUtils.unresolvedRef;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.equalTo;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.greaterThan;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.ifThenElse;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.literal;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.minus;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.nullOf;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.or;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.plus;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.typeLiteral;
import static org.apache.flink.table.planner.functions.InternalFunctionDefinitions.THROW_EXCEPTION;

/**
 * Base class for built-in single value aggregate function.
 */
public abstract class SingleValueAggFunction extends DeclarativeAggregateFunction {

	private static final long serialVersionUID = 8850662568341069949L;
	private static final Expression ZERO = literal(0, DataTypes.INT());
	private static final Expression ONE = literal(1, DataTypes.INT());
	private static final String ERROR_MSG = "SingleValueAggFunction received more than one element.";
	private UnresolvedReferenceExpression value = unresolvedRef("value");
	private UnresolvedReferenceExpression count = unresolvedRef("count");

	@Override
	public int operandCount() {
		return 1;
	}

	@Override
	public UnresolvedReferenceExpression[] aggBufferAttributes() {
		return new UnresolvedReferenceExpression[]{value, count};
	}

	@Override
	public DataType[] getAggBufferTypes() {
		return new DataType[]{
			getResultType(),
			DataTypes.INT()};
	}

	@Override
	public Expression[] initialValuesExpressions() {
		return new Expression[] {
			/* value = */ nullOf(getResultType()),
			/* count = */ ZERO,
		};
	}

	@Override
	public Expression[] accumulateExpressions() {
		return new Expression[] {
			/* value = count > 0 ? exception : operand(0) */
			ifThenElse(greaterThan(count, ZERO),
				throwException(ERROR_MSG, getResultType()),
				operand(0)),
			/* count = count + 1 */
			plus(count, ONE)
		};
	}

	@Override
	public Expression[] retractExpressions() {
		return new Expression[] {
			/* value = count == 1 || count == 0 ? null : exception */
			ifThenElse(or(equalTo(count, ONE), equalTo(count, ZERO)),
				nullOf(getResultType()),
				throwException(ERROR_MSG, getResultType())),
			/* count = count - 1 */
			minus(count, ONE)
		};
	}

	@Override
	public Expression[] mergeExpressions() {
		return new Expression[] {
			ifThenElse(greaterThan(plus(count, mergeOperand(count)), ONE),
				throwException(ERROR_MSG, getResultType()),
				ifThenElse(equalTo(plus(count, mergeOperand(count)), ZERO),
					ifThenElse(
						// both zero or right > 0 means we need to reserve the new value here
						or(equalTo(count, ZERO), greaterThan(mergeOperand(count), ZERO)),
						mergeOperand(value),
						nullOf(getResultType())),
					mergeOperand(value))),
			plus(count, mergeOperand(count))
		};
	}

	@Override
	public Expression getValueExpression() {
		return value;
	}

	private static Expression throwException(String msg, DataType type) {
		// it is the internal function without catalog.
		// so it can not be find in any catalog or built-in functions.
		return new CallExpression(THROW_EXCEPTION, Arrays.asList(literal(msg), typeLiteral(type)), type);
	}

	/**
	 * Built-in byte single value aggregate function.
	 */
	public static final class ByteSingleValueAggFunction extends SingleValueAggFunction {

		private static final long serialVersionUID = 320495723666949978L;

		@Override
		public DataType getResultType() {
			return DataTypes.TINYINT();
		}
	}

	/**
	 * Built-in short single value aggregate function.
	 */
	public static final class ShortSingleValueAggFunction extends SingleValueAggFunction {

		private static final long serialVersionUID = 320495723666949978L;

		@Override
		public DataType getResultType() {
			return DataTypes.SMALLINT();
		}
	}

	/**
	 * Built-in int single value aggregate function.
	 */
	public static final class IntSingleValueAggFunction extends SingleValueAggFunction {

		private static final long serialVersionUID = 320495723666949978L;

		@Override
		public DataType getResultType() {
			return DataTypes.INT();
		}
	}

	/**
	 * Built-in long single value aggregate function.
	 */
	public static final class LongSingleValueAggFunction extends SingleValueAggFunction {

		private static final long serialVersionUID = 320495723666949978L;

		@Override
		public DataType getResultType() {
			return DataTypes.BIGINT();
		}
	}

	/**
	 * Built-in float single value aggregate function.
	 */
	public static final class FloatSingleValueAggFunction extends SingleValueAggFunction {

		private static final long serialVersionUID = 320495723666949978L;

		@Override
		public DataType getResultType() {
			return DataTypes.FLOAT();
		}
	}

	/**
	 * Built-in double single value aggregate function.
	 */
	public static final class DoubleSingleValueAggFunction extends SingleValueAggFunction {

		private static final long serialVersionUID = 320495723666949978L;

		@Override
		public DataType getResultType() {
			return DataTypes.DOUBLE();
		}
	}

	/**
	 * Built-in boolean single value aggregate function.
	 */
	public static final class BooleanSingleValueAggFunction extends SingleValueAggFunction {

		private static final long serialVersionUID = 320495723666949978L;

		@Override
		public DataType getResultType() {
			return DataTypes.BOOLEAN();
		}
	}

	/**
	 * Built-in decimal single value aggregate function.
	 */
	public static final class DecimalSingleValueAggFunction extends SingleValueAggFunction {

		private static final long serialVersionUID = 320495723666949978L;

		private final DecimalType type;

		public DecimalSingleValueAggFunction(DecimalType type) {
			this.type = type;
		}

		@Override
		public DataType getResultType() {
			return DataTypes.DECIMAL(type.getPrecision(), type.getScale());
		}
	}

	/**
	 * Built-in string single value aggregate function.
	 */
	public static final class StringSingleValueAggFunction extends SingleValueAggFunction {

		private static final long serialVersionUID = 320495723666949978L;

		@Override
		public DataType getResultType() {
			return DataTypes.STRING();
		}
	}

	/**
	 * Built-in date single value aggregate function.
	 */
	public static final class DateSingleValueAggFunction extends SingleValueAggFunction {

		private static final long serialVersionUID = 320495723666949978L;

		@Override
		public DataType getResultType() {
			return DataTypes.DATE();
		}
	}

	/**
	 * Built-in time single value aggregate function.
	 */
	public static final class TimeSingleValueAggFunction extends SingleValueAggFunction {

		private static final long serialVersionUID = 320495723666949978L;

		@Override
		public DataType getResultType() {
			return DataTypes.TIME(TimeType.DEFAULT_PRECISION);
		}
	}

	/**
	 * Built-in timestamp single value aggregate function.
	 */
	public static final class TimestampSingleValueAggFunction extends SingleValueAggFunction {

		private static final long serialVersionUID = 320495723666949978L;

		private final TimestampType type;

		public TimestampSingleValueAggFunction(TimestampType type) {
			this.type = type;
		}

		@Override
		public DataType getResultType() {
			return DataTypes.TIMESTAMP(type.getPrecision());
		}
	}
}
