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
import org.apache.flink.table.type.InternalType;
import org.apache.flink.table.type.TypeConverters;
import org.apache.flink.table.typeutils.DecimalTypeInfo;

import static org.apache.flink.table.expressions.ExpressionBuilder.ifThenElse;
import static org.apache.flink.table.expressions.ExpressionBuilder.isNull;
import static org.apache.flink.table.expressions.ExpressionBuilder.lessThan;
import static org.apache.flink.table.expressions.ExpressionBuilder.nullOf;

/**
 * built-in min aggregate function.
 */
public abstract class MinAggFunction extends DeclarativeAggregateFunction {
	private UnresolvedReferenceExpression min = new UnresolvedReferenceExpression("min");

	@Override
	public int operandCount() {
		return 1;
	}

	@Override
	public UnresolvedReferenceExpression[] aggBufferAttributes() {
		return new UnresolvedReferenceExpression[] { min };
	}

	@Override
	public InternalType[] getAggBufferTypes() {
		return new InternalType[] { TypeConverters.createInternalTypeFromTypeInfo(getResultType()) };
	}

	@Override
	public Expression[] initialValuesExpressions() {
		return new Expression[] {
				/* min = */ nullOf(getResultType())
		};
	}

	@Override
	public Expression[] accumulateExpressions() {
		return new Expression[] {
				/* min = */
				ifThenElse(isNull(operand(0)), min,
						ifThenElse(isNull(min), operand(0),
								ifThenElse(lessThan(operand(0), min), operand(0), min)))
		};
	}

	@Override
	public Expression[] retractExpressions() {
		// TODO FLINK-12295, ignore exception now
//		throw new TableException("This function does not support retraction, Please choose MinWithRetractAggFunction.");
		return new Expression[0];
	}

	@Override
	public Expression[] mergeExpressions() {
		return new Expression[] {
				/* min = */
				ifThenElse(isNull(mergeOperand(min)), min,
						ifThenElse(isNull(min), mergeOperand(min),
								ifThenElse(lessThan(mergeOperand(min), min), mergeOperand(min), min)))
		};
	}

	@Override
	public Expression getValueExpression() {
		return min;
	}

	/**
	 * Built-in Int Min aggregate function.
	 */
	public static class IntMinAggFunction extends MinAggFunction {

		@Override
		public TypeInformation getResultType() {
			return Types.INT;
		}
	}

	/**
	 * Built-in Byte Min aggregate function.
	 */
	public static class ByteMinAggFunction extends MinAggFunction {
		@Override
		public TypeInformation getResultType() {
			return Types.BYTE;
		}
	}

	/**
	 * Built-in Short Min aggregate function.
	 */
	public static class ShortMinAggFunction extends MinAggFunction {
		@Override
		public TypeInformation getResultType() {
			return Types.SHORT;
		}
	}

	/**
	 * Built-in Long Min aggregate function.
	 */
	public static class LongMinAggFunction extends MinAggFunction {
		@Override
		public TypeInformation getResultType() {
			return Types.LONG;
		}
	}

	/**
	 * Built-in Float Min aggregate function.
	 */
	public static class FloatMinAggFunction extends MinAggFunction {
		@Override
		public TypeInformation getResultType() {
			return Types.FLOAT;
		}
	}

	/**
	 * Built-in Double Min aggregate function.
	 */
	public static class DoubleMinAggFunction extends MinAggFunction {
		@Override
		public TypeInformation getResultType() {
			return Types.DOUBLE;
		}
	}

	/**
	 * Built-in Decimal Min aggregate function.
	 */
	public static class DecimalMinAggFunction extends MinAggFunction {
		private DecimalTypeInfo decimalType;

		public DecimalMinAggFunction(DecimalTypeInfo decimalType) {
			this.decimalType = decimalType;
		}

		@Override
		public TypeInformation getResultType() {
			return decimalType;
		}
	}

	/**
	 * Built-in Boolean Min aggregate function.
	 */
	public static class BooleanMinAggFunction extends MinAggFunction {
		@Override
		public TypeInformation getResultType() {
			return Types.BOOLEAN;
		}
	}

	/**
	 * Built-in String Min aggregate function.
	 */
	public static class StringMinAggFunction extends MinAggFunction {
		@Override
		public TypeInformation getResultType() {
			return Types.STRING;
		}
	}

	/**
	 * Built-in Date Min aggregate function.
	 */
	public static class DateMinAggFunction extends MinAggFunction {
		@Override
		public TypeInformation getResultType() {
			return Types.SQL_DATE;
		}
	}

	/**
	 * Built-in Time Min aggregate function.
	 */
	public static class TimeMinAggFunction extends MinAggFunction {
		@Override
		public TypeInformation getResultType() {
			return Types.SQL_TIME;
		}
	}

	/**
	 * Built-in Timestamp Min aggregate function.
	 */
	public static class TimestampMinAggFunction extends MinAggFunction {
		@Override
		public TypeInformation getResultType() {
			return Types.SQL_TIMESTAMP;
		}
	}
}
