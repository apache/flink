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
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionBuilder;
import org.apache.flink.table.expressions.UnresolvedReferenceExpression;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Optional;

import static org.apache.flink.table.expressions.ExpressionBuilder.equalTo;
import static org.apache.flink.table.expressions.ExpressionBuilder.ifThenElse;
import static org.apache.flink.table.expressions.ExpressionBuilder.isNull;
import static org.apache.flink.table.expressions.ExpressionBuilder.literal;

/**
 * built-in rank like aggregate function, e.g. rank, dense_rank
 */
public abstract class RankLikeAggFunctionBase extends DeclarativeAggregateFunction {
	protected UnresolvedReferenceExpression sequence = new UnresolvedReferenceExpression("sequence");
	protected UnresolvedReferenceExpression[] lastValues;
	protected LogicalType[] orderKeyTypes;

	public RankLikeAggFunctionBase(LogicalType[] orderKeyTypes) {
		this.orderKeyTypes = orderKeyTypes;
		lastValues = new UnresolvedReferenceExpression[orderKeyTypes.length];
		for (int i = 0; i < orderKeyTypes.length; ++i) {
			lastValues[i] = new UnresolvedReferenceExpression("lastValue_" + i);
		}
	}

	@Override
	public int operandCount() {
		return orderKeyTypes.length;
	}

	@Override
	public DataType getResultType() {
		return DataTypes.BIGINT();
	}

	@Override
	public Expression[] retractExpressions() {
		throw new TableException("This function does not support retraction.");
	}

	@Override
	public Expression[] mergeExpressions() {
		throw new TableException("This function does not support merge.");
	}

	@Override
	public Expression getValueExpression() {
		return sequence;
	}

	protected Expression orderKeyEqualsExpression() {
		Expression[] orderKeyEquals = new Expression[orderKeyTypes.length];
		for (int i = 0; i < orderKeyTypes.length; ++i) {
			// pseudo code:
			// if (lastValue_i is null) {
			//   if (operand(i) is null) true else false
			// } else {
			//   lastValue_i equalTo orderKey(i)
			// }
			Expression lasValue = lastValues[i];
			orderKeyEquals[i] = ifThenElse(isNull(lasValue),
					ifThenElse(isNull(operand(i)), literal(true), literal(false)),
					equalTo(lasValue, operand(i)));
		}
		Optional<Expression> ret = Arrays.stream(orderKeyEquals).reduce(ExpressionBuilder::and);
		return ret.orElseGet(() -> literal(true));
	}

	protected Expression generateInitLiteral(LogicalType orderType) {
		switch (orderType.getTypeRoot()) {
			case BOOLEAN:
				return literal(false);
			case TINYINT:
				return literal((byte) 0);
			case SMALLINT:
				return literal((short) 0);
			case INTEGER:
				return literal(0);
			case BIGINT:
				return literal(0L);
			case FLOAT:
				return literal(0.0f);
			case DOUBLE:
				return literal(0.0d);
			case DECIMAL:
				return literal(java.math.BigDecimal.ZERO);
			case VARCHAR:
				return literal("");
			case DATE:
				return literal(new Date(0));
			case TIME_WITHOUT_TIME_ZONE:
				return literal(new Time(0));
			case TIMESTAMP_WITHOUT_TIME_ZONE:
				return literal(new Timestamp(0));
			default:
				throw new TableException("Unsupported type: " + orderType);
		}
	}
}
