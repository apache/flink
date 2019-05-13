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

package org.apache.flink.table.functions;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.UnresolvedReferenceExpression;
import org.apache.flink.table.type.InternalType;
import org.apache.flink.table.type.InternalTypes;

import static org.apache.flink.table.expressions.ExpressionBuilder.literal;
import static org.apache.flink.table.expressions.ExpressionBuilder.minus;
import static org.apache.flink.table.expressions.ExpressionBuilder.plus;

/**
 * This count1 aggregate function returns the count1 of values
 * which go into it like [[CountAggFunction]].
 * It differs in that null values are also counted.
 */
public class Count1AggFunction extends DeclarativeAggregateFunction {
	private UnresolvedReferenceExpression count1 = new UnresolvedReferenceExpression("count1");

	@Override
	public int operandCount() {
		return 1;
	}

	@Override
	public UnresolvedReferenceExpression[] aggBufferAttributes() {
		return new UnresolvedReferenceExpression[] { count1 };
	}

	@Override
	public InternalType[] getAggBufferTypes() {
		return new InternalType[] { InternalTypes.LONG };
	}

	@Override
	public TypeInformation getResultType() {
		return Types.LONG;
	}

	@Override
	public Expression[] initialValuesExpressions() {
		return new Expression[] {
				/* count1 = */ literal(0L, getResultType())
		};
	}

	@Override
	public Expression[] accumulateExpressions() {
		return new Expression[] {
				/* count1 = */ plus(count1, literal(1L))
		};
	}

	@Override
	public Expression[] retractExpressions() {
		return new Expression[] {
				/* count1 = */ minus(count1, literal(1L))
		};
	}

	@Override
	public Expression[] mergeExpressions() {
		return new Expression[] {
				/* count1 = */ plus(count1, mergeOperand(count1))
		};
	}

	@Override
	public Expression getValueExpression() {
		return count1;
	}
}
