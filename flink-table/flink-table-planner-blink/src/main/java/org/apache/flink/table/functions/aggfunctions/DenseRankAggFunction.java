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

import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.UnresolvedReferenceExpression;
import org.apache.flink.table.type.InternalType;
import org.apache.flink.table.type.InternalTypes;

import static org.apache.flink.table.expressions.ExpressionBuilder.ifThenElse;
import static org.apache.flink.table.expressions.ExpressionBuilder.literal;
import static org.apache.flink.table.expressions.ExpressionBuilder.plus;

/**
 * built-in dense_rank aggregate function.
 */
public class DenseRankAggFunction extends RankLikeAggFunctionBase {

	public DenseRankAggFunction(InternalType[] orderKeyTypes) {
		super(orderKeyTypes);
	}

	@Override
	public UnresolvedReferenceExpression[] aggBufferAttributes() {
		UnresolvedReferenceExpression[] aggBufferAttrs = new UnresolvedReferenceExpression[1 + lastValues.length];
		aggBufferAttrs[0] = sequence;
		System.arraycopy(lastValues, 0, aggBufferAttrs, 1, lastValues.length);
		return aggBufferAttrs;
	}

	@Override
	public InternalType[] getAggBufferTypes() {
		InternalType[] aggBufferTypes = new InternalType[1 + orderKeyTypes.length];
		aggBufferTypes[0] = InternalTypes.LONG;
		System.arraycopy(orderKeyTypes, 0, aggBufferTypes, 1, orderKeyTypes.length);
		return aggBufferTypes;
	}

	@Override
	public Expression[] initialValuesExpressions() {
		Expression[] initExpressions = new Expression[1 + orderKeyTypes.length];
		// sequence = 0L
		initExpressions[0] = literal(0L);
		for (int i = 0; i < orderKeyTypes.length; ++i) {
			// lastValue_i = init value
			initExpressions[i + 1] = generateInitLiteral(orderKeyTypes[i]);
		}
		return initExpressions;
	}

	@Override
	public Expression[] accumulateExpressions() {
		Expression[] accExpressions = new Expression[1 + operands().length];
		// sequence = if (lastValues equalTo orderKeys) sequence else sequence + 1
		accExpressions[0] = ifThenElse(orderKeyEqualsExpression(), sequence, plus(sequence, literal(1L)));
		Expression[] operands = operands();
		for (int i = 0; i < operands.length; ++i) {
			// lastValue_i = orderKey[i]
			accExpressions[i + 1] = operands[i];
		}
		return accExpressions;
	}

}
