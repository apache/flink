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
import org.apache.flink.table.type.InternalType;
import org.apache.flink.table.type.InternalTypes;

import static org.apache.flink.table.expressions.ExpressionBuilder.literal;
import static org.apache.flink.table.expressions.ExpressionBuilder.plus;

/**
 * built-in row_number aggregate function.
 */
public class RowNumberAggFunction extends DeclarativeAggregateFunction {
	private UnresolvedReferenceExpression sequence = new UnresolvedReferenceExpression("seq");

	@Override
	public int operandCount() {
		return 0;
	}

	@Override
	public UnresolvedReferenceExpression[] aggBufferAttributes() {
		return new UnresolvedReferenceExpression[] { sequence };
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
				literal(0L)
		};
	}

	@Override
	public Expression[] accumulateExpressions() {
		return new Expression[] {
				plus(sequence, literal(1L))
		};
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
}
