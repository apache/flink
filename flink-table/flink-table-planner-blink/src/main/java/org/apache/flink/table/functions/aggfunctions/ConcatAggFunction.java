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
import org.apache.flink.table.expressions.UnresolvedReferenceExpression;
import org.apache.flink.table.types.DataType;

import static org.apache.flink.table.expressions.ExpressionBuilder.concat;
import static org.apache.flink.table.expressions.ExpressionBuilder.ifThenElse;
import static org.apache.flink.table.expressions.ExpressionBuilder.isNull;
import static org.apache.flink.table.expressions.ExpressionBuilder.literal;
import static org.apache.flink.table.expressions.ExpressionBuilder.nullOf;

/**
 * built-in concat aggregate function.
 */
public class ConcatAggFunction extends DeclarativeAggregateFunction {
	private int operandCount;
	private UnresolvedReferenceExpression acc = new UnresolvedReferenceExpression("concatAcc");
	private UnresolvedReferenceExpression accDelimiter = new UnresolvedReferenceExpression("accDelimiter");
	private Expression delimiter;
	private Expression operand;

	public ConcatAggFunction(int operandCount) {
		this.operandCount = operandCount;
		if (operandCount == 1) {
			delimiter = literal("\n", DataTypes.STRING());
			operand = operand(0);
		} else {
			delimiter = operand(0);
			operand = operand(1);
		}
	}

	@Override
	public int operandCount() {
		return operandCount;
	}

	@Override
	public UnresolvedReferenceExpression[] aggBufferAttributes() {
		return new UnresolvedReferenceExpression[] { accDelimiter, acc };
	}

	@Override
	public DataType[] getAggBufferTypes() {
		return new DataType[] { DataTypes.STRING(), DataTypes.STRING() };
	}

	@Override
	public DataType getResultType() {
		return DataTypes.STRING();
	}

	@Override
	public Expression[] initialValuesExpressions() {
		return new Expression[] {
				/* delimiter */ literal("\n", DataTypes.STRING()),
				/* acc */ nullOf(DataTypes.STRING())
		};
	}

	@Override
	public Expression[] accumulateExpressions() {
		return new Expression[] {
				/* delimiter */
				delimiter,
				/* acc */
				ifThenElse(isNull(operand), acc,
						ifThenElse(isNull(acc), operand, concat(concat(acc, delimiter), operand)))
		};
	}

	@Override
	public Expression[] retractExpressions() {
		throw new TableException("This function does not support retraction.");
	}

	@Override
	public Expression[] mergeExpressions() {
		return new Expression[] {
				/* delimiter */
				mergeOperand(accDelimiter),
				/* acc */
				ifThenElse(isNull(mergeOperand(acc)), acc,
						ifThenElse(isNull(acc), mergeOperand(acc),
								concat(concat(acc, mergeOperand(accDelimiter)), mergeOperand(acc))))
		};
	}

	@Override
	public Expression getValueExpression() {
		return acc;
	}
}
