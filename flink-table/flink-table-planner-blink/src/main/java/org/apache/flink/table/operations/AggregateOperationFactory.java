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

package org.apache.flink.table.operations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.expressions.AggregateFunctionDefinition;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.functions.TableAggregateFunction;

import java.util.stream.Stream;

/**
 * Utility class for creating a valid {@link AggregateQueryOperation} or {@link WindowAggregateQueryOperation}.
 */
@Internal
public class AggregateOperationFactory {

	/**
	 * Return true if the input {@link Expression} is a {@link CallExpression} of table aggregate function.
	 */
	public static boolean isTableAggFunctionCall(Expression expression) {
		return Stream.of(expression)
			.filter(p -> p instanceof CallExpression)
			.map(p -> (CallExpression) p)
			.filter(p -> p.getFunctionDefinition() instanceof AggregateFunctionDefinition)
			.map(p -> (AggregateFunctionDefinition) p.getFunctionDefinition())
			.anyMatch(p -> p.getAggregateFunction() instanceof TableAggregateFunction);
	}
}
