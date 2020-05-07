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

package org.apache.flink.table.planner.expressions.converter;

import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;

import java.util.Optional;

/**
 * Rule to convert {@link CallExpression}.
 */
public interface CallExpressionConvertRule {

	/**
	 * Convert call expression with context to RexNode.
	 *
	 * @return Success return RexNode of {@link Optional#of}, Fail return {@link Optional#empty()}.
	 */
	Optional<RexNode> convert(CallExpression call, ConvertContext context);

	/**
	 * Context of {@link CallExpressionConvertRule}.
	 */
	interface ConvertContext {

		/**
		 * Convert expression to RexNode, used by children conversion.
		 */
		RexNode toRexNode(Expression expr);

		RelBuilder getRelBuilder();

		FlinkTypeFactory getTypeFactory();

		DataTypeFactory getDataTypeFactory();
	}
}
