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

package org.apache.flink.table.delegation;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionParser;

import java.lang.reflect.Constructor;
import java.util.List;

/**
 * Temporary utility for parsing expressions inside a String. This parses exactly the same expressions
 * that would be accepted by the Scala Expression DSL.
 *
 * <p>{@link PlannerExpressionParser} is used by {@link ExpressionParser} to parse expressions.
 */
@Internal
public interface PlannerExpressionParser {

	static PlannerExpressionParser create() {
		return SingletonPlannerExpressionParser.getExpressionParser();
	}

	Expression parseExpression(String exprString);

	List<Expression> parseExpressionList(String expression);

	/**
	 * Util class to create {@link PlannerExpressionParser} instance. Use singleton pattern to avoid
	 * creating many {@link PlannerExpressionParser}.
	 */
	class SingletonPlannerExpressionParser {

		private static volatile PlannerExpressionParser expressionParser;

		private SingletonPlannerExpressionParser() {}

		public static PlannerExpressionParser getExpressionParser() {

			if (expressionParser == null) {
				try {
					Class<?> clazz = Class.forName("org.apache.flink.table.expressions.PlannerExpressionParserImpl");
					Constructor<?> con = clazz.getConstructor();
					expressionParser = (PlannerExpressionParser) con.newInstance();
				} catch (Throwable t) {
					throw new TableException("Construction of PlannerExpressionParserImpl class failed.", t);
				}
			}
			return expressionParser;
		}
	}
}
