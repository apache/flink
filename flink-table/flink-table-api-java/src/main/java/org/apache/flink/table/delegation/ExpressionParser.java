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
import org.apache.flink.table.expressions.Expression;

import java.util.List;

/**
 * {@link Expression} parser used by Table API to parse strings in AST expression. This parses
 * exactly the same expressions that would be accepted by the Scala Expression DSL.
 *
 * @deprecated The Java String Expression DSL is deprecated.
 */
@Internal
@Deprecated
public interface ExpressionParser {

    /** Default instance of the {@link ExpressionParser}. */
    ExpressionParser INSTANCE = ExpressionParserFactory.getDefault().create();

    Expression parseExpression(String exprString);

    List<Expression> parseExpressionList(String expression);
}
