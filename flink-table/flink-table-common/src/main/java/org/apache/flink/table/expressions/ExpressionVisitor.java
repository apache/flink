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

package org.apache.flink.table.expressions;

import org.apache.flink.annotation.PublicEvolving;

/**
 * The visitor definition of {@link Expression}.
 *
 * <p>An expression visitor transforms an expression to instances of {@code R}.
 *
 * <p>Please note that only {@link ResolvedExpression}s are listed here. Pure API expression are
 * handled in {@link #visit(Expression)}.
 */
@PublicEvolving
public interface ExpressionVisitor<R> {

    // --------------------------------------------------------------------------------------------
    // resolved expressions
    // --------------------------------------------------------------------------------------------

    R visit(CallExpression call);

    R visit(ValueLiteralExpression valueLiteral);

    R visit(FieldReferenceExpression fieldReference);

    R visit(TypeLiteralExpression typeLiteral);

    // --------------------------------------------------------------------------------------------
    // other expressions
    // --------------------------------------------------------------------------------------------

    R visit(Expression other);
}
