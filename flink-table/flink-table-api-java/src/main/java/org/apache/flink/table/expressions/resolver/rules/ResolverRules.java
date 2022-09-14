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

package org.apache.flink.table.expressions.resolver.rules;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.ApiExpression;
import org.apache.flink.table.expressions.SqlCallExpression;
import org.apache.flink.table.expressions.UnresolvedReferenceExpression;

/** Contains instances of {@link ResolverRule}. */
@Internal
public final class ResolverRules {

    /**
     * Resolves {@link UnresolvedReferenceExpression}. See {@link ReferenceResolverRule} for
     * details.
     */
    public static final ResolverRule FIELD_RESOLVE = new ReferenceResolverRule();

    /** Resolves {@link SqlCallExpression}s. */
    public static final ResolverRule RESOLVE_SQL_CALL = new ResolveSqlCallRule();

    /**
     * Resolves call based on argument types. See {@link ResolveCallByArgumentsRule} for details.
     */
    public static final ResolverRule RESOLVE_CALL_BY_ARGUMENTS = new ResolveCallByArgumentsRule();

    /** Looks up unresolved call by name. See {@link LookupCallByNameRule} for details. */
    public static final ResolverRule LOOKUP_CALL_BY_NAME = new LookupCallByNameRule();

    /**
     * Concatenates over aggregations with corresponding over window. See {@link
     * OverWindowResolverRule} for details.
     */
    public static final ResolverRule OVER_WINDOWS = new OverWindowResolverRule();

    /**
     * Resolves '*' expressions to corresponding fields of inputs. See {@link
     * StarReferenceFlatteningRule} for details.
     */
    public static final ResolverRule FLATTEN_STAR_REFERENCE = new StarReferenceFlatteningRule();

    /**
     * Resolves column functions to corresponding fields of inputs. See {@link
     * ExpandColumnFunctionsRule} for details.
     */
    public static final ResolverRule EXPAND_COLUMN_FUNCTIONS = new ExpandColumnFunctionsRule();

    /** Looks up unresolved calls of built-in functions to make them fully qualified. */
    public static final ResolverRule QUALIFY_BUILT_IN_FUNCTIONS = new QualifyBuiltInFunctionsRule();

    /** Unwraps all {@link ApiExpression}. */
    public static final ResolverRule UNWRAP_API_EXPRESSION = new UnwrapApiExpressionRule();

    private ResolverRules() {}
}
