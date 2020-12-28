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

package org.apache.flink.table.api;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.expressions.ApiExpressionUtils;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionParser;

/** Session window on time. */
@PublicEvolving
public final class SessionWithGapOnTime {

    private final Expression timeField;
    private final Expression gap;

    SessionWithGapOnTime(Expression timeField, Expression gap) {
        this.timeField = ApiExpressionUtils.unwrapFromApi(timeField);
        this.gap = ApiExpressionUtils.unwrapFromApi(gap);
    }

    /**
     * Assigns an alias for this window that the following {@code groupBy()} and {@code select()}
     * clause can refer to. {@code select()} statement can access window properties such as window
     * start or end time.
     *
     * @param alias alias for this window
     * @return this window
     */
    public SessionWithGapOnTimeWithAlias as(String alias) {
        return as(ExpressionParser.parseExpression(alias));
    }

    /**
     * Assigns an alias for this window that the following {@code groupBy()} and {@code select()}
     * clause can refer to. {@code select()} statement can access window properties such as window
     * start or end time.
     *
     * @param alias alias for this window
     * @return this window
     */
    public SessionWithGapOnTimeWithAlias as(Expression alias) {
        return new SessionWithGapOnTimeWithAlias(alias, timeField, gap);
    }
}
