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

/**
 * Represents an artifact that can be explained using a summary string.
 *
 * @see #explain(ExplainDetail...)
 */
@PublicEvolving
public interface Explainable<SELF extends Explainable<SELF>> {

    /**
     * Returns the AST of this object and the execution plan to compute the result of the given
     * statement.
     *
     * @param extraDetails The extra explain details which the result of this method should include,
     *     e.g. estimated cost, changelog mode for streaming
     * @return AST and the execution plan.
     */
    default String explain(ExplainDetail... extraDetails) {
        return explain(ExplainFormat.TEXT, extraDetails);
    }

    /**
     * Returns the AST of this object and the execution plan to compute the result of the given
     * statement.
     *
     * @param format The output format of explained plan
     * @param extraDetails The extra explain details which the result of this method should include,
     *     e.g. estimated cost, changelog mode for streaming
     * @return AST and the execution plan.
     */
    String explain(ExplainFormat format, ExplainDetail... extraDetails);

    /** Like {@link #explain(ExplainDetail...)}, but piping the result to {@link System#out}. */
    @SuppressWarnings("unchecked")
    default SELF printExplain(ExplainDetail... extraDetails) {
        System.out.println(explain(extraDetails));
        return (SELF) this;
    }
}
