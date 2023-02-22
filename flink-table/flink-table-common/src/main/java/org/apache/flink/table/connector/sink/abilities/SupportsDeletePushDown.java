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

package org.apache.flink.table.connector.sink.abilities;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.expressions.ResolvedExpression;

import java.util.List;
import java.util.Optional;

/**
 * Enables to push down filters decomposed from the {@code WHERE} clause in delete statement to
 * {@link DynamicTableSink}. The table sink can delete existing data directly according to the
 * filters.
 *
 * <p>Flink will get the filters in conjunctive form and push down the filters into sink by calling
 * method {@link #applyDeleteFilters(List)} in the planning phase. If it returns true, Flink will
 * then call {@link #executeDeletion()} to execute the actual deletion during execution phase.
 *
 * <p>Given the following SQL:
 *
 * <pre>{@code
 * DELETE FROM t WHERE (a = '1' OR a = '2') AND b IS NOT NULL;*
 * }</pre>
 *
 * <p>In the example above, the {@code WHERE} clause will be decomposed into two filters
 *
 * <ul>
 *   <li>{@code [a = '1' OR a = '2']}
 *   <li>{@code [b IS NOT NULL]}
 * </ul>
 *
 * <p>If the sink can accept both filters which means the sink can delete data directly according to
 * the filters, {@link #applyDeleteFilters(List)} should return true. Otherwise, it should return
 * false.
 *
 * <p>Note: For the cases where the filter expression is not available, e.g., sub-query or {@link
 * #applyDeleteFilters(List)} returns false, if the sink implements {@link SupportsRowLevelDelete},
 * Flink will try to rewrite the delete statement and produce row-level changes, see {@link
 * SupportsRowLevelDelete} for more details. Otherwise, Flink will throw {@link
 * UnsupportedOperationException}.
 */
@PublicEvolving
public interface SupportsDeletePushDown {

    /**
     * Provides a list of filters specified by {@code WHERE} clause in conjunctive form and return
     * the acceptance status to planner during planning phase.
     *
     * @param filters a list of resolved filter expressions.
     * @return true if the sink accepts all filters; false otherwise.
     */
    boolean applyDeleteFilters(List<ResolvedExpression> filters);

    /**
     * Deletes data during execution phase.
     *
     * <p>Note: The method will be involved iff the method {@link #applyDeleteFilters(List)} returns
     * true.
     *
     * @return the number of the estimated rows to be deleted, or {@link Optional#empty()} for the
     *     unknown condition.
     */
    Optional<Long> executeDeletion();
}
