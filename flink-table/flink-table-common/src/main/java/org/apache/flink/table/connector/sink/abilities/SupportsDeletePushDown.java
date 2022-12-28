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

import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.expressions.ResolvedExpression;

import java.util.List;
import java.util.Optional;

/**
 * Enable to delete existing data in a {@link DynamicTableSink} directly according to the filter
 * expressions in {@code DELETE} clause.
 *
 * <p>Given the following SQL:
 *
 * <pre>{@code
 * DELETE FROM t WHERE (a = '1' OR a = '2') AND b IS NOT NULL;
 *
 * }</pre>
 *
 * <p>In the example above, {@code [a = '1' OR a = '2']} and {@code [b IS NOT NULL]} are acceptable
 * filters.
 *
 * <p>Flink will get the filters in conjunctive form and push down the filters into sink by calling
 * method {@link #applyDeleteFilters} in the planner phase. If it returns true, Flink will then call
 * method to execute the actual delete in execution phase.
 *
 * <p>Note: in the case that the filter expression is not available, e.g., sub-query or {@link
 * #applyDeleteFilters} returns false, if the sink implements {@link SupportsRowLevelDelete}, Flink
 * will try to rewrite the delete operation and produce row-level changes, see {@link
 * SupportsRowLevelDelete} for more details. Otherwise, Flink will throw {@link
 * UnsupportedOperationException}.
 */
public interface SupportsDeletePushDown {

    /**
     * Provides a list of filters from delete operation in conjunctive form in planning phase. A
     * sink can either return true if it can accept all filters or return false if it can not
     * accept.
     *
     * <p>If it returns true, Flink will then call the method {@link #executeDeletion} in execution
     * phase to do the actual deletion.
     *
     * <p>If it returns false, and the sink still implements {@link SupportsRowLevelDelete}, Flink
     * will rewrite the delete operation and produce row-level changes. Otherwise, Flink will throw
     * {@link UnsupportedOperationException}.
     */
    boolean applyDeleteFilters(List<ResolvedExpression> filters);

    /**
     * Do the actual deletion in the execution phase, and return how many rows has been deleted,
     * Optional.empty() is for unknown delete rows.
     *
     * <p>Note that this method will be involved if and only if {@link #applyDeleteFilters(List
     * ResolvedExpression)} returns true. So, please make sure the implementation for this method
     * will do delete the data correctly.
     *
     * <p>Note that in this method, the sink won't get the filters since they have been passed to
     * the method {@link #applyDeleteFilters} before. The sink may need to keep these filters, so
     * that it can get the filters if necessary to finish the deletion.
     */
    Optional<Long> executeDeletion();
}
