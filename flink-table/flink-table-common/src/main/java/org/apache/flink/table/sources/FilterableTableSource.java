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

package org.apache.flink.table.sources;

import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.expressions.Expression;

import java.util.List;

/**
 * Adds support for filtering push-down to a {@link TableSource}. A {@link TableSource} extending
 * this interface is able to filter records before returning.
 *
 * @deprecated This interface will not be supported in the new source design around {@link
 *     DynamicTableSource}. Use {@link SupportsFilterPushDown} instead. See FLIP-95 for more
 *     information.
 */
@Deprecated
public interface FilterableTableSource<T> {

    /**
     * Check and pick all predicates this table source can support. The passed in predicates have
     * been translated in conjunctive form, and table source can only pick those predicates that it
     * supports.
     *
     * <p>After trying to push predicates down, we should return a new {@link TableSource} instance
     * which holds all pushed down predicates. Even if we actually pushed nothing down, it is
     * recommended that we still return a new {@link TableSource} instance since we will mark the
     * returned instance as filter push down has been tried.
     *
     * <p>We also should note to not changing the form of the predicates passed in. It has been
     * organized in CNF conjunctive form, and we should only take or leave each element from the
     * list. Don't try to reorganize the predicates if you are absolutely confident with that.
     *
     * @param predicates A list contains conjunctive predicates, you should pick and remove all
     *     expressions that can be pushed down. The remaining elements of this list will further
     *     evaluated by framework.
     * @return A new cloned instance of {@link TableSource} with or without any filters been pushed
     *     into it.
     */
    TableSource<T> applyPredicate(List<Expression> predicates);

    /**
     * Return the flag to indicate whether filter push down has been tried. Must return true on the
     * returned instance of {@link #applyPredicate}.
     */
    boolean isFilterPushedDown();
}
