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
import org.apache.flink.table.expressions.Expression;

import java.util.List;

import static org.apache.flink.table.expressions.ApiExpressionUtils.unresolvedRef;

/** Partially defined over window with (optional) partitioning and order. */
@PublicEvolving
public final class OverWindowPartitionedOrdered {

    private final List<Expression> partitionBy;
    private final Expression orderBy;

    OverWindowPartitionedOrdered(List<Expression> partitionBy, Expression orderBy) {
        this.partitionBy = partitionBy;
        this.orderBy = orderBy;
    }

    /**
     * Set the preceding offset (based on time or row-count intervals) for over window.
     *
     * @param preceding preceding offset relative to the current row.
     * @return an over window with defined preceding
     */
    public OverWindowPartitionedOrderedPreceding preceding(Expression preceding) {
        return new OverWindowPartitionedOrderedPreceding(partitionBy, orderBy, preceding);
    }

    /**
     * Assigns an alias for this window that the following {@code select()} clause can refer to.
     *
     * @param alias alias for this over window
     * @return the fully defined over window
     */
    public OverWindow as(String alias) {
        return as(unresolvedRef(alias));
    }

    /**
     * Assigns an alias for this window that the following {@code select()} clause can refer to.
     *
     * @param alias alias for this over window
     * @return the fully defined over window
     */
    public OverWindow as(Expression alias) {
        return new OverWindow(alias, partitionBy, orderBy, null, null);
    }
}
