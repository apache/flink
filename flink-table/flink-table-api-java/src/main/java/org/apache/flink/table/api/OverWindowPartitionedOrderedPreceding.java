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

import javax.annotation.Nullable;

import java.util.List;

import static org.apache.flink.table.expressions.ApiExpressionUtils.unresolvedRef;

/** Partially defined over window with (optional) partitioning, order, and preceding. */
@PublicEvolving
public final class OverWindowPartitionedOrderedPreceding {

    private final List<Expression> partitionBy;
    private final Expression orderBy;
    private final @Nullable Expression preceding;
    private @Nullable Expression optionalFollowing = null;

    OverWindowPartitionedOrderedPreceding(
            List<Expression> partitionBy, Expression orderBy, Expression preceding) {
        this.partitionBy = partitionBy;
        this.orderBy = orderBy;
        this.preceding = preceding;
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
        return new OverWindow(alias, partitionBy, orderBy, preceding, optionalFollowing);
    }

    /**
     * Set the following offset (based on time or row-count intervals) for over window.
     *
     * @param following following offset that relative to the current row.
     * @return an over window with defined following
     */
    public OverWindowPartitionedOrderedPreceding following(Expression following) {
        optionalFollowing = following;
        return this;
    }
}
