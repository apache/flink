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
import java.util.Optional;

/**
 * An over window specification.
 *
 * <p>Similar to SQL, over window aggregates compute an aggregate for each input row over a range of
 * its neighboring rows.
 */
@PublicEvolving
public final class OverWindow {

    private final Expression alias;
    private final List<Expression> partitioning;
    private final Expression order;
    private final @Nullable Expression preceding;
    private final @Nullable Expression following;

    OverWindow(
            Expression alias,
            List<Expression> partitionBy,
            Expression orderBy,
            @Nullable Expression preceding,
            @Nullable Expression following) {
        this.alias = alias;
        this.partitioning = partitionBy;
        this.order = orderBy;
        this.preceding = preceding;
        this.following = following;
    }

    public Expression getAlias() {
        return alias;
    }

    public List<Expression> getPartitioning() {
        return partitioning;
    }

    public Expression getOrder() {
        return order;
    }

    public Optional<Expression> getPreceding() {
        return Optional.ofNullable(preceding);
    }

    public Optional<Expression> getFollowing() {
        return Optional.ofNullable(following);
    }
}
