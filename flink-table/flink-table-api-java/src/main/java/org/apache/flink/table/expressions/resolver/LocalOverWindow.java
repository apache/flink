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

package org.apache.flink.table.expressions.resolver;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.expressions.Expression;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;

/** Local over window created during expression resolution. */
@Internal
public final class LocalOverWindow {

    private Expression alias;

    private List<Expression> partitionBy;

    private Expression orderBy;

    private @Nullable Expression preceding;

    private @Nullable Expression following;

    LocalOverWindow(
            Expression alias,
            List<Expression> partitionBy,
            Expression orderBy,
            @Nullable Expression preceding,
            @Nullable Expression following) {
        this.alias = alias;
        this.partitionBy = partitionBy;
        this.orderBy = orderBy;
        this.preceding = preceding;
        this.following = following;
    }

    public Expression getAlias() {
        return alias;
    }

    public List<Expression> getPartitionBy() {
        return partitionBy;
    }

    public Expression getOrderBy() {
        return orderBy;
    }

    public Optional<Expression> getPreceding() {
        return Optional.ofNullable(preceding);
    }

    public Optional<Expression> getFollowing() {
        return Optional.ofNullable(following);
    }
}
