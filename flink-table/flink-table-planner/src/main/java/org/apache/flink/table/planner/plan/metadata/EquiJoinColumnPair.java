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

package org.apache.flink.table.planner.plan.metadata;

import java.util.Objects;

/**
 * Two output columns that an equi-join condition equates, see {@link
 * FlinkMetadata.EquiJoinColumnPairs}.
 *
 * <p>The two columns are equal or both null in every row, unless the pair comes from an outer
 * join's own condition: in the rows that join pads with nulls, the column on the null-generating
 * side is null while the other column need not be.
 */
public final class EquiJoinColumnPair {

    private final int source;
    private final int target;
    private final boolean fromOuterJoin;

    private EquiJoinColumnPair(int source, int target, boolean fromOuterJoin) {
        this.source = source;
        this.target = target;
        this.fromOuterJoin = fromOuterJoin;
    }

    public static EquiJoinColumnPair of(int source, int target, boolean fromOuterJoin) {
        return new EquiJoinColumnPair(source, target, fromOuterJoin);
    }

    /** One column of the pair. */
    public int getSource() {
        return source;
    }

    /** The other column of the pair. */
    public int getTarget() {
        return target;
    }

    /** Whether the pair comes from an outer join's own condition, see the class comment. */
    public boolean isFromOuterJoin() {
        return fromOuterJoin;
    }

    /** The same pair with its columns at the given positions. */
    public EquiJoinColumnPair withColumns(int source, int target) {
        return new EquiJoinColumnPair(source, target, fromOuterJoin);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        EquiJoinColumnPair that = (EquiJoinColumnPair) o;
        return source == that.source
                && target == that.target
                && fromOuterJoin == that.fromOuterJoin;
    }

    @Override
    public int hashCode() {
        return Objects.hash(source, target, fromOuterJoin);
    }

    @Override
    public String toString() {
        return "(" + source + ", " + target + (fromOuterJoin ? ", outer join" : "") + ")";
    }
}
