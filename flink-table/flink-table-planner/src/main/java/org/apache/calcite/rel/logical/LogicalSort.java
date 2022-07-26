/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.calcite.rel.logical;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexNode;

/** Sub-class of {@link Sort} not targeted at any particular engine or calling convention. */
public final class LogicalSort extends Sort {
    private LogicalSort(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode input,
            RelCollation collation,
            RexNode offset,
            RexNode fetch) {
        super(cluster, traitSet, input, collation, offset, fetch);
        assert traitSet.containsIfApplicable(Convention.NONE);
    }

    /** Creates a LogicalSort by parsing serialized output. */
    public LogicalSort(RelInput input) {
        super(input);
    }

    /**
     * Creates a LogicalSort.
     *
     * @param input Input relational expression
     * @param collation array of sort specifications
     * @param offset Expression for number of rows to discard before returning first row
     * @param fetch Expression for number of rows to fetch
     */
    public static LogicalSort create(
            RelNode input, RelCollation collation, RexNode offset, RexNode fetch) {
        RelOptCluster cluster = input.getCluster();
        collation = RelCollationTraitDef.INSTANCE.canonize(collation);
        RelTraitSet traitSet = input.getTraitSet().replace(Convention.NONE).replace(collation);
        return new LogicalSort(cluster, traitSet, input, collation, offset, fetch);
    }

    // ~ Methods ----------------------------------------------------------------

    @Override
    public Sort copy(
            RelTraitSet traitSet,
            RelNode newInput,
            RelCollation newCollation,
            RexNode offset,
            RexNode fetch) {
        return new LogicalSort(getCluster(), traitSet, newInput, newCollation, offset, fetch);
    }

    @Override
    public RelNode accept(RelShuttle shuttle) {
        return shuttle.visit(this);
    }
}
