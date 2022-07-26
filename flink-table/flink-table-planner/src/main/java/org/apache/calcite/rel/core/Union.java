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

package org.apache.calcite.rel.core;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.sql.SqlKind;

import java.util.Collections;
import java.util.List;

/**
 * Relational expression that returns the union of the rows of its inputs, optionally eliminating
 * duplicates.
 *
 * <p>Temporarily copy from calcite to cherry-pick [CALCITE-5107] and will be removed when upgrade
 * the latest calcite.
 *
 * <p>Corresponds to SQL {@code UNION} and {@code UNION ALL}.
 */
public abstract class Union extends SetOp {
    // ~ Constructors -----------------------------------------------------------

    protected Union(
            RelOptCluster cluster,
            RelTraitSet traits,
            List<RelHint> hints,
            List<RelNode> inputs,
            boolean all) {
        super(cluster, traits, hints, inputs, SqlKind.UNION, all);
    }

    protected Union(RelOptCluster cluster, RelTraitSet traits, List<RelNode> inputs, boolean all) {
        super(cluster, traits, Collections.emptyList(), inputs, SqlKind.UNION, all);
    }

    /** Creates a Union by parsing serialized output. */
    protected Union(RelInput input) {
        super(input);
    }

    // ~ Methods ----------------------------------------------------------------

    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        double dRows = RelMdUtil.getUnionAllRowCount(mq, this);
        if (!all) {
            dRows *= 0.5;
        }
        return dRows;
    }

    @Deprecated // to be removed before 2.0
    public static double estimateRowCount(RelNode rel) {
        final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
        return RelMdUtil.getUnionAllRowCount(mq, (Union) rel);
    }
}
