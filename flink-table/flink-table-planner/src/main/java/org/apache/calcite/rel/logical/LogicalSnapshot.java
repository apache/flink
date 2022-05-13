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

package org.apache.calcite.rel.logical;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Snapshot;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMdDistribution;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Litmus;

/**
 * Sub-class of {@link org.apache.calcite.rel.core.Snapshot} not targeted at any particular engine
 * or calling convention. The class was copied over because of * CALCITE-4554. *
 *
 * <p>Line 80 ~ 91: Calcite only supports timestamp type as period type, but Flink supports both
 * Timestamp and TimestampLtz. Should be removed once calcite support TimestampLtz as period type.
 */
public class LogicalSnapshot extends Snapshot {

    // ~ Constructors -----------------------------------------------------------

    /**
     * Creates a LogicalSnapshot.
     *
     * <p>Use {@link #create} unless you know what you're doing.
     *
     * @param cluster Cluster that this relational expression belongs to
     * @param traitSet The traits of this relational expression
     * @param input Input relational expression
     * @param period Timestamp expression which as the table was at the given time in the past
     */
    public LogicalSnapshot(
            RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RexNode period) {
        super(cluster, traitSet, input, period);
    }

    @Override
    public Snapshot copy(RelTraitSet traitSet, RelNode input, RexNode period) {
        return new LogicalSnapshot(getCluster(), traitSet, input, period);
    }

    /** Creates a LogicalSnapshot. */
    public static LogicalSnapshot create(RelNode input, RexNode period) {
        final RelOptCluster cluster = input.getCluster();
        final RelMetadataQuery mq = cluster.getMetadataQuery();
        final RelTraitSet traitSet =
                cluster.traitSet()
                        .replace(Convention.NONE)
                        .replaceIfs(
                                RelCollationTraitDef.INSTANCE,
                                () -> RelMdCollation.snapshot(mq, input))
                        .replaceIf(
                                RelDistributionTraitDef.INSTANCE,
                                () -> RelMdDistribution.snapshot(mq, input));
        return new LogicalSnapshot(cluster, traitSet, input, period);
    }

    @Override
    public boolean isValid(Litmus litmus, Context context) {
        SqlTypeName periodTypeName = getPeriod().getType().getSqlTypeName();

        if (!(periodTypeName == SqlTypeName.TIMESTAMP
                || periodTypeName == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE)) {
            return litmus.fail(
                    "The system time period specification expects TIMESTAMP or TIMESTAMP"
                            + " WITH LOCAL TIME ZONE type but is '"
                            + periodTypeName
                            + "'");
        }
        return litmus.succeed();
    }
}
