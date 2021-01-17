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

package org.apache.flink.table.planner.plan.nodes.exec.stream;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.utils.PartitionSpec;
import org.apache.flink.table.planner.plan.nodes.exec.utils.SortSpec;
import org.apache.flink.table.planner.plan.utils.RankProcessStrategy;
import org.apache.flink.table.runtime.operators.rank.ConstantRankRange;
import org.apache.flink.table.runtime.operators.rank.RankType;
import org.apache.flink.table.types.logical.RowType;

/** Stream {@link ExecNode} for Limit. */
public class StreamExecLimit extends StreamExecRank {

    private final long limitEnd;

    public StreamExecLimit(
            long limitStart,
            long limitEnd,
            boolean generateUpdateBefore,
            boolean needRetraction,
            ExecEdge inputEdge,
            RowType outputType,
            String description) {
        super(
                RankType.ROW_NUMBER,
                PartitionSpec.ALL_IN_ONE,
                SortSpec.ANY,
                new ConstantRankRange(limitStart + 1, limitEnd),
                getRankStrategy(needRetraction),
                false,
                generateUpdateBefore,
                inputEdge,
                outputType,
                description);
        this.limitEnd = limitEnd;
    }

    private static RankProcessStrategy getRankStrategy(boolean needRetraction) {
        if (needRetraction) {
            return RankProcessStrategy.RETRACT_STRATEGY;
        } else {
            return RankProcessStrategy.APPEND_FAST_STRATEGY;
        }
    }

    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner) {
        if (limitEnd == Long.MAX_VALUE) {
            throw new TableException(
                    "FETCH is missed, which on streaming table is not supported currently.");
        }
        return super.translateToPlanInternal(planner);
    }
}
