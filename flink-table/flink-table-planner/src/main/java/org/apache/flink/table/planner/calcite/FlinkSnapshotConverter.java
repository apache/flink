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

package org.apache.flink.table.planner.calcite;

import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalCalc;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalSnapshot;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalWatermarkAssigner;
import org.apache.flink.table.planner.plan.schema.TimeIndicatorRelDataType;

import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;

/**
 * Traverses a {@link RelNode} tree and update the child node of {@link FlinkLogicalSnapshot} to set
 * {@link FlinkLogicalTableSourceScan#eventTimeSnapshot} property.
 *
 * <p>Note: only snapshot on event time period will update the child {@link
 * FlinkLogicalTableSourceScan}.
 */
public final class FlinkSnapshotConverter extends RelHomogeneousShuttle {

    public static RelNode update(RelNode input) {
        FlinkSnapshotConverter converter = new FlinkSnapshotConverter();
        return input.accept(converter);
    }

    @Override
    public RelNode visit(RelNode node) {
        if (node instanceof FlinkLogicalSnapshot) {
            final FlinkLogicalSnapshot snapshot = (FlinkLogicalSnapshot) node;
            if (isEventTime(snapshot.getPeriod().getType())) {
                final RelNode child = snapshot.getInput();
                final RelNode newChild = transmitSnapshotRequirement(child);
                if (newChild != child) {
                    return snapshot.copy(snapshot.getTraitSet(), newChild, snapshot.getPeriod());
                }
            }
            return snapshot;
        }
        return super.visit(node);
    }

    private boolean isEventTime(RelDataType period) {
        if (period instanceof TimeIndicatorRelDataType) {
            return ((TimeIndicatorRelDataType) period).isEventTime();
        }
        return false;
    }

    private RelNode transmitSnapshotRequirement(RelNode node) {
        if (node instanceof FlinkLogicalCalc) {
            final FlinkLogicalCalc calc = (FlinkLogicalCalc) node;
            final RelNode child = calc.getInput();
            final RelNode newChild = transmitSnapshotRequirement(child);
            if (newChild != child) {
                return calc.copy(calc.getTraitSet(), newChild, calc.getProgram());
            }
            return calc;
        }
        if (node instanceof FlinkLogicalWatermarkAssigner) {
            final FlinkLogicalWatermarkAssigner wma = (FlinkLogicalWatermarkAssigner) node;
            final RelNode child = wma.getInput();
            final RelNode newChild = transmitSnapshotRequirement(child);
            if (newChild != child) {
                return wma.copy(
                        wma.getTraitSet(), newChild, wma.rowtimeFieldIndex(), wma.watermarkExpr());
            }
            return wma;
        }
        if (node instanceof FlinkLogicalTableSourceScan) {
            final FlinkLogicalTableSourceScan ts = (FlinkLogicalTableSourceScan) node;
            // update eventTimeSnapshot to true
            return ts.copy(ts.getTraitSet(), ts.relOptTable(), true);
        }
        return node;
    }
}
