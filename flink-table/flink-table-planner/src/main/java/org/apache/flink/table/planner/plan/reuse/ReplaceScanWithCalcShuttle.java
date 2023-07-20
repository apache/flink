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

package org.apache.flink.table.planner.plan.reuse;

import org.apache.flink.table.planner.plan.nodes.physical.common.CommonPhysicalTableSourceScan;
import org.apache.flink.table.planner.plan.utils.DefaultRelShuttle;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;

import java.util.Map;

import static org.apache.flink.table.planner.plan.utils.FlinkRelUtil.isMergeable;
import static org.apache.flink.table.planner.plan.utils.FlinkRelUtil.merge;

/** Replace {@link CommonPhysicalTableSourceScan} with {@link Calc}. */
public class ReplaceScanWithCalcShuttle extends DefaultRelShuttle {

    private final Map<CommonPhysicalTableSourceScan, RelNode> replaceMap;

    public ReplaceScanWithCalcShuttle(Map<CommonPhysicalTableSourceScan, RelNode> replaceMap) {
        this.replaceMap = replaceMap;
    }

    @Override
    public RelNode visit(RelNode rel) {
        if (rel instanceof Calc && rel.getInput(0) instanceof CommonPhysicalTableSourceScan) {
            // if there is already one Calc, we should merge it and new projection node.
            Calc calc = (Calc) rel;
            RelNode input = calc.getInput();
            RelNode newNode = replaceMap.get(input);
            if (newNode instanceof Calc && isMergeable(calc, (Calc) newNode)) {
                return merge(calc, (Calc) newNode);
            }
        } else if (rel instanceof CommonPhysicalTableSourceScan) {
            RelNode newNode = replaceMap.get(rel);
            if (newNode != null) {
                return newNode;
            }
        }

        return super.visit(rel);
    }
}
