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

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.planner.plan.reuse.ScanReuserUtils.getDigest;

/** Find reusable sources. */
public class ReusableScanVisitor extends RelVisitor {

    private final Map<String, List<CommonPhysicalTableSourceScan>> digestToReusableScans =
            new HashMap<>();

    @Override
    public void visit(RelNode node, int ordinal, RelNode parent) {
        if (node instanceof CommonPhysicalTableSourceScan) {
            CommonPhysicalTableSourceScan scan = (CommonPhysicalTableSourceScan) node;
            String digest = getDigest(scan, true);
            digestToReusableScans.computeIfAbsent(digest, k -> new ArrayList<>()).add(scan);
            // If the scan has input such as dpp dynamic scan node, so also need to consider the
            // input
            if (!scan.getInputs().isEmpty()) {
                super.visit(scan.getInput(0), 0, scan);
            }
        } else {
            super.visit(node, ordinal, parent);
        }
    }

    public Map<String, List<CommonPhysicalTableSourceScan>> digestToReusableScans() {
        return digestToReusableScans;
    }
}
