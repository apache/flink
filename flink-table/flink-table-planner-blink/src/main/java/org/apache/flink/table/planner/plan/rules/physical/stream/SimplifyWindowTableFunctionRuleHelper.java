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

package org.apache.flink.table.planner.plan.rules.physical.stream;

import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWindowTableFunction;

import org.apache.calcite.rel.RelNode;

import java.util.Collections;
import java.util.List;

/**
 * A utility class to help {@link SimplifyWindowTableFunctionWithWindowJoinRule} and {@link
 * SimplifyWindowTableFunctionWithWindowRankRule}.
 */
public class SimplifyWindowTableFunctionRuleHelper {

    /**
     * Replace the leaf node, and build a new {@link RelNode} tree in the given nodes order which is
     * in root-down direction.
     */
    public static RelNode rebuild(List<RelNode> nodes) {
        final StreamPhysicalWindowTableFunction windowTVF =
                (StreamPhysicalWindowTableFunction) nodes.get(nodes.size() - 1);
        if (needSimplify(windowTVF)) {
            final StreamPhysicalWindowTableFunction newWindowTVF = windowTVF.copy(true);
            RelNode root = newWindowTVF;
            for (int i = nodes.size() - 2; i >= 0; i--) {
                RelNode node = nodes.get(i);
                root = node.copy(node.getTraitSet(), Collections.singletonList(root));
            }
            return root;
        } else {
            return nodes.get(0);
        }
    }

    public static boolean needSimplify(StreamPhysicalWindowTableFunction windowTVF) {
        // excludes windowTVF which is already simplified to emit by per record
        return windowTVF.windowing().isRowtime() && !windowTVF.emitPerRecord();
    }

    private SimplifyWindowTableFunctionRuleHelper() {}
}
