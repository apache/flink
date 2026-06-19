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

package org.apache.flink.table.runtime.strategy;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.graph.util.ImmutableStreamEdge;
import org.apache.flink.streaming.api.graph.util.ImmutableStreamNode;

import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS;

/** Utils for adaptive join optimization strategies. */
public class AdaptiveJoinOptimizationUtils {

    public static List<ImmutableStreamEdge> filterEdges(
            List<ImmutableStreamEdge> inEdges, int typeNumber) {
        return inEdges.stream()
                .filter(e -> e.getTypeNumber() == typeNumber)
                .collect(Collectors.toList());
    }

    public static boolean isBroadcastJoin(ImmutableStreamNode adaptiveJoinNode) {
        return adaptiveJoinNode.getInEdges().stream()
                .anyMatch(ImmutableStreamEdge::isBroadcastEdge);
    }

    public static boolean isBroadcastJoinDisabled(ReadableConfig config) {
        String value = config.get(TABLE_EXEC_DISABLED_OPERATORS);
        if (value == null) {
            return false;
        }
        String[] operators = value.split(",");
        for (String operator : operators) {
            operator = operator.trim();
            if (operator.isEmpty()) {
                continue;
            }
            if (operator.equals("HashJoin") || operator.equals("BroadcastHashJoin")) {
                return true;
            }
        }
        return false;
    }
}
