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
 * limitations under the License
 */

package org.apache.flink.runtime.executiongraph.failover.flip1;

import org.apache.flink.runtime.jobgraph.topology.LogicalPipelinedRegion;
import org.apache.flink.runtime.jobgraph.topology.LogicalResult;
import org.apache.flink.runtime.jobgraph.topology.LogicalVertex;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.runtime.executiongraph.failover.flip1.PipelinedRegionComputeUtil.buildRawRegions;
import static org.apache.flink.runtime.executiongraph.failover.flip1.PipelinedRegionComputeUtil.uniqueRegions;

/** Utils for computing {@link LogicalPipelinedRegion}s. */
public final class LogicalPipelinedRegionComputeUtil {

    public static Set<Set<LogicalVertex>> computePipelinedRegions(
            final Iterable<? extends LogicalVertex> topologicallySortedVertices) {

        final Map<LogicalVertex, Set<LogicalVertex>> vertexToRegion =
                buildRawRegions(
                        topologicallySortedVertices,
                        LogicalPipelinedRegionComputeUtil::getNonReconnectableConsumedResults);

        // Since LogicalTopology is a DAG, there is no need to do cycle detection nor to merge
        // regions on cycles.
        return uniqueRegions(vertexToRegion);
    }

    private static Iterable<LogicalResult> getNonReconnectableConsumedResults(
            LogicalVertex vertex) {
        List<LogicalResult> nonReconnectableConsumedResults = new ArrayList<>();
        for (LogicalResult consumedResult : vertex.getConsumedResults()) {
            if (!consumedResult.getResultType().isReconnectable()) {
                nonReconnectableConsumedResults.add(consumedResult);
            }
        }
        return nonReconnectableConsumedResults;
    }

    private LogicalPipelinedRegionComputeUtil() {}
}
