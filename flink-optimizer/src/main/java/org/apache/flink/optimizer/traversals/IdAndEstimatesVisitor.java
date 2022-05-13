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

package org.apache.flink.optimizer.traversals;

import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.dag.DagConnection;
import org.apache.flink.optimizer.dag.IterationNode;
import org.apache.flink.optimizer.dag.OptimizerNode;
import org.apache.flink.util.Visitor;

/**
 * This traversal of the optimizer DAG assigns IDs to each node (in a pre-order fashion), and calls
 * each node to compute its estimates. The latter happens in the postVisit function, where it is
 * guaranteed that all predecessors have computed their estimates.
 */
public class IdAndEstimatesVisitor implements Visitor<OptimizerNode> {

    private final DataStatistics statistics;

    private int id = 1;

    public IdAndEstimatesVisitor(DataStatistics statistics) {
        this.statistics = statistics;
    }

    @Override
    public boolean preVisit(OptimizerNode visitable) {
        return visitable.getId() == -1;
    }

    @Override
    public void postVisit(OptimizerNode visitable) {
        // the node ids
        visitable.initId(this.id++);

        // connections need to figure out their maximum path depths
        for (DagConnection conn : visitable.getIncomingConnections()) {
            conn.initMaxDepth();
        }
        for (DagConnection conn : visitable.getBroadcastConnections()) {
            conn.initMaxDepth();
        }

        // the estimates
        visitable.computeOutputEstimates(this.statistics);

        // if required, recurse into the step function
        if (visitable instanceof IterationNode) {
            ((IterationNode) visitable).acceptForStepFunction(this);
        }
    }
}
