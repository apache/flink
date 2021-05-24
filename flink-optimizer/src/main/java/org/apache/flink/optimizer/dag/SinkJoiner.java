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

package org.apache.flink.optimizer.dag;

import org.apache.flink.api.common.ExecutionMode;
import org.apache.flink.api.common.typeinfo.NothingTypeInfo;
import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.operators.OperatorDescriptorDual;
import org.apache.flink.optimizer.operators.UtilSinkJoinOpDescriptor;
import org.apache.flink.optimizer.util.NoOpBinaryUdfOp;
import org.apache.flink.types.Nothing;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * This class represents a utility node that is not part of the actual plan. It is used for plans
 * with multiple data sinks to transform it into a plan with a single root node. That way, the code
 * that makes sure no costs are double-counted and that candidate selection works correctly with
 * nodes that have multiple outputs is transparently reused.
 */
public class SinkJoiner extends TwoInputNode {

    public SinkJoiner(OptimizerNode input1, OptimizerNode input2) {
        super(new NoOpBinaryUdfOp<Nothing>(new NothingTypeInfo()));

        DagConnection conn1 = new DagConnection(input1, this, null, ExecutionMode.PIPELINED);
        DagConnection conn2 = new DagConnection(input2, this, null, ExecutionMode.PIPELINED);

        this.input1 = conn1;
        this.input2 = conn2;

        setParallelism(1);
    }

    @Override
    public String getOperatorName() {
        return "Internal Utility Node";
    }

    @Override
    public List<DagConnection> getOutgoingConnections() {
        return Collections.emptyList();
    }

    @Override
    public void computeUnclosedBranchStack() {
        if (this.openBranches != null) {
            return;
        }

        addClosedBranches(getFirstPredecessorNode().closedBranchingNodes);
        addClosedBranches(getSecondPredecessorNode().closedBranchingNodes);

        List<UnclosedBranchDescriptor> pred1branches = getFirstPredecessorNode().openBranches;
        List<UnclosedBranchDescriptor> pred2branches = getSecondPredecessorNode().openBranches;

        // if the predecessors do not have branches, then we have multiple sinks that do not
        // originate from
        // a common data flow.
        if (pred1branches == null || pred1branches.isEmpty()) {

            this.openBranches =
                    (pred2branches == null || pred2branches.isEmpty())
                            ? Collections.<UnclosedBranchDescriptor>emptyList()
                            : // both empty - disconnected flow
                            pred2branches;
        } else if (pred2branches == null || pred2branches.isEmpty()) {
            this.openBranches = pred1branches;
        } else {
            // copy the lists and merge
            List<UnclosedBranchDescriptor> result1 =
                    new ArrayList<UnclosedBranchDescriptor>(pred1branches);
            List<UnclosedBranchDescriptor> result2 =
                    new ArrayList<UnclosedBranchDescriptor>(pred2branches);

            ArrayList<UnclosedBranchDescriptor> result = new ArrayList<UnclosedBranchDescriptor>();
            mergeLists(result1, result2, result, false);

            this.openBranches =
                    result.isEmpty() ? Collections.<UnclosedBranchDescriptor>emptyList() : result;
        }
    }

    @Override
    protected List<OperatorDescriptorDual> getPossibleProperties() {
        return Collections.<OperatorDescriptorDual>singletonList(new UtilSinkJoinOpDescriptor());
    }

    @Override
    public void computeOutputEstimates(DataStatistics statistics) {
        // nothing to be done here
    }

    @Override
    protected void computeOperatorSpecificDefaultEstimates(DataStatistics statistics) {
        // no estimates needed at this point
    }
}
