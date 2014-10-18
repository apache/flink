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


package org.apache.flink.compiler.dag;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.flink.api.common.typeinfo.NothingTypeInfo;
import org.apache.flink.compiler.CompilerException;
import org.apache.flink.compiler.DataStatistics;
import org.apache.flink.compiler.operators.OperatorDescriptorDual;
import org.apache.flink.compiler.operators.UtilSinkJoinOpDescriptor;
import org.apache.flink.compiler.util.NoOpBinaryUdfOp;
import org.apache.flink.types.Nothing;

/**
 * This class represents a utility node that is not part of the actual plan. It is used for plans with multiple data sinks to
 * transform it into a plan with a single root node. That way, the code that makes sure no costs are double-counted and that 
 * candidate selection works correctly with nodes that have multiple outputs is transparently reused.
 */
public class SinkJoiner extends TwoInputNode {
	
	public SinkJoiner(OptimizerNode input1, OptimizerNode input2) {
		super(new NoOpBinaryUdfOp<Nothing>(new NothingTypeInfo()));

		PactConnection conn1 = new PactConnection(input1, this);
		PactConnection conn2 = new PactConnection(input2, this);
		
		this.input1 = conn1;
		this.input2 = conn2;
		
		setDegreeOfParallelism(1);
	}
	
	@Override
	public String getName() {
		return "Internal Utility Node";
	}
	
	@Override
	public List<PactConnection> getOutgoingConnections() {
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
		
		// if the predecessors do not have branches, then we have multiple sinks that do not originate from
		// a common data flow.
		if (pred1branches == null || pred1branches.isEmpty() || pred2branches == null || pred2branches.isEmpty()) {
			throw new CompilerException("The given program contains multiple disconnected data flows.");
		}
		
		// copy the lists and merge
		List<UnclosedBranchDescriptor> result1 = new ArrayList<UnclosedBranchDescriptor>(pred1branches);
		List<UnclosedBranchDescriptor> result2 = new ArrayList<UnclosedBranchDescriptor>(pred2branches);
		
		ArrayList<UnclosedBranchDescriptor> result = new ArrayList<UnclosedBranchDescriptor>();
		mergeLists(result1, result2, result);
		
//		if (!didCloseSomeBranch) {
//			// if the sink joiners do not close branches, then we have disjoint data flows.
//			throw new CompilerException("The given program contains multiple disconnected data flows.");
//		}
		
		this.openBranches = result.isEmpty() ? Collections.<UnclosedBranchDescriptor>emptyList() : result;
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
