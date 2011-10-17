/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.compiler.plan;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.contract.SingleInputContract;
import eu.stratosphere.pact.common.plan.Visitor;
import eu.stratosphere.pact.compiler.CompilerException;
import eu.stratosphere.pact.compiler.Costs;
import eu.stratosphere.pact.compiler.GlobalProperties;
import eu.stratosphere.pact.compiler.LocalProperties;
import eu.stratosphere.pact.compiler.PactCompiler;
import eu.stratosphere.pact.runtime.task.util.OutputEmitter.ShipStrategy;

/**
 * A node in the optimizer plan that represents a PACT with a single input.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public abstract class SingleInputNode extends OptimizerNode {

	final protected List<PactConnection> input = new ArrayList<PactConnection>(); // The list of input edges

	/**
	 * Creates a new node with a single input for the optimizer plan.
	 * 
	 * @param pactContract
	 *        The PACT that the node represents.
	 */
	public SingleInputNode(SingleInputContract<?, ?, ?, ?> pactContract) {
		super(pactContract);
	}

	/**
	 * Copy constructor to create a copy of a node with a different predecessor. The predecessor
	 * is assumed to be of the same type and merely a copy with different strategies, as they
	 * are created in the process of the plan enumeration.
	 * 
	 * @param template
	 *        The node to create a copy of.
	 * @param pred
	 *        The new predecessor.
	 * @param conn
	 *        The old connection to copy properties from.
	 * @param globalProps
	 *        The global properties of this copy.
	 * @param localProps
	 *        The local properties of this copy.
	 */
	protected SingleInputNode(OptimizerNode template, List<OptimizerNode> pred, List<PactConnection> conn,
			GlobalProperties globalProps, LocalProperties localProps) {
		super(template, globalProps, localProps);

		int i = 0;
		for(PactConnection c: conn) {
			this.input.add(new PactConnection(c, pred.get(i++), this));
		}

		// copy the child's branch-plan map
		if (this.branchPlan == null) {
			this.branchPlan = new HashMap<OptimizerNode, OptimizerNode>();
		}
		for(OptimizerNode n : pred) {
			if(n.branchPlan != null)
				this.branchPlan.putAll(n.branchPlan);
		}
		if(this.branchPlan.size() == 0)
			this.branchPlan = null;
	}

	/**
	 * Gets the <tt>PactConnection</tt> through which this node receives its input.
	 * 
	 * @return The input connection.
	 */
	public List<PactConnection> getInputConnections() {
		return this.input;
	}

	/**
	 * Sets the <tt>PactConnection</tt> through which this node receives its input.
	 * 
	 * @param conn
	 *        The input connection to set.
	 */
	public void addInputConnection(PactConnection conn) {
		this.input.add(conn);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#getIncomingConnections()
	 */
	@Override
	public List<List<PactConnection>> getIncomingConnections() {
		return Collections.singletonList(this.input);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#setInputs(java.util.Map)
	 */
	@Override
	public void setInputs(Map<Contract, OptimizerNode> contractToNode) throws CompilerException {
		// get the predecessor node
		List<Contract> children = ((SingleInputContract<?, ?, ?, ?>) getPactContract()).getInputs();
		
		for(Contract child : children) {
			OptimizerNode pred = contractToNode.get(child);
	
			// create a connection
			PactConnection conn = new PactConnection(pred, this);
			addInputConnection(conn);
			pred.addOutgoingConnection(conn);
	
			// see if an internal hint dictates the strategy to use
			Configuration conf = getPactContract().getParameters();
			String shipStrategy = conf.getString(PactCompiler.HINT_SHIP_STRATEGY, null);
			if (shipStrategy != null) {
				if (PactCompiler.HINT_SHIP_STRATEGY_FORWARD.equals(shipStrategy)) {
					conn.setShipStrategy(ShipStrategy.FORWARD);
				} else if (PactCompiler.HINT_SHIP_STRATEGY_REPARTITION.equals(shipStrategy)) {
					conn.setShipStrategy(ShipStrategy.PARTITION_HASH);
				} else {
					throw new CompilerException("Invalid hint for the shipping strategy of a single input connection: "
						+ shipStrategy);
				}
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#computeUnclosedBranchStack()
	 */
	@Override
	public void computeUnclosedBranchStack() {
		if (this.openBranches == null) {
			// we don't join branches, so we merely have to check, whether out immediate child is a
			// branch (has multiple outputs). If yes, we add that one as a branch, otherwise out
			// branch stack is the same as the child's
			this.openBranches = new ArrayList<UnclosedBranchDescriptor>();
			for(PactConnection c : this.input) {
				List<UnclosedBranchDescriptor> parentBranchList = c.getSourcePact().getBranchesForParent(this);
				if(parentBranchList != null)
					this.openBranches.addAll(parentBranchList);
			}
			if(this.openBranches.size() == 0)
				this.openBranches = null;
		}
	}

	// ------------------------------------------------------------------------

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.pact.compiler.plan.OptimizerNode#accept(eu.stratosphere.pact.common.plan.Visitor
	 * )
	 */
	@Override
	public void accept(Visitor<OptimizerNode> visitor) {
		boolean descend = visitor.preVisit(this);

		if (descend) {
			for(PactConnection c : this.input) {
				OptimizerNode n = c.getSourcePact();
				if (n != null) {
					n.accept(visitor);
				}
			}

			visitor.postVisit(this);
		}
	}
	
	/**
	 * This function overrides the standard behavior of computing costs in the {@link eu.stratosphere.pact.compiler.plan.OptimizerNode}.
	 * Since nodes with multiple inputs may join branched plans, care must be taken not to double-count the costs of the subtree rooted
	 * at the last unjoined branch.
	 * 
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#setCosts(eu.stratosphere.pact.compiler.Costs)
	 */
	@Override
	public void setCosts(Costs nodeCosts) {
		super.setCosts(nodeCosts);
		
		// TODO: mjsax
		// see TwoInputNode.java
	}
}
