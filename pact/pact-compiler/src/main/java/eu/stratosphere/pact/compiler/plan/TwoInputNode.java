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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.contract.DualInputContract;
import eu.stratosphere.pact.common.plan.Visitor;
import eu.stratosphere.pact.compiler.CompilerException;
import eu.stratosphere.pact.compiler.Costs;
import eu.stratosphere.pact.compiler.GlobalProperties;
import eu.stratosphere.pact.compiler.LocalProperties;
import eu.stratosphere.pact.compiler.PactCompiler;
import eu.stratosphere.pact.runtime.task.util.OutputEmitter.ShipStrategy;

/**
 * A node in the optimizer plan that represents a PACT with a two different inputs, such as MATCH or CROSS.
 * The two inputs are not substitutable in their sides.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public abstract class TwoInputNode extends OptimizerNode
{
	final protected List<PactConnection> input1 = new ArrayList<PactConnection>(); // The first input edge

	final protected List<PactConnection> input2 = new ArrayList<PactConnection>(); // The second input edge

	private List<List<PactConnection>> inputs; // the cached list of inputs

	private OptimizerNode lastJoinedBranchNode; // the node with latest branch (node with multiple outputs)
	                                          // that both children share and that is at least partially joined

	/**
	 * Creates a new node with a single input for the optimizer plan.
	 * 
	 * @param pactContract
	 *        The PACT that the node represents.
	 */
	public TwoInputNode(DualInputContract<?, ?, ?, ?, ?, ?> pactContract) {
		super(pactContract);

		this.inputs = new ArrayList<List<PactConnection>>(2);
	}

	/**
	 * Copy constructor to create a copy of a node with different predecessors. The predecessors
	 * is assumed to be of the same type as in the template node and merely copies with different
	 * strategies, as they are created in the process of the plan enumeration.
	 * 
	 * @param template
	 *        The node to create a copy of.
	 * @param pred1
	 *        The new predecessor for the first input.
	 * @param pred2
	 *        The new predecessor for the second input.
	 * @param conn1
	 *        The old connection of the first input to copy properties from.
	 * @param conn2
	 *        The old connection of the second input to copy properties from.
	 * @param globalProps
	 *        The global properties of this copy.
	 * @param localProps
	 *        The local properties of this copy.
	 */
	protected TwoInputNode(TwoInputNode template, List<OptimizerNode> pred1, List<OptimizerNode> pred2, List<PactConnection> conn1,
			List<PactConnection> conn2, GlobalProperties globalProps, LocalProperties localProps) {
		super(template, globalProps, localProps);

		this.inputs = new ArrayList<List<PactConnection>>(2);
		
		int i = 0;
		for(PactConnection c : conn1) {
			PactConnection cc = new PactConnection(c, pred1.get(i++), this); 
			this.input1.add(cc);
		}
		this.inputs.add(this.input1);
		
		i = 0;
		for(PactConnection c : conn2) {
			PactConnection cc = new PactConnection(c, pred2.get(i++), this); 
			this.input2.add(cc);
		}
		this.inputs.add(this.input2);

		// remember the highest node in our sub-plan that branched.
		this.lastJoinedBranchNode = template.lastJoinedBranchNode;
		
		// merge the branchPlan maps according the the template's uncloseBranchesStack
		if (template.openBranches != null)
		{
			if (this.branchPlan == null) {
				this.branchPlan = new HashMap<OptimizerNode, OptimizerNode>(8);
			}

			Iterator<OptimizerNode> it1 = pred1.iterator();
			Iterator<OptimizerNode> it2 = pred2.iterator();
			
			for (UnclosedBranchDescriptor uc : template.openBranches) {
				OptimizerNode brancher = uc.branchingNode;
	
				// we take the candidate from pred1. if both have it, we could take it from either,
				// as they have to be the same
				OptimizerNode selectedCandidate = null;
				if (it1.hasNext()) {
					OptimizerNode n = it1.next();
					
					if(n.branchPlan != null) {
						// predecessor 1 has branching children, see if it got the branch we are looking for
						selectedCandidate = n.branchPlan.get(brancher);
					}
				}
	
				if (selectedCandidate == null && it2.hasNext()) {
					OptimizerNode n = it2.next();
					
					if(n.branchPlan != null) {
						// predecessor 2 has branching children, see if it got the branch we are looking for
						selectedCandidate = n.branchPlan.get(brancher);
					}
				}

				if (selectedCandidate == null) {
					throw new CompilerException(
						"Candidates for a node with open branches are missing information about the selected candidate ");
				}

				this.branchPlan.put(brancher, selectedCandidate);
			}
		}
	}

	// ------------------------------------------------------------------------

	/**
	 * Gets the <tt>PactConnection</tt> through which this node receives its <i>first</i> input.
	 * 
	 * @return The first input connection.
	 */
	public List<PactConnection> getFirstInputConnection() {
		return this.input1;
	}

	/**
	 * Gets the <tt>PactConnection</tt> through which this node receives its <i>second</i> input.
	 * 
	 * @return The second input connection.
	 */
	public List<PactConnection> getSecondInputConnection() {
		return this.input2;
	}

	/**
	 * Sets the <tt>PactConnection</tt> through which this node receives its <i>first</i> input.
	 * 
	 * @param conn
	 *        The first input connection.
	 */
	public void setFirstInputConnection(PactConnection conn) {
		this.input1.add(conn);
	}

	/**
	 * Sets the <tt>PactConnection</tt> through which this node receives its <i>second</i> input.
	 * 
	 * @param conn
	 *        The second input connection.
	 */
	public void setSecondInputConnection(PactConnection conn) {
		this.input2.add(conn);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#getIncomingConnections()
	 */
	@Override
	public List<List<PactConnection>> getIncomingConnections() {
		return this.inputs;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#setInputs(java.util.Map)
	 */
	@Override
	public void setInputs(Map<Contract, OptimizerNode> contractToNode) {
		// get the predecessors
		DualInputContract<?, ?, ?, ?, ?, ?> contr = (DualInputContract<?, ?, ?, ?, ?, ?>) getPactContract();
		
		List<Contract> leftPreds = contr.getFirstInputs();
		List<Contract> rightPreds = contr.getSecondInputs();
		
		for(Contract cl : leftPreds) {
			OptimizerNode pred1 = contractToNode.get(cl);
			// create the connections and add them
			PactConnection conn1 = new PactConnection(pred1, this);
			this.input1.add(conn1);
			pred1.addOutgoingConnection(conn1);
		}

		for(Contract cr : rightPreds) {
			OptimizerNode pred2 = contractToNode.get(cr);
			// create the connections and add them
			PactConnection conn2 = new PactConnection(pred2, this);
			this.input2.add(conn2);
			pred2.addOutgoingConnection(conn2);
		}

		// see if there is a hint that dictates which shipping strategy to use for BOTH inputs
		Configuration conf = getPactContract().getParameters();
		String shipStrategy = conf.getString(PactCompiler.HINT_SHIP_STRATEGY, null);
		if (shipStrategy != null) {
			if (PactCompiler.HINT_SHIP_STRATEGY_FORWARD.equals(shipStrategy)) {
				for(PactConnection c : this.input1) {
					c.setShipStrategy(ShipStrategy.FORWARD);
				}
				for(PactConnection c : this.input2) {
					c.setShipStrategy(ShipStrategy.FORWARD);
				}
			} else if (PactCompiler.HINT_SHIP_STRATEGY_BROADCAST.equals(shipStrategy)) {
				for(PactConnection c : this.input1) {
					c.setShipStrategy(ShipStrategy.BROADCAST);
				}
				for(PactConnection c : this.input2) {
					c.setShipStrategy(ShipStrategy.BROADCAST);
				}
			} else if (PactCompiler.HINT_SHIP_STRATEGY_REPARTITION.equals(shipStrategy)) {
				for(PactConnection c : this.input1) {
					c.setShipStrategy(ShipStrategy.PARTITION_HASH);
				}
				for(PactConnection c : this.input2) {
					c.setShipStrategy(ShipStrategy.PARTITION_HASH);
				}
			} else {
				throw new CompilerException("Unknown hint for shipping strategy: " + shipStrategy);
			}
		}

		// see if there is a hint that dictates which shipping strategy to use for the FIRST input
		shipStrategy = conf.getString(PactCompiler.HINT_SHIP_STRATEGY_FIRST_INPUT, null);
		if (shipStrategy != null) {
			if (PactCompiler.HINT_SHIP_STRATEGY_FORWARD.equals(shipStrategy)) {
				for(PactConnection c : this.input1) {
					c.setShipStrategy(ShipStrategy.FORWARD);
				}
			} else if (PactCompiler.HINT_SHIP_STRATEGY_BROADCAST.equals(shipStrategy)) {
				for(PactConnection c : this.input1) {
					c.setShipStrategy(ShipStrategy.BROADCAST);
				}
			} else if (PactCompiler.HINT_SHIP_STRATEGY_REPARTITION.equals(shipStrategy)) {
				for(PactConnection c : this.input1) {
					c.setShipStrategy(ShipStrategy.PARTITION_HASH);
				}
			} else {
				throw new CompilerException("Unknown hint for shipping strategy of input one: " + shipStrategy);
			}
		}

		// see if there is a hint that dictates which shipping strategy to use for the SECOND input
		shipStrategy = conf.getString(PactCompiler.HINT_SHIP_STRATEGY_SECOND_INPUT, null);
		if (shipStrategy != null) {
			if (PactCompiler.HINT_SHIP_STRATEGY_FORWARD.equals(shipStrategy)) {
				for(PactConnection c : this.input2) {
					c.setShipStrategy(ShipStrategy.FORWARD);
				}
			} else if (PactCompiler.HINT_SHIP_STRATEGY_BROADCAST.equals(shipStrategy)) {
				for(PactConnection c : this.input2) {
					c.setShipStrategy(ShipStrategy.BROADCAST);
				}
			} else if (PactCompiler.HINT_SHIP_STRATEGY_REPARTITION.equals(shipStrategy)) {
				for(PactConnection c : this.input2) {
					c.setShipStrategy(ShipStrategy.PARTITION_HASH);
				}
			} else {
				throw new CompilerException("Unknown hint for shipping strategy of input two: " + shipStrategy);
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#computeUnclosedBranchStack()
	 */
	@Override
	public void computeUnclosedBranchStack() {
		if (this.openBranches != null) {
			return;
		}

		List<UnclosedBranchDescriptor> l;

		List<UnclosedBranchDescriptor> child1open = new ArrayList<UnclosedBranchDescriptor>();
		for(PactConnection c : this.input1) {
			l = c.getSourcePact().getBranchesForParent(this);
			if(l != null)
				child1open.addAll(l);
		}
		List<UnclosedBranchDescriptor> child2open = new ArrayList<UnclosedBranchDescriptor>();
		for(PactConnection c : this.input2) {
			l = c.getSourcePact().getBranchesForParent(this);
			if(l != null)
				child2open.addAll(l);
		}

		// check how many open branches we have. the cases:
		// 1) if both are null or empty, the result is null
		// 2) if one side is null (or empty), the result is the other side.
		// 3) both are set, then we need to merge.
		if (child1open.isEmpty()) {
			this.openBranches = child2open;
		} else if (child2open.isEmpty()) {
			this.openBranches = child1open;
		} else {
			// both have a history. merge...
			this.openBranches = new ArrayList<OptimizerNode.UnclosedBranchDescriptor>(4);

			int index1 = child1open.size() - 1;
			int index2 = child2open.size() - 1;

			while (index1 >= 0 || index2 >= 0) {
				int id1 = -1;
				int id2 = index2 >= 0 ? child2open.get(index2).getBranchingNode().getId() : -1;

				while (index1 >= 0 && (id1 = child1open.get(index1).getBranchingNode().getId()) > id2) {
					this.openBranches.add(child1open.get(index1));
					index1--;
				}
				while (index2 >= 0 && (id2 = child2open.get(index2).getBranchingNode().getId()) > id1) {
					this.openBranches.add(child2open.get(index2));
					index2--;
				}

				// match: they share a common branching child
				if (id1 == id2) {
					// if this is the latest common child, remember it
					OptimizerNode currBanchingNode = child1open.get(index1).getBranchingNode();

					if (this.lastJoinedBranchNode == null) {
						this.lastJoinedBranchNode = currBanchingNode;
					}

					// see, if this node closes the branch
					long joinedInputs = child1open.get(index1).getJoinedPathsVector()
						| child2open.get(index2).getJoinedPathsVector();

					// this is 2^size - 1, which is all bits set at positions 0..size-1
					long allInputs = (0x1L << currBanchingNode.getOutgoingConnections().size()) - 1;

					if (joinedInputs == allInputs) {
						// closed - we can remove it from the stack
					} else {
						// not quite closed
						this.openBranches.add(new UnclosedBranchDescriptor(currBanchingNode, joinedInputs));
					}

					index1--;
					index2--;
				}

			}

			if (this.openBranches.isEmpty()) {
				this.openBranches = null;
			} else {
				// merged. now we need to reverse the list, because we added the elements in reverse order
				Collections.reverse(this.openBranches);
			}
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
			if (this.input1 != null) {
				for(PactConnection c : this.input1) {
					if(c.getSourcePact() != null) {
						c.getSourcePact().accept(visitor);
					}
				}
			}
			if (this.input2 != null) {
				for(PactConnection c : this.input2) {
					if(c.getSourcePact() != null) {
						c.getSourcePact().accept(visitor);
					}
				}
			}

			visitor.postVisit(this);
		}
	}

	// ------------------------------------------------------------------------
	//                       Handling of branches
	// ------------------------------------------------------------------------

	/**
	 * Checks whether to candidate plans for the sub-plan of this node are comparable. The two
	 * alternative plans are comparable, if
	 * a) There is no branch in the sub-plan of this node
	 * b) Both candidates have the same candidate as the child at the last open branch. 
	 * 
	 * @param child1Candidate
	 * @param child2Candidate
	 * @return
	 */
	protected boolean areBranchCompatible(OptimizerNode child1Candidate, OptimizerNode child2Candidate)
	{
		// if there is no open branch, the children are always compatible.
		// in most plans, that will be the dominant case
		if (this.lastJoinedBranchNode == null) {
			return true;
		}
		// else
		return child1Candidate.branchPlan.get(this.lastJoinedBranchNode) == 
				child2Candidate.branchPlan.get(this.lastJoinedBranchNode);
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
//		// check, if this node has no branch beneath it, no double-counted cost then
//		if (this.lastJoinedBranchNode == null) {
//			return;
//		}
//		
//		
//		// get the children and check their existence
//		OptimizerNode child1 = (this.input1 == null ? null : this.input1.getSourcePact());
//		OptimizerNode child2 = (this.input2 == null ? null : this.input2.getSourcePact());
//		
//		if (child1 == null || child2 == null) {
//			return;
//		}
//		
//		// get the cumulative costs of the last joined branching node
//		OptimizerNode lastCommonChild = child1.branchPlan.get(this.lastJoinedBranchNode);
//		Costs douleCounted = lastCommonChild.getCumulativeCosts();
//		getCumulativeCosts().subtractCosts(douleCounted);
	}
}
