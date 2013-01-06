/***********************************************************************************************************************
 *
 * Copyright (C) 2012 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.pact.compiler.plan.candidate;

import java.util.Iterator;
import java.util.NoSuchElementException;

import eu.stratosphere.pact.common.plan.Visitor;
import eu.stratosphere.pact.common.util.FieldList;
import eu.stratosphere.pact.compiler.CompilerException;
import eu.stratosphere.pact.compiler.costs.Costs;
import eu.stratosphere.pact.compiler.dataproperties.GlobalProperties;
import eu.stratosphere.pact.compiler.dataproperties.LocalProperties;
import eu.stratosphere.pact.compiler.plan.OptimizerNode;
import eu.stratosphere.pact.compiler.plan.TwoInputNode;
import eu.stratosphere.pact.generic.types.TypeComparatorFactory;
import eu.stratosphere.pact.generic.types.TypePairComparatorFactory;
import eu.stratosphere.pact.runtime.shipping.ShipStrategyType;
import eu.stratosphere.pact.runtime.task.DriverStrategy;

/**
 *
 */
public class DualInputPlanNode extends PlanNode
{
	protected final Channel input1;
	protected final Channel input2;
	
	protected final FieldList keys1;
	protected final FieldList keys2;
	
	protected final boolean[] sortOrders;
	
	private TypeComparatorFactory<?> comparator1;
	private TypeComparatorFactory<?> comparator2;
	private TypePairComparatorFactory<?, ?> pairComparator;
	
	public Object postPassHelper1;
	public Object postPassHelper2;
	
	// --------------------------------------------------------------------------------------------

	public DualInputPlanNode(OptimizerNode template, Channel input1, Channel input2, DriverStrategy localStrategy)
	{
		this(template, input1, input2, localStrategy, null, null);
	}
	
	public DualInputPlanNode(OptimizerNode template, Channel input1, Channel input2,
			DriverStrategy localStrategy, FieldList driverKeyFields1, FieldList driverKeyFields2)
	{
		this(template, input1, input2, localStrategy, driverKeyFields1, driverKeyFields2, null);
	}
	
	public DualInputPlanNode(OptimizerNode template, Channel input1, Channel input2, 
			DriverStrategy localStrategy, FieldList driverKeyFields1, FieldList driverKeyFields2, 
			boolean[] driverSortOrders)
	{
		super(template, localStrategy);
		this.input1 = input1;
		this.input2 = input2;
		this.keys1 = driverKeyFields1;
		this.keys2 = driverKeyFields2;
		this.sortOrders = driverSortOrders;
		
		if (this.input1.getShipStrategy() == ShipStrategyType.BROADCAST) {
			this.input1.setReplicationFactor(getDegreeOfParallelism());
		}
		if (this.input2.getShipStrategy() == ShipStrategyType.BROADCAST) {
			this.input2.setReplicationFactor(getDegreeOfParallelism());
		}
		
		mergeBranchPlanMaps();
		computePropertiesAfterStrategies();
		
		// add the new unique information
		updatePropertiesWithUniqueSets(template.getUniqueFields());
	}
	
	private void computePropertiesAfterStrategies() {
		// adjust the global properties
		final GlobalProperties gp1 = this.input1.getGlobalProperties().clone().filterByNodesConstantSet(this.template, 0);
		final GlobalProperties gp2 = this.input2.getGlobalProperties().clone().filterByNodesConstantSet(this.template, 1);
		this.globalProps = GlobalProperties.combine(gp1, gp2);
		
		// adjust the local properties
		final LocalProperties lp1 = this.input1.getLocalProperties().clone().filterByNodesConstantSet(this.template, 0);
		final LocalProperties lp2 = this.input2.getLocalProperties().clone().filterByNodesConstantSet(this.template, 1);
		
		switch (getDriverStrategy()) {
		case HYBRIDHASH_BUILD_FIRST:
		case HYBRIDHASH_BUILD_SECOND:
		case NESTEDLOOP_BLOCKED_OUTER_FIRST:
		case NESTEDLOOP_BLOCKED_OUTER_SECOND:
			lp1.reset();
			this.localProps = lp1;
			break;
		case MERGE:
			this.localProps = lp1;
			break;
		case NESTEDLOOP_STREAMED_OUTER_FIRST:
			this.localProps = lp1;
			lp1.clearUniqueFieldSets();
			break;
		case NESTEDLOOP_STREAMED_OUTER_SECOND:
			this.localProps = lp2;
			lp2.clearUniqueFieldSets();
		default:
			throw new CompilerException("Unrecognized Driver Strategy for two-input plan node: " + getDriverStrategy());
		}
	}
	
	private void mergeBranchPlanMaps() {
//		// merge the branchPlan maps according the the template's uncloseBranchesStack
//		if (this.template.hasUnclosedBranches()) {
//			if (this.branchPlan == null) {
//				this.branchPlan = new HashMap<OptimizerNode, PlanNode>(8);
//			}
//			
//			final PlanNode pred1 = this.input1.getSource();
//			final PlanNode pred2 = this.input2.getSource();
//	
//			for (UnclosedBranchDescriptor uc : template.openBranches) {
//				OptimizerNode brancher = uc.branchingNode;
//				OptimizerNode selectedCandidate = null;
//	
//				if(pred1.branchPlan != null) {
//					// predecessor 1 has branching children, see if it got the branch we are looking for
//					selectedCandidate = pred1.branchPlan.get(brancher);
//					this.branchPlan.put(brancher, selectedCandidate);
//				}
//				
//				if (selectedCandidate == null && pred2.branchPlan != null) {
//					// predecessor 2 has branching children, see if it got the branch we are looking for
//					selectedCandidate = pred2.branchPlan.get(brancher);
//					this.branchPlan.put(brancher, selectedCandidate);
//				}
//	
//				if (selectedCandidate == null) {
//					throw new CompilerException(
//						"Candidates for a node with open branches are missing information about the selected candidate ");
//				}
//			}
//		}
	}

	// --------------------------------------------------------------------------------------------
	
	public TwoInputNode getTwoInputNode() {
		if (this.template instanceof TwoInputNode) {
			return (TwoInputNode) this.template;
		} else {
			throw new RuntimeException();
		}
	}
	
	public FieldList getKeysForInput1() {
		return this.keys1;
	}
	
	public FieldList getKeysForInput2() {
		return this.keys2;
	}
	
	public boolean[] getSortOrders() {
		return this.sortOrders;
	}
	
	public TypeComparatorFactory<?> getComparator1() {
		return this.comparator1;
	}
	
	public TypeComparatorFactory<?> getComparator2() {
		return this.comparator2;
	}
	
	public void setComparator1(TypeComparatorFactory<?> comparator) {
		this.comparator1 = comparator;
	}
	
	public void setComparator2(TypeComparatorFactory<?> comparator) {
		this.comparator2 = comparator;
	}
	
	public TypePairComparatorFactory<?, ?> getPairComparator() {
		return this.pairComparator;
	}
	
	public void setPairComparator(TypePairComparatorFactory<?, ?> comparator) {
		this.pairComparator = comparator;
	}
	
	/**
	 * Gets the first input channel to this node.
	 * 
	 * @return The first input channel to this node.
	 */
	public Channel getInput1() {
		return this.input1;
	}
	
	/**
	 * Gets the second input channel to this node.
	 * 
	 * @return The second input channel to this node.
	 */
	public Channel getInput2() {
		return this.input2;
	}
	
	/**
	 * This function overrides the standard behavior of computing costs in the {@link eu.stratosphere.pact.compiler.plan.candidate.PlanNode}.
	 * Since nodes with multiple inputs may join branched plans, care must be taken not to double-count the costs of the subtree rooted
	 * at the last unjoined branch.
	 * 
	 * @see eu.stratosphere.pact.compiler.plan.candidate.PlanNode#setCosts(eu.stratosphere.pact.compiler.costs.Costs)
	 */
	@Override
	public void setCosts(Costs nodeCosts) {
		super.setCosts(nodeCosts);
		
		// check, if this node has no branch beneath it, no double-counted cost then
		if (this.template.getLastJoinedBranchNode() == null) {
			return;
		}

		// get the cumulative costs of the last joined branching node
		PlanNode lastCommonChild = this.input1.getSource().branchPlan.get(this.template.getLastJoinedBranchNode());
		Costs douleCounted = lastCommonChild.getCumulativeCosts();
		getCumulativeCosts().subtractCosts(douleCounted);
		
	}
	
	// --------------------------------------------------------------------------------------------
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.plan.Visitable#accept(eu.stratosphere.pact.common.plan.Visitor)
	 */
	@Override
	public void accept(Visitor<PlanNode> visitor) {
		if (visitor.preVisit(this)) {
			this.input1.getSource().accept(visitor);
			this.input2.getSource().accept(visitor);
			visitor.postVisit(this);
		}
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.candidate.PlanNode#getPredecessors()
	 */
	@Override
	public Iterator<PlanNode> getPredecessors() {
		return new Iterator<PlanNode>() {
			private int hasLeft = 2;
			@Override
			public boolean hasNext() {
				return this.hasLeft > 0;
			}
			@Override
			public PlanNode next() {
				if (this.hasLeft == 2) {
					this.hasLeft = 1;
					return DualInputPlanNode.this.input1.getSource();
				} else if (this.hasLeft == 1) {
					this.hasLeft = 0;
					return DualInputPlanNode.this.input2.getSource();
				} else
					throw new NoSuchElementException();
			}
			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
		};
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.candidate.PlanNode#getInputs()
	 */
	@Override
	public Iterator<Channel> getInputs() {
		return new Iterator<Channel>() {
			private int hasLeft = 2;
			@Override
			public boolean hasNext() {
				return this.hasLeft > 0;
			}
			@Override
			public Channel next() {
				if (this.hasLeft == 2) {
					this.hasLeft = 1;
					return DualInputPlanNode.this.input1;
				} else if (this.hasLeft == 1) {
					this.hasLeft = 0;
					return DualInputPlanNode.this.input2;
				} else
					throw new NoSuchElementException();
			}
			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
		};
	}
}
