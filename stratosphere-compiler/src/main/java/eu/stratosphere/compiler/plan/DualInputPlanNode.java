/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.compiler.plan;

import static eu.stratosphere.compiler.plan.PlanNode.SourceAndDamReport.FOUND_SOURCE;
import static eu.stratosphere.compiler.plan.PlanNode.SourceAndDamReport.FOUND_SOURCE_AND_DAM;
import static eu.stratosphere.compiler.plan.PlanNode.SourceAndDamReport.NOT_FOUND;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import eu.stratosphere.api.common.operators.util.FieldList;
import eu.stratosphere.api.common.typeutils.TypeComparatorFactory;
import eu.stratosphere.api.common.typeutils.TypePairComparatorFactory;
import eu.stratosphere.compiler.dag.OptimizerNode;
import eu.stratosphere.compiler.dag.TwoInputNode;
import eu.stratosphere.pact.runtime.shipping.ShipStrategyType;
import eu.stratosphere.pact.runtime.task.DamBehavior;
import eu.stratosphere.pact.runtime.task.DriverStrategy;
import eu.stratosphere.util.Visitor;

/**
 *
 */
public class DualInputPlanNode extends PlanNode {
	
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

	public DualInputPlanNode(OptimizerNode template, String nodeName, Channel input1, Channel input2, DriverStrategy diverStrategy) {
		this(template, nodeName, input1, input2, diverStrategy, null, null, null);
	}
	
	public DualInputPlanNode(OptimizerNode template, String nodeName, Channel input1, Channel input2,
			DriverStrategy diverStrategy, FieldList driverKeyFields1, FieldList driverKeyFields2)
	{
		this(template, nodeName, input1, input2, diverStrategy, driverKeyFields1, driverKeyFields2,
									SingleInputPlanNode.getTrueArray(driverKeyFields1.size()));
	}
	
	public DualInputPlanNode(OptimizerNode template, String nodeName, Channel input1, Channel input2, DriverStrategy diverStrategy,
			FieldList driverKeyFields1, FieldList driverKeyFields2, boolean[] driverSortOrders)
	{
		super(template, nodeName, diverStrategy);
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
		
		mergeBranchPlanMaps(input1.getSource(), input2.getSource());
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
	
	// --------------------------------------------------------------------------------------------
	

	@Override
	public void accept(Visitor<PlanNode> visitor) {
		if (visitor.preVisit(this)) {
			this.input1.getSource().accept(visitor);
			this.input2.getSource().accept(visitor);
			
			for (Channel broadcastInput : getBroadcastInputs()) {
				broadcastInput.getSource().accept(visitor);
			}
			
			visitor.postVisit(this);
		}
	}
	

	@Override
	public Iterator<PlanNode> getPredecessors() {
		if (getBroadcastInputs() == null || getBroadcastInputs().isEmpty()) {
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
					} else {
						throw new NoSuchElementException();
					}
				}
				@Override
				public void remove() {
					throw new UnsupportedOperationException();
				}
			};
		} else {
			List<PlanNode> preds = new ArrayList<PlanNode>();
			
			preds.add(input1.getSource());
			preds.add(input2.getSource());

			for (Channel c : getBroadcastInputs()) {
				preds.add(c.getSource());
			}
			
			return preds.iterator();
		}
	}


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
				} else {
					throw new NoSuchElementException();
				}
			}
			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
		};
	}


	@Override
	public SourceAndDamReport hasDamOnPathDownTo(PlanNode source) {
		if (source == this) {
			return FOUND_SOURCE;
		}
		
		// check first input
		SourceAndDamReport res1 = this.input1.getSource().hasDamOnPathDownTo(source);
		if (res1 == FOUND_SOURCE_AND_DAM) {
			return FOUND_SOURCE_AND_DAM;
		}
		else if (res1 == FOUND_SOURCE) {
			if (this.input1.getLocalStrategy().dams() || this.input1.getTempMode().breaksPipeline() ||
					getDriverStrategy().firstDam() == DamBehavior.FULL_DAM) {
				return FOUND_SOURCE_AND_DAM;
			} else {
				return FOUND_SOURCE;
			}
		}
		else {
			SourceAndDamReport res2 = this.input2.getSource().hasDamOnPathDownTo(source);
			if (res2 == FOUND_SOURCE_AND_DAM) {
				return FOUND_SOURCE_AND_DAM;
			}
			else if (res2 == FOUND_SOURCE) {
				if (this.input2.getLocalStrategy().dams() || this.input2.getTempMode().breaksPipeline() ||
						getDriverStrategy().secondDam() == DamBehavior.FULL_DAM) {
					return FOUND_SOURCE_AND_DAM;
				} else {
					return FOUND_SOURCE;
				}
			}
			else {
				// NOT_FOUND
				// check the broadcast inputs
				
				for (NamedChannel nc : getBroadcastInputs()) {
					SourceAndDamReport bcRes = nc.getSource().hasDamOnPathDownTo(source);
					if (bcRes != NOT_FOUND) {
						// broadcast inputs are always dams
						return FOUND_SOURCE_AND_DAM;
					}
				}
				return NOT_FOUND;
			}
		}
	}
}
