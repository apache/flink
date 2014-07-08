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
import java.util.Collections;
import java.util.List;

import eu.stratosphere.api.common.operators.util.FieldList;
import eu.stratosphere.api.common.typeutils.TypeComparatorFactory;
import eu.stratosphere.compiler.dag.OptimizerNode;
import eu.stratosphere.compiler.dag.SingleInputNode;
import eu.stratosphere.pact.runtime.shipping.ShipStrategyType;
import eu.stratosphere.pact.runtime.task.DamBehavior;
import eu.stratosphere.pact.runtime.task.DriverStrategy;
import eu.stratosphere.util.Visitor;

/**
 * 
 */
public class SingleInputPlanNode extends PlanNode {
	
	protected final Channel input;
	
	protected final FieldList keys;
	
	protected final boolean[] sortOrders;
	
	private TypeComparatorFactory<?> comparator;
	
	public Object postPassHelper;
	
	// --------------------------------------------------------------------------------------------

	public SingleInputPlanNode(OptimizerNode template, String nodeName, Channel input, DriverStrategy driverStrategy) {
		this(template, nodeName, input, driverStrategy, null, null);
	}
	
	public SingleInputPlanNode(OptimizerNode template, String nodeName, Channel input, 
			DriverStrategy driverStrategy, FieldList driverKeyFields)
	{
		this(template, nodeName, input, driverStrategy, driverKeyFields, getTrueArray(driverKeyFields.size()));
	}
	
	public SingleInputPlanNode(OptimizerNode template, String nodeName, Channel input, 
			DriverStrategy driverStrategy, FieldList driverKeyFields, boolean[] driverSortOrders)
	{
		super(template, nodeName, driverStrategy);
		this.input = input;
		this.keys = driverKeyFields;
		this.sortOrders = driverSortOrders;
		
		if (this.input.getShipStrategy() == ShipStrategyType.BROADCAST) {
			this.input.setReplicationFactor(getDegreeOfParallelism());
		}
		
		final PlanNode predNode = input.getSource();
		if (this.branchPlan == null) {
			this.branchPlan = predNode.branchPlan;
		} else if (predNode.branchPlan != null) {
			this.branchPlan.putAll(predNode.branchPlan);
		}
	}

	// --------------------------------------------------------------------------------------------
	
	public SingleInputNode getSingleInputNode() {
		if (this.template instanceof SingleInputNode) {
			return (SingleInputNode) this.template;
		} else {
			throw new RuntimeException();
		}
	}
	
	/**
	 * Gets the input channel to this node.
	 * 
	 * @return The input channel to this node.
	 */
	public Channel getInput() {
		return this.input;
	}
	
	/**
	 * Gets the predecessor of this node, i.e. the source of the input channel.
	 * 
	 * @return The predecessor of this node.
	 */
	public PlanNode getPredecessor() {
		return this.input.getSource();
	}
	
	public FieldList getKeys() {
		return this.keys;
	}
	
	public boolean[] getSortOrders() {
		return sortOrders;
	}
	
	/**
	 * Gets the comparator from this PlanNode.
	 *
	 * @return The comparator.
	 */
	public TypeComparatorFactory<?> getComparator() {
		return comparator;
	}
	
	/**
	 * Sets the comparator for this PlanNode.
	 *
	 * @param comparator The comparator to set.
	 */
	public void setComparator(TypeComparatorFactory<?> comparator) {
		this.comparator = comparator;
	}
	
	// --------------------------------------------------------------------------------------------
	

	@Override
	public void accept(Visitor<PlanNode> visitor) {
		if (visitor.preVisit(this)) {
			this.input.getSource().accept(visitor);
			
			for (Channel broadcastInput : getBroadcastInputs()) {
				broadcastInput.getSource().accept(visitor);
			}
			
			visitor.postVisit(this);
		}
	}


	@Override
	public Iterable<PlanNode> getPredecessors() {
		if (getBroadcastInputs() == null || getBroadcastInputs().isEmpty()) {
			return Collections.singleton(this.input.getSource());
		}
		else {
			List<PlanNode> preds = new ArrayList<PlanNode>();
			preds.add(input.getSource());
			
			for (Channel c : getBroadcastInputs()) {
				preds.add(c.getSource());
			}
			
			return preds;
		}
	}


	@Override
	public Iterable<Channel> getInputs() {
		return Collections.singleton(this.input);
	}

	@Override
	public SourceAndDamReport hasDamOnPathDownTo(PlanNode source) {
		if (source == this) {
			return FOUND_SOURCE;
		}
		SourceAndDamReport res = this.input.getSource().hasDamOnPathDownTo(source);
		if (res == FOUND_SOURCE_AND_DAM) {
			return FOUND_SOURCE_AND_DAM;
		}
		else if (res == FOUND_SOURCE) {
			return (this.input.getLocalStrategy().dams() || this.input.getTempMode().breaksPipeline() ||
					getDriverStrategy().firstDam() == DamBehavior.FULL_DAM) ?
				FOUND_SOURCE_AND_DAM : FOUND_SOURCE;
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
	
	// --------------------------------------------------------------------------------------------
	
	protected static boolean[] getTrueArray(int length) {
		final boolean[] a = new boolean[length];
		for (int i = 0; i < length; i++) {
			a[i] = true;
		}
		return a;
	}
}
