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


package org.apache.flink.optimizer.plan;

import static org.apache.flink.optimizer.plan.PlanNode.SourceAndDamReport.FOUND_SOURCE;
import static org.apache.flink.optimizer.plan.PlanNode.SourceAndDamReport.FOUND_SOURCE_AND_DAM;
import static org.apache.flink.optimizer.plan.PlanNode.SourceAndDamReport.NOT_FOUND;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.apache.flink.api.common.operators.util.FieldList;
import org.apache.flink.api.common.typeutils.TypeComparatorFactory;
import org.apache.flink.optimizer.CompilerException;
import org.apache.flink.optimizer.dag.OptimizerNode;
import org.apache.flink.optimizer.dag.SingleInputNode;
import org.apache.flink.runtime.operators.DamBehavior;
import org.apache.flink.runtime.operators.DriverStrategy;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
import org.apache.flink.util.Visitor;

/**
 * 
 */
public class SingleInputPlanNode extends PlanNode {
	
	protected final Channel input;
	
	protected final FieldList[] driverKeys;
	
	protected final boolean[][] driverSortOrders;
	
	private TypeComparatorFactory<?>[] comparators;
	
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
		
		this.comparators = new TypeComparatorFactory<?>[driverStrategy.getNumRequiredComparators()];
		this.driverKeys = new FieldList[driverStrategy.getNumRequiredComparators()];
		this.driverSortOrders = new boolean[driverStrategy.getNumRequiredComparators()][];
		
		if(driverStrategy.getNumRequiredComparators() > 0) {
			this.driverKeys[0] = driverKeyFields;
			this.driverSortOrders[0] = driverSortOrders;
		}
		
		if (this.input.getShipStrategy() == ShipStrategyType.BROADCAST) {
			this.input.setReplicationFactor(getParallelism());
		}
		
		final PlanNode predNode = input.getSource();
		
		if (predNode.branchPlan != null && !predNode.branchPlan.isEmpty()) {
			
			if (this.branchPlan == null) {
				this.branchPlan = new HashMap<OptimizerNode, PlanNode>();
			}
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
	
	/**
	 * Sets the key field indexes for the specified driver comparator.
	 * 
	 * @param keys The key field indexes for the specified driver comparator.
	 * @param id The ID of the driver comparator.
	 */
	public void setDriverKeyInfo(FieldList keys, int id) {
		this.setDriverKeyInfo(keys, getTrueArray(keys.size()), id);
	}
	
	/**
	 * Sets the key field information for the specified driver comparator.
	 * 
	 * @param keys The key field indexes for the specified driver comparator.
	 * @param sortOrder The key sort order for the specified driver comparator.
	 * @param id The ID of the driver comparator.
	 */
	public void setDriverKeyInfo(FieldList keys, boolean[] sortOrder, int id) {
		if(id < 0 || id >= driverKeys.length) {
			throw new CompilerException("Invalid id for driver key information. DriverStrategy requires only "
											+super.getDriverStrategy().getNumRequiredComparators()+" comparators.");
		}
		this.driverKeys[id] = keys;
		this.driverSortOrders[id] = sortOrder;
	}
	
	/**
	 * Gets the key field indexes for the specified driver comparator.
	 * 
	 * @param id The id of the driver comparator for which the key field indexes are requested.
	 * @return The key field indexes of the specified driver comparator.
	 */
	public FieldList getKeys(int id) {
		return this.driverKeys[id];
	}
	
	/**
	 * Gets the sort order for the specified driver comparator.
	 * 
	 * @param id The id of the driver comparator for which the sort order is requested.
	 * @return The sort order of the specified driver comparator.
	 */
	public boolean[] getSortOrders(int id) {
		return driverSortOrders[id];
	}
	
	/**
	 * Gets the specified comparator from this PlanNode.
	 * 
	 * @param id The ID of the requested comparator.
	 *
	 * @return The specified comparator.
	 */
	public TypeComparatorFactory<?> getComparator(int id) {
		return comparators[id];
	}
	
	/**
	 * Sets the specified comparator for this PlanNode.
	 *
	 * @param comparator The comparator to set.
	 * @param id The ID of the comparator to set.
	 */
	public void setComparator(TypeComparatorFactory<?> comparator, int id) {
		this.comparators[id] = comparator;
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
