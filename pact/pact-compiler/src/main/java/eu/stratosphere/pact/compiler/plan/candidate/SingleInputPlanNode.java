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

import eu.stratosphere.pact.common.generic.types.TypeComparatorFactory;
import eu.stratosphere.pact.common.generic.types.TypeSerializerFactory;
import eu.stratosphere.pact.common.plan.Visitor;
import eu.stratosphere.pact.common.util.FieldList;
import eu.stratosphere.pact.compiler.CompilerException;
import eu.stratosphere.pact.compiler.Costs;
import eu.stratosphere.pact.compiler.plan.OptimizerNode;
import eu.stratosphere.pact.compiler.plan.SingleInputNode;
import eu.stratosphere.pact.runtime.shipping.ShipStrategyType;
import eu.stratosphere.pact.runtime.task.DriverStrategy;


/**
 * @author Stephan Ewen
 */
public class SingleInputPlanNode extends PlanNode
{
	protected final Channel input;
	
	protected final FieldList keys;
	
	protected final boolean[] sortOrders;
	
	private TypeSerializerFactory<?> serializer;
	
	private TypeComparatorFactory<?> comparator;
	
	public Object postPassHelper;
	
	// --------------------------------------------------------------------------------------------

//	public SingleInputPlanNode(OptimizerNode template, Channel input, DriverStrategy localStrategy)
//	{
//		this(template, input, localStrategy, null);
//	}
//	
//	public SingleInputPlanNode(OptimizerNode template, Channel input, 
//			DriverStrategy localStrategy, FieldList driverKeyFields)
//	{
//		this(template, input, localStrategy, driverKeyFields, null);
//	}
	
	public SingleInputPlanNode(OptimizerNode template, Channel input, 
			DriverStrategy localStrategy, FieldList driverKeyFields, boolean[] driverSortOrders)
	{
		super(template, localStrategy);
		this.input = input;
		this.keys = driverKeyFields;
		this.sortOrders = driverSortOrders;
		
		if (this.input.getShipStrategy() == ShipStrategyType.BROADCAST) {
			this.input.setReplicationFactor(getDegreeOfParallelism());
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
	
	public FieldList getKeys() {
		return this.keys;
	}
	
	public boolean[] getSortOrders() {
		return sortOrders;
	}
	
	/**
	 * This function overrides the standard behavior of computing costs in the {@link eu.stratosphere.pact.compiler.plan.candidate.PlanNode}.
	 * It adds a check for correct branch handling.
	 * 
	 * @see eu.stratosphere.pact.compiler.plan.candidate.PlanNode#setCosts(eu.stratosphere.pact.compiler.Costs)
	 */
	@Override
	public void setCosts(Costs nodeCosts) {
		super.setCosts(nodeCosts);
		// check, if this node has no branch beneath it, no double-counted cost then
		if (this.lastJoinedBranchNode != null) {
			throw new CompilerException("SingleInputPlanNode should not have a branch node");
		}
	}
	
	/**
	 * Gets the serializer from this PlanNode.
	 *
	 * @return The serializer.
	 */
	public TypeSerializerFactory<?> getSerializer() {
		return serializer;
	}
	
	/**
	 * Sets the serializer for this PlanNode.
	 *
	 * @param serializer The serializer to set.
	 */
	public void setSerializer(TypeSerializerFactory<?> serializer) {
		this.serializer = serializer;
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
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.plan.Visitable#accept(eu.stratosphere.pact.common.plan.Visitor)
	 */
	@Override
	public void accept(Visitor<PlanNode> visitor) {
		if (visitor.preVisit(this)) {
			this.input.getSource().accept(visitor);
			visitor.postVisit(this);
		}
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.candidate.PlanNode#getPredecessors()
	 */
	@Override
	public Iterator<PlanNode> getPredecessors() {
		return new Iterator<PlanNode>() {
			private boolean hasLeft = true;
			@Override
			public boolean hasNext() {
				return this.hasLeft;
			}
			@Override
			public PlanNode next() {
				if (this.hasLeft) {
					this.hasLeft = false;
					return SingleInputPlanNode.this.input.getSource();
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
			private boolean hasLeft = true;
			@Override
			public boolean hasNext() {
				return this.hasLeft;
			}
			@Override
			public Channel next() {
				if (this.hasLeft) {
					this.hasLeft = false;
					return SingleInputPlanNode.this.input;
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
