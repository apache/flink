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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import eu.stratosphere.pact.common.util.FieldSet;
import eu.stratosphere.pact.runtime.shipping.ShipStrategyType;

/**
 * A connection between to PACTs. Represents a channel together with a data shipping
 * strategy. The data shipping strategy of a channel may be fixed though compiler hints. In that
 * case, it is not modifiable.
 * <p>
 * The connections are also used by the optimization algorithm to propagate interesting properties from the sinks in the
 * direction of the sources.
 */
public class PactConnection implements EstimateProvider
{
	private final OptimizerNode sourcePact; // The source node of the connection

	private final OptimizerNode targetPact; // The target node of the connection.

	private List<InterestingProperties> interestingProps; // local properties that succeeding nodes are interested in

	private ShipStrategyType shipStrategy; // The data distribution strategy, if preset

	/**
	 * Creates a new Connection between two nodes. The shipping strategy is by default <tt>NONE</tt>.
	 * The temp mode is by default <tt>NONE</tt>.
	 * 
	 * @param source
	 *        The source node.
	 * @param target
	 *        The target node.
	 */
	public PactConnection(OptimizerNode source, OptimizerNode target) {
		this(source, target, null);
	}

	/**
	 * Creates a new Connection between two nodes.
	 * 
	 * @param source
	 *        The source node.
	 * @param target
	 *        The target node.
	 * @param shipStrategy
	 *        The shipping strategy.
	 */
	public PactConnection(OptimizerNode source, OptimizerNode target, ShipStrategyType shipStrategy) {
		if (source == null || target == null) {
			throw new NullPointerException("Source and target must not be null.");
		}

		this.sourcePact = source;
		this.targetPact = target;
		this.shipStrategy = shipStrategy;
	}

	/**
	 * Gets the source of the connection.
	 * 
	 * @return The source Node.
	 */
	public OptimizerNode getSourcePact() {
		return this.sourcePact;
	}

	/**
	 * Gets the target of the connection.
	 * 
	 * @return The target node.
	 */
	public OptimizerNode getTargetPact() {
		return this.targetPact;
	}

	/**
	 * Gets the shipping strategy for this connection.
	 * 
	 * @return The connection's shipping strategy.
	 */
	public ShipStrategyType getShipStrategy() {
		return this.shipStrategy;
	}

	/**
	 * Sets the shipping strategy for this connection.
	 * 
	 * @param strategy
	 *        The shipping strategy to be applied to this connection.
	 */
	public void setShipStrategy(ShipStrategyType strategy) {
//		// adjust the ship strategy to the interesting properties, if necessary
//		if (strategy.type() == ShipStrategyType.FORWARD && this.sourcePact.getDegreeOfParallelism() < this.targetPact.getDegreeOfParallelism()) {
//			// check, whether we have an interesting property on partitioning. if so, make sure that we use a
//			// forward strategy that preserves that partitioning by locally routing the keys correctly
//			if (this.interestingProps != null) {
//				for (InterestingProperties props : this.interestingProps) {
//					PartitionProperty pp = props.getGlobalProperties().getPartitioning();
//					if (pp == PartitionProperty.HASH_PARTITIONED || pp == PartitionProperty.ANY) {
//						strategy = new PartitionLocalHashSS(props.getGlobalProperties().getPartitionedFields());
//						break;
//					}
//					else if (pp == PartitionProperty.RANGE_PARTITIONED) {
//						throw new CompilerException("Range partitioning during forwards with changing degree " +
//								"of parallelism is currently not handled!");
//					}
//				}
//			}
//		}

		this.shipStrategy = strategy;
	}

	/**
	 * Gets the interesting properties object for this pact connection.
	 * If the interesting properties for this connections have not yet been set,
	 * this method returns null. If they have been set, but no interesting properties
	 * exist, this returns an empty collection.
	 * 
	 * @return The collection of all interesting properties, or null, if they have not yet been set.
	 */
	public List<InterestingProperties> getInterestingProperties() {
		return this.interestingProps;
	}

	/**
	 * Adds a set of interesting properties to this pact connection.
	 * 
	 * @param props
	 *        The set of interesting properties to add.
	 */
	public void addInterestingProperties(InterestingProperties props) {
		if (this.interestingProps == null) {
			this.interestingProps = new ArrayList<InterestingProperties>();
		}

		this.interestingProps.add(props);
	}

	/**
	 * Adds a collection of interesting property sets to this pact connection.
	 * 
	 * @param props
	 *        The collection of interesting properties to add.
	 */
	public void addAllInterestingProperties(Collection<InterestingProperties> props) {
		if (this.interestingProps == null) {
			this.interestingProps = new ArrayList<InterestingProperties>();
		}

		this.interestingProps.addAll(props);
	}

	/**
	 * Sets the interesting properties of this connection to an empty collection.
	 * That means, the interesting properties have been set, but none exist.
	 */
	public void setNoInterestingProperties() {
		this.interestingProps = Collections.emptyList();
	}

	// --------------------------------------------------------------------------------------------
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.EstimateProvider#getEstimatedOutputSize()
	 */
	@Override
	public long getEstimatedOutputSize() {
		return this.sourcePact.getEstimatedOutputSize();
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.EstimateProvider#getEstimatedNumRecords()
	 */
	@Override
	public long getEstimatedNumRecords() {
		return this.sourcePact.getEstimatedNumRecords();
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.EstimateProvider#getEstimatedCardinalities()
	 */
	@Override
	public Map<FieldSet, Long> getEstimatedCardinalities() {
		return this.sourcePact.getEstimatedCardinalities();
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.EstimateProvider#getEstimatedCardinality(eu.stratosphere.pact.common.util.FieldSet)
	 */
	@Override
	public long getEstimatedCardinality(FieldSet cP) {
		return this.sourcePact.getEstimatedCardinality(cP);
	}
	
	// --------------------------------------------------------------------------------------------

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	public String toString() {
		StringBuilder buf = new StringBuilder(50);
		buf.append("Connection: ");

		if (this.sourcePact == null) {
			buf.append("null");
		} else {
			buf.append(this.sourcePact.getPactContract().getName());
			buf.append('(').append(this.sourcePact.getPactType().name()).append(')');
		}

		buf.append(" -> ");

		if (this.shipStrategy != null) {
			buf.append('[');
			buf.append(this.shipStrategy.name());
			buf.append(']').append(' ');
		}

		if (this.targetPact == null) {
			buf.append("null");
		} else {
			buf.append(this.targetPact.getPactContract().getName());
			buf.append('(').append(this.targetPact.getPactType().name()).append(')');
		}

		return buf.toString();
	}
}