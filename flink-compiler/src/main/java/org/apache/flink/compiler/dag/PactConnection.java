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

import org.apache.flink.compiler.dataproperties.InterestingProperties;
import org.apache.flink.compiler.plandump.DumpableConnection;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;

/**
 * A connection between to PACTs. Represents a channel together with a data shipping
 * strategy. The data shipping strategy of a channel may be fixed though compiler hints. In that
 * case, it is not modifiable.
 * <p>
 * The connections are also used by the optimization algorithm to propagate interesting properties from the sinks in the
 * direction of the sources.
 */
public class PactConnection implements EstimateProvider, DumpableConnection<OptimizerNode> {
	
	private final OptimizerNode source; // The source node of the connection

	private final OptimizerNode target; // The target node of the connection.

	private InterestingProperties interestingProps; // local properties that succeeding nodes are interested in

	private ShipStrategyType shipStrategy; // The data distribution strategy, if preset
	
	private TempMode materializationMode = TempMode.NONE;
	
	private int maxDepth = -1;

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
		this.source = source;
		this.target = target;
		this.shipStrategy = shipStrategy;
	}
	
	/**
	 * Creates a new Connection between two nodes.
	 * 
	 * @param source
	 *        The source node.
	 */
	public PactConnection(OptimizerNode source) {
		if (source == null) {
			throw new NullPointerException("Source and target must not be null.");
		}
		this.source = source;
		this.target = null;
		this.shipStrategy = ShipStrategyType.NONE;
	}

	/**
	 * Gets the source of the connection.
	 * 
	 * @return The source Node.
	 */
	public OptimizerNode getSource() {
		return this.source;
	}

	/**
	 * Gets the target of the connection.
	 * 
	 * @return The target node.
	 */
	public OptimizerNode getTarget() {
		return this.target;
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
		this.shipStrategy = strategy;
	}

	/**
	 * Gets the interesting properties object for this pact connection.
	 * If the interesting properties for this connections have not yet been set,
	 * this method returns null.
	 * 
	 * @return The collection of all interesting properties, or null, if they have not yet been set.
	 */
	public InterestingProperties getInterestingProperties() {
		return this.interestingProps;
	}

	/**
	 * Sets the interesting properties for this pact connection.
	 * 
	 * @param props The interesting properties.
	 */
	public void setInterestingProperties(InterestingProperties props) {
		if (this.interestingProps == null) {
			this.interestingProps = props;
		} else {
			throw new IllegalStateException("Interesting Properties have already been set.");
		}
	}
	
	public void clearInterestingProperties() {
		this.interestingProps = null;
	}
	
	public void initMaxDepth() {
		
		if (this.maxDepth == -1) {
			this.maxDepth = this.source.getMaxDepth() + 1;
		} else {
			throw new IllegalStateException("Maximum path depth has already been initialized.");
		}
	}
	
	public int getMaxDepth() {
		if (this.maxDepth != -1) {
			return this.maxDepth;
		} else {
			throw new IllegalStateException("Maximum path depth has not been initialized.");
		}
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public long getEstimatedOutputSize() {
		return this.source.getEstimatedOutputSize();
	}

	@Override
	public long getEstimatedNumRecords() {
		return this.source.getEstimatedNumRecords();
	}
	
	@Override
	public float getEstimatedAvgWidthPerOutputRecord() {
		return this.source.getEstimatedAvgWidthPerOutputRecord();
	}
	
	// --------------------------------------------------------------------------------------------

	
	public TempMode getMaterializationMode() {
		return this.materializationMode;
	}
	
	public void setMaterializationMode(TempMode materializationMode) {
		this.materializationMode = materializationMode;
	}
	
	public boolean isOnDynamicPath() {
		return this.source.isOnDynamicPath();
	}
	
	public int getCostWeight() {
		return this.source.getCostWeight();
	}

	// --------------------------------------------------------------------------------------------

	public String toString() {
		StringBuilder buf = new StringBuilder(50);
		buf.append("Connection: ");

		if (this.source == null) {
			buf.append("null");
		} else {
			buf.append(this.source.getPactContract().getName());
			buf.append('(').append(this.source.getName()).append(')');
		}

		buf.append(" -> ");

		if (this.shipStrategy != null) {
			buf.append('[');
			buf.append(this.shipStrategy.name());
			buf.append(']').append(' ');
		}

		if (this.target == null) {
			buf.append("null");
		} else {
			buf.append(this.target.getPactContract().getName());
			buf.append('(').append(this.target.getName()).append(')');
		}

		return buf.toString();
	}
}
