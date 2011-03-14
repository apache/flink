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

import eu.stratosphere.pact.common.contract.Order;
import eu.stratosphere.pact.compiler.CompilerException;
import eu.stratosphere.pact.compiler.GlobalProperties;
import eu.stratosphere.pact.compiler.LocalProperties;
import eu.stratosphere.pact.compiler.PartitionProperty;
import eu.stratosphere.pact.runtime.task.util.OutputEmitter.ShipStrategy;

/**
 * A connection between to PACTs. Represents a channel together with a data shipping
 * strategy. The data shipping strategy of a channel may be fixed though compiler hints. In that
 * case, it is not modifiable.
 * <p>
 * The connections are also used by the optimization algorithm to propagate interesting properties from the sinks in the
 * direction of the sources.
 */
public class PactConnection {

	/**
	 * Enumeration to indicate the mode of temporarily materializing the data that flows across a connection.
	 * Introducing such an artificial dam is sometimes necessary to avoid that a certain data flows deadlock
	 * themselves.
	 */
	public enum TempMode {
		NONE, TEMP_SENDER_SIDE, TEMP_RECEIVER_SIDE
	}

	private final OptimizerNode sourcePact; // The source node of the connection

	private final OptimizerNode targetPact; // The target node of the connection.

	private List<InterestingProperties> interestingProps; // local properties that succeeding nodes are interested in

	private ShipStrategy shipStrategy; // The data distribution strategy

	private TempMode tempMode; // indicates whether and where the connection needs to be temped by a TempTask

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
		this(source, target, ShipStrategy.NONE);
	}

	/**
	 * Creates a new Connection between two nodes.
	 * The temp mode is by default <tt>NONE</tt>.
	 * 
	 * @param source
	 *        The source node.
	 * @param target
	 *        The target node.
	 * @param shipStrategy
	 *        The shipping strategy.
	 */
	public PactConnection(OptimizerNode source, OptimizerNode target, ShipStrategy shipStrategy) {
		this(source, target, shipStrategy, TempMode.NONE);
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
	 * @param tempMode
	 *        The temp mode.
	 */
	public PactConnection(OptimizerNode source, OptimizerNode target, ShipStrategy shipStrategy, TempMode tempMode) {
		if (source == null || target == null) {
			throw new NullPointerException("Source and target must not be null.");
		}

		this.sourcePact = source;
		this.targetPact = target;
		this.shipStrategy = shipStrategy;
		this.tempMode = tempMode;
	}

	/**
	 * Creates a copy of the given connection, but sets as source and target the given nodes.
	 * This constructor is intended as a partial-copy constructor for the enumeration of
	 * plans in the optimization phase.
	 * 
	 * @param template
	 *        The connection to copy the properties from.
	 * @param source
	 *        The reference to the source node.
	 * @param target
	 *        The reference to the target node.
	 */
	public PactConnection(PactConnection template, OptimizerNode source, OptimizerNode target) {
		this.sourcePact = source;
		this.targetPact = target;

		this.shipStrategy = template.shipStrategy;
		this.tempMode = template.tempMode;

		this.interestingProps = template.interestingProps;
	}

	/**
	 * Gets the source of the connection.
	 * 
	 * @return The source Node.
	 */
	public OptimizerNode getSourcePact() {
		return sourcePact;
	}

	/**
	 * Gets the target of the connection.
	 * 
	 * @return The target node.
	 */
	public OptimizerNode getTargetPact() {
		return targetPact;
	}

	/**
	 * Gets the shipping strategy for this connection.
	 * 
	 * @return The connection's shipping strategy.
	 */
	public ShipStrategy getShipStrategy() {
		return shipStrategy;
	}

	/**
	 * Sets the shipping strategy for this connection.
	 * 
	 * @param strategy
	 *        The shipping strategy to be applied to this connection.
	 */
	public void setShipStrategy(ShipStrategy strategy) {
		// adjust the ship strategy to the interesting properties, if necessary
		if (strategy == ShipStrategy.FORWARD && sourcePact.getDegreeOfParallelism() < targetPact.getDegreeOfParallelism()) {
			// check, whether we have an interesting property on partitioning. if so, make sure that we use a
			// forward strategy that preserves that partitioning by locally routing the keys correctly
			if(this.interestingProps != null) {
				for (InterestingProperties props : this.interestingProps) {
					PartitionProperty pp = props.getGlobalProperties().getPartitioning();
					if (pp == PartitionProperty.HASH_PARTITIONED || pp == PartitionProperty.ANY) {
						strategy = ShipStrategy.PARTITION_LOCAL_HASH;
						break;
					}
					else if (pp == PartitionProperty.RANGE_PARTITIONED) {
						throw new CompilerException("Range partitioning during forwards with changing degree " +
								"of parallelism is currently not handled!");
					}
				}
			}
		}
		
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
		return interestingProps;
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

	/**
	 * Returns the TempMode of the Connection. NONE if the connection is not temped,
	 * TEMP_SENDER_SIDE if the connection is temped on the sender node, and
	 * TEMP_RECEIVER_SIDE if the connection is temped on the receiver node.
	 * 
	 * @return TempMode of the connection
	 */
	public TempMode getTempMode() {
		return tempMode;
	}

	/**
	 * Sets the temp mode of the connection.
	 * 
	 * @param tempMode
	 *        The temp mode of the connection.
	 */
	public void setTempMode(TempMode tempMode) {
		this.tempMode = tempMode;
	}

	/**
	 * Gets the global properties of the data after this connection.
	 * 
	 * @return The global data properties of the output data.
	 */
	public GlobalProperties getGlobalProperties() {
		return PactConnection.getGlobalPropertiesAfterConnection(sourcePact, targetPact, shipStrategy);
	}

	/**
	 * Gets the local properties of the data after this connection.
	 * 
	 * @return The local data properties of the output data.
	 */
	public LocalProperties getLocalProperties() {
		return PactConnection.getLocalPropertiesAfterConnection(sourcePact, targetPact, shipStrategy);
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	public String toString() {
		StringBuilder buf = new StringBuilder(50);
		buf.append("Connection: ");

		if (sourcePact == null) {
			buf.append("null");
		} else {
			buf.append(sourcePact.getPactContract().getName());
			buf.append('(').append(sourcePact.getPactType().name()).append(')');
		}

		buf.append(" -> ");

		if (shipStrategy != null) {
			buf.append('[');
			buf.append(shipStrategy.name());
			buf.append(']').append(' ');
		}

		if (targetPact == null) {
			buf.append("null");
		} else {
			buf.append(targetPact.getPactContract().getName());
			buf.append('(').append(targetPact.getPactType().name()).append(')');
		}

		return buf.toString();
	}

	/**
	 * Gets the global properties of the source's output after it crossed a pact connection with
	 * the given shipping strategy.
	 * Global properties are maintained on <tt>FORWARD</tt> connections.
	 * If a partitioning happens, then a partitioning property exists afterwards.
	 * A <tt>BROADCAST</tt> connection destroys the key uniqueness.
	 * <p>
	 * If the shipping strategy has not yet been determined, the properties of the connections source are returned.
	 * 
	 * @return The properties of the data after this channel.
	 */
	public static GlobalProperties getGlobalPropertiesAfterConnection(OptimizerNode source, OptimizerNode target, ShipStrategy shipMode) {
		GlobalProperties gp = source.getGlobalProperties().createCopy();

		switch (shipMode) {
		case BROADCAST:
			gp.reset();
			break;
		case PARTITION_RANGE:
			gp.setPartitioning(PartitionProperty.RANGE_PARTITIONED);
			break;
		case PARTITION_HASH:
			gp.setPartitioning(PartitionProperty.HASH_PARTITIONED);
			gp.setKeyOrder(Order.NONE);
			break;
		case FORWARD:
			if (source.getDegreeOfParallelism() > target.getDegreeOfParallelism()) {
				gp.setKeyOrder(Order.NONE);
			}
			// nothing else changes
			break;
		case NONE:
			throw new CompilerException(
				"Cannot determine properties after connection, id shipping strategy is not set.");
		case SFR:
		default:
			throw new CompilerException("Unsupported shipping strategy: " + shipMode.name());
		}

		return gp;
	}

	/**
	 * Gets the local properties of the sources output after it crossed a pact connection with the given
	 * strategy. Local properties are only maintained on <tt>FORWARD</tt> connections.
	 * 
	 * @return The properties of the data after a channel using the given strategy.
	 */
	public static LocalProperties getLocalPropertiesAfterConnection(OptimizerNode source, OptimizerNode target, ShipStrategy shipMode) {
		LocalProperties lp = source.getLocalProperties().createCopy();

		if (shipMode == null || shipMode == ShipStrategy.NONE) {
			throw new CompilerException("Cannot determine properties if shipping strategy is not defined.");
		}
		else if (shipMode == ShipStrategy.FORWARD) {
			if (source.getDegreeOfParallelism() > target.getDegreeOfParallelism()) {
				// any order is destroyed by the random merging of the inputs
				lp.setKeyOrder(Order.NONE);
				// keys are only grouped if they are unique
				lp.setKeysGrouped(lp.isKeyUnique());
			}
		}
		else {
			lp.reset();
		}

		return lp;
	}
}