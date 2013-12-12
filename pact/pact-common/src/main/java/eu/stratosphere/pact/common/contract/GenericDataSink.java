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

package eu.stratosphere.pact.common.contract;

import java.util.ArrayList;
import java.util.List;

import eu.stratosphere.pact.common.distributions.DataDistribution;
import eu.stratosphere.pact.common.util.Visitor;
import eu.stratosphere.pact.generic.contract.Contract;
import eu.stratosphere.pact.generic.contract.UserCodeClassWrapper;
import eu.stratosphere.pact.generic.contract.UserCodeObjectWrapper;
import eu.stratosphere.pact.generic.contract.UserCodeWrapper;
import eu.stratosphere.pact.generic.io.OutputFormat;

/**
 * Contract for nodes which act as data sinks, storing the data they receive somewhere instead of sending it to another
 * contract. The way the data is stored is handled by the {@link OutputFormat}.
 * 
 */
public class GenericDataSink extends Contract {
	
	private static String DEFAULT_NAME = "<Unnamed Generic Data Sink>";

	// --------------------------------------------------------------------------------------------
	
	protected final UserCodeWrapper<? extends OutputFormat<?>> formatWrapper;

	private List<Contract> input = new ArrayList<Contract>();

	private Ordering localOrdering;
	
	private Ordering partitionOrdering;
	
	private DataDistribution distribution;

	// --------------------------------------------------------------------------------------------

	/**
	 * Creates a GenericDataSink with the provided {@link OutputFormat} implementation 
	 * and the given name. 
	 * 
	 * @param f The {@link OutputFormat} implementation used to sink the data.
	 * @param name The given name for the sink, used in plans, logs and progress messages.
	 */
	public GenericDataSink(OutputFormat<?> f, String name) {
		super(name);
		this.formatWrapper = new UserCodeObjectWrapper<OutputFormat<?>>(f);
	}

	
	/**
	 * Creates a GenericDataSink with the provided {@link OutputFormat} implementation
	 * and a default name.
	 * 
	 * @param f The {@link OutputFormat} implementation used to sink the data.
	 */
	public GenericDataSink(OutputFormat<?> f) {
		this(f, DEFAULT_NAME);
	}
	
	/**
	 * Creates a GenericDataSink with the provided {@link OutputFormat} implementation the default name.
	 * It uses the given contract as its input.
	 * 
	 * @param f The {@link OutputFormat} implementation used to sink the data.
	 * @param input The contract to use as the input.
	 */
	public GenericDataSink(OutputFormat<?> f, Contract input) {
		this(f, input, DEFAULT_NAME);
	}
	
	/**
	 * Creates a GenericDataSink with the provided {@link OutputFormat} implementation the default name.
	 * It uses the given contracts as its input.
	 * 
	 * @param f The {@link OutputFormat} implementation used to sink the data.
	 * @param input The contracts to use as the input.
	 */
	public GenericDataSink(OutputFormat<?> f, List<Contract> input) {
		this(f, input, DEFAULT_NAME);
	}

	/**
	 * Creates a GenericDataSink with the provided {@link OutputFormat} implementation and the given name.
	 * It uses the given contract as its input.
	 * 
	 * @param f The {@link OutputFormat} implementation used to sink the data.
	 * @param input The contract to use as the input.
	 * @param name The given name for the sink, used in plans, logs and progress messages.
	 */
	public GenericDataSink(OutputFormat<?> f, Contract input, String name) {
		this(f, name);
		addInput(input);
	}

	/**
	 * Creates a GenericDataSink with the provided {@link OutputFormat} implementation and the given name.
	 * It uses the given contracts as its input.
	 * 
	 * @param f The {@link OutputFormat} implementation used to sink the data.
	 * @param input The contracts to use as the input.
	 * @param name The given name for the sink, used in plans, logs and progress messages.
	 */
	public GenericDataSink(OutputFormat<?> f, List<Contract> input, String name) {
		this(f, name);
		addInputs(input);
	}
	
	/**
	 * Creates a GenericDataSink with the provided {@link OutputFormat} implementation 
	 * and the given name. 
	 * 
	 * @param f The {@link OutputFormat} implementation used to sink the data.
	 * @param name The given name for the sink, used in plans, logs and progress messages.
	 */
	public GenericDataSink(Class<? extends OutputFormat<?>> f, String name) {
		super(name);
		this.formatWrapper = new UserCodeClassWrapper<OutputFormat<?>>(f);
	}

	
	/**
	 * Creates a GenericDataSink with the provided {@link OutputFormat} implementation
	 * and a default name.
	 * 
	 * @param f The {@link OutputFormat} implementation used to sink the data.
	 */
	public GenericDataSink(Class<? extends OutputFormat<?>> f) {
		this(f, DEFAULT_NAME);
	}
	
	/**
	 * Creates a GenericDataSink with the provided {@link OutputFormat} implementation the default name.
	 * It uses the given contract as its input.
	 * 
	 * @param f The {@link OutputFormat} implementation used to sink the data.
	 * @param input The contract to use as the input.
	 */
	public GenericDataSink(Class<? extends OutputFormat<?>> f, Contract input) {
		this(f, input, DEFAULT_NAME);
	}
	
	/**
	 * Creates a GenericDataSink with the provided {@link OutputFormat} implementation the default name.
	 * It uses the given contracts as its input.
	 * 
	 * @param f The {@link OutputFormat} implementation used to sink the data.
	 * @param input The contracts to use as the input.
	 */
	public GenericDataSink(Class<? extends OutputFormat<?>> f, List<Contract> input) {
		this(f, input, DEFAULT_NAME);
	}

	/**
	 * Creates a GenericDataSink with the provided {@link OutputFormat} implementation and the given name.
	 * It uses the given contract as its input.
	 * 
	 * @param f The {@link OutputFormat} implementation used to sink the data.
	 * @param input The contract to use as the input.
	 * @param name The given name for the sink, used in plans, logs and progress messages.
	 */
	public GenericDataSink(Class<? extends OutputFormat<?>> f, Contract input, String name) {
		this(f, name);
		addInput(input);
	}

	/**
	 * Creates a GenericDataSink with the provided {@link OutputFormat} implementation and the given name.
	 * It uses the given contracts as its input.
	 * 
	 * @param f The {@link OutputFormat} implementation used to sink the data.
	 * @param input The contracts to use as the input.
	 * @param name The given name for the sink, used in plans, logs and progress messages.
	 */
	public GenericDataSink(Class<? extends OutputFormat<?>> f, List<Contract> input, String name) {
		this(f, name);
		addInputs(input);
	}

	// --------------------------------------------------------------------------------------------
	
	/**
	 * Returns the contract which acts as the input, or null, if none is set.
	 * 
	 * @return the contract's input contract.
	 */
	public List<Contract> getInputs() {
		return this.input;
	}

	/**
	 * Connects the input to the task wrapped in this contract.
	 * 
	 * @param input the contract's input contract
	 */
	public void addInput(Contract input) {
		this.input.add(input);
	}

	/**
	 * Connects the inputs to the task wrapped in this contract
	 * 
	 * @param inputs The contracts will be set as input.
	 */
	public void addInputs(List<Contract> inputs) {
		this.input.addAll(inputs);
	}

	/**
	 * Clears all previous connections and sets the given contract as
	 * single input of this contract.
	 * 
	 * @param input	The contract will be set as input.
	 */
	public void setInput(Contract input) {
		this.input.clear();
		this.input.add(input);
	}
	
	/**
	 * Clears all previous connections and sets the given contracts as
	 * inputs of this contract.
	 * 
	 * @param inputs The contracts will be set as inputs.
	 */
	public void setInputs(List<? extends Contract> inputs) {
		this.input.clear();
		this.input.addAll(inputs);
	}
	
	/**
	 * Sets the order in which the sink must write its data. For any value other then <tt>NONE</tt>,
	 * this will cause the system to perform a global sort, or try to reuse an order from a
	 * previous operation.
	 * 
	 * @param globalOrder The order to write the data in.
	 */
	public void setGlobalOrder(Ordering globalOrder) {
		this.localOrdering = globalOrder;
		setRangePartitioned(globalOrder);
	}
	
	/**
	 * Sets the order in which the sink must write its data. For any value other then <tt>NONE</tt>,
	 * this will cause the system to perform a global sort, or try to reuse an order from a
	 * previous operation.
	 * 
	 * @param globalOrder The order to write the data in.
	 * @param distribution The distribution to use for the range partitioning.
	 */
	public void setGlobalOrder(Ordering globalOrder, DataDistribution distribution) {
		this.localOrdering = globalOrder;
		setRangePartitioned(globalOrder, distribution);
	}

	/**
	 * Gets the order, in which the data sink writes its data locally. Local order means that
	 * with in each fragment of the file inside the distributed file system, the data is ordered,
	 * but not across file fragments.
	 * 
	 * @return NONE, if the sink writes data in any order, or ASCENDING (resp. DESCENDING),
	 *         if the sink writes it data with a local ascending (resp. descending) order.
	 */
	public Ordering getLocalOrder() {
		return this.localOrdering;
	}
	
	/**
	 * Sets the order in which the sink must write its data within each fragment in the distributed
	 * file system. For any value other then <tt>NONE</tt>, this will cause the system to perform a
	 * local sort, or try to reuse an order from a previous operation.
	 * 
	 * @param localOrder The local order to write the data in.
	 */
	public void setLocalOrder(Ordering localOrder) {
		this.localOrdering = localOrder;
	}
	
	/**
	 * Gets the record ordering over which the sink partitions in ranges.
	 * 
	 * @return The record ordering over which to partition in ranges.
	 */
	public Ordering getPartitionOrdering() {
		return this.partitionOrdering;
	}
	
	/**
	 * Sets the sink to partition the records into ranges over the given ordering.
	 * 
	 * @param partitionOrdering The record ordering over which to partition in ranges.
	 */
	public void setRangePartitioned(Ordering partitionOrdering) {
		throw new UnsupportedOperationException(
			"Range partitioning is currently only supported with a user supplied data distribution.");
	}
	
	/**
	 * Sets the sink to partition the records into ranges over the given ordering.
	 * The bucket boundaries are determined using the given data distribution.
	 * 
	 * @param partitionOrdering The record ordering over which to partition in ranges.
	 * @param distribution The distribution to use for the range partitioning.
	 */
	public void setRangePartitioned(Ordering partitionOrdering, DataDistribution distribution) {
		if (partitionOrdering.getNumberOfFields() != distribution.getNumberOfFields())
			throw new IllegalArgumentException("The number of keys in the distribution must match number of ordered fields.");
		
		// TODO: check compatibility of distribution and ordering (number and order of keys, key types, etc.
		// TODO: adapt partition ordering to data distribution (use prefix of ordering)
		
		this.partitionOrdering = partitionOrdering;
		this.distribution = distribution;
	}
	
	/**
	 * Gets the distribution to use for the range partitioning.
	 * 
	 * @return The distribution to use for the range partitioning.
	 */
	public DataDistribution getDataDistribution() {
		return this.distribution;
	}
	
	/**
	 * Gets the class describing this sinks output format.
	 * 
	 * @return The output format class.
	 */
	public UserCodeWrapper<? extends OutputFormat<?>> getFormatWrapper() {
		return this.formatWrapper;
	}
	
	/**
	 * Gets the class describing the output format.
	 * <p>
	 * This method is basically identical to {@link #getFormatWrapper()}.
	 * 
	 * @return The class describing the output format.
	 * 
	 * @see eu.stratosphere.pact.generic.contract.Contract#getUserCodeWrapper()
	 */
	@Override
	public UserCodeWrapper<? extends OutputFormat<?>> getUserCodeWrapper() {
		return this.formatWrapper;
	}

	// --------------------------------------------------------------------------------------------
	
	/**
	 * Accepts the visitor and applies it this instance. This method applies the visitor in a depth-first traversal.
	 * The visitors pre-visit method is called and, if returning 
	 * <tt>true</tt>, the visitor is recursively applied on the single input. After the recursion returned,
	 * the post-visit method is called.
	 * 
	 * @param visitor The visitor.
	 *  
	 * @see eu.stratosphere.pact.common.util.Visitable#accept(eu.stratosphere.pact.common.util.Visitor)
	 */
	@Override
	public void accept(Visitor<Contract> visitor) {
		boolean descend = visitor.preVisit(this);
		if (descend) {
			for (Contract c : this.input) {
				c.accept(visitor);
			}
			visitor.postVisit(this);
		}
	}
	
	// --------------------------------------------------------------------------------------------
	
	@Override
	public String toString() {
		return this.name;
	}
}
