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


package org.apache.flink.api.common.operators.base;

import java.util.List;

import org.apache.flink.api.common.distributions.DataDistribution;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.operators.Operator;
import org.apache.flink.api.common.operators.Ordering;
import org.apache.flink.api.common.operators.UnaryOperatorInformation;
import org.apache.flink.api.common.operators.util.UserCodeObjectWrapper;
import org.apache.flink.api.common.operators.util.UserCodeWrapper;
import org.apache.flink.types.Nothing;
import org.apache.flink.util.Visitor;

import com.google.common.base.Preconditions;

/**
 * Operator for nodes that act as data sinks, storing the data they receive.
 * The way the data is stored is handled by the {@link org.apache.flink.api.common.io.OutputFormat}.
 */
public class GenericDataSinkBase<IN> extends Operator<Nothing> {

	protected final UserCodeWrapper<? extends OutputFormat<IN>> formatWrapper;

	protected Operator<IN> input = null;

	private Ordering localOrdering;

	private Ordering partitionOrdering;

	private DataDistribution distribution;

	// --------------------------------------------------------------------------------------------

	/**
	 * Creates a GenericDataSink with the provided {@link org.apache.flink.api.common.io.OutputFormat} implementation
	 * and the given name.
	 *
	 * @param f The {@link org.apache.flink.api.common.io.OutputFormat} implementation used to sink the data.
	 * @param name The given name for the sink, used in plans, logs and progress messages.
	 */
	public GenericDataSinkBase(OutputFormat<IN> f, UnaryOperatorInformation<IN, Nothing> operatorInfo, String name) {
		super(operatorInfo, name);

		Preconditions.checkNotNull(f, "The OutputFormat may not be null.");
		this.formatWrapper = new UserCodeObjectWrapper<OutputFormat<IN>>(f);
	}

	/**
	 * Creates a GenericDataSink with the provided {@link org.apache.flink.api.common.io.OutputFormat} implementation
	 * and the given name.
	 *
	 * @param f The {@link org.apache.flink.api.common.io.OutputFormat} implementation used to sink the data.
	 * @param name The given name for the sink, used in plans, logs and progress messages.
	 */
	public GenericDataSinkBase(UserCodeWrapper<? extends OutputFormat<IN>> f, UnaryOperatorInformation<IN, Nothing> operatorInfo, String name) {
		super(operatorInfo, name);
		Preconditions.checkNotNull(f, "The OutputFormat class may not be null.");
		this.formatWrapper = f;
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Returns this operator's input operator.
	 *
	 * @return This operator's input.
	 */
	public Operator<IN> getInput() {
		return this.input;
	}

	/**
	 * Sets the given operator as the input to this operator.
	 *
	 * @param input The operator to use as the input.
	 */
	public void setInput(Operator<IN> input) {
		Preconditions.checkNotNull(input, "The input may not be null.");
		this.input = input;
	}

	/**
	 * Sets the input to the union of the given operators.
	 *
	 * @param inputs The operator(s) that form the input.
	 * @deprecated This method will be removed in future versions. Use the {@link org.apache.flink.api.common.operators.Union} operator instead.
	 */
	@Deprecated
	public void setInputs(Operator<IN>... inputs) {
		Preconditions.checkNotNull(inputs, "The inputs may not be null.");
		this.input = Operator.createUnionCascade(inputs);
	}

	/**
	 * Sets the input to the union of the given operators.
	 *
	 * @param inputs The operator(s) that form the input.
	 * @deprecated This method will be removed in future versions. Use the {@link org.apache.flink.api.common.operators.Union} operator instead.
	 */
	@Deprecated
	public void setInputs(List<Operator<IN>> inputs) {
		Preconditions.checkNotNull(inputs, "The inputs may not be null.");
		this.input = Operator.createUnionCascade(inputs);
	}

	/**
	 * Adds to the input the union of the given operators.
	 *
	 * @param inputs The operator(s) to be unioned with the input.
	 * @deprecated This method will be removed in future versions. Use the {@link org.apache.flink.api.common.operators.Union} operator instead.
	 */
	@Deprecated
	public void addInput(Operator<IN>... inputs) {
		Preconditions.checkNotNull(inputs, "The input may not be null.");
		this.input = Operator.createUnionCascade(this.input, inputs);
	}

	/**
	 * Adds to the input the union of the given operators.
	 *
	 * @param inputs The operator(s) to be unioned with the input.
	 * @deprecated This method will be removed in future versions. Use the {@link org.apache.flink.api.common.operators.Union} operator instead.
	 */
	@SuppressWarnings("unchecked")
	@Deprecated
	public void addInputs(List<? extends Operator<IN>> inputs) {
		Preconditions.checkNotNull(inputs, "The inputs may not be null.");
		this.input = createUnionCascade(this.input, (Operator<IN>[]) inputs.toArray(new Operator[inputs.size()]));
	}

	// --------------------------------------------------------------------------------------------
	
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
		if (partitionOrdering.getNumberOfFields() != distribution.getNumberOfFields()) {
			throw new IllegalArgumentException("The number of keys in the distribution must match number of ordered fields.");
		}
		
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
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Gets the class describing this sinks output format.
	 * 
	 * @return The output format class.
	 */
	public UserCodeWrapper<? extends OutputFormat<IN>> getFormatWrapper() {
		return this.formatWrapper;
	}
	
	/**
	 * Gets the class describing the output format.
	 * <p>
	 * This method is basically identical to {@link #getFormatWrapper()}.
	 * 
	 * @return The class describing the output format.
	 * 
	 * @see org.apache.flink.api.common.operators.Operator#getUserCodeWrapper()
	 */
	@Override
	public UserCodeWrapper<? extends OutputFormat<IN>> getUserCodeWrapper() {
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
	 * @see org.apache.flink.util.Visitable#accept(org.apache.flink.util.Visitor)
	 */
	@Override
	public void accept(Visitor<Operator<?>> visitor) {
		boolean descend = visitor.preVisit(this);
		if (descend) {
			this.input.accept(visitor);
			visitor.postVisit(this);
		}
	}
	
	// --------------------------------------------------------------------------------------------
	
	@Override
	public String toString() {
		return this.name;
	}
}
