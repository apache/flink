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

import eu.stratosphere.pact.common.io.OutputFormat;
import eu.stratosphere.pact.common.plan.Visitor;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.Value;

/**
 * Contract for nodes which act as data sinks, storing the data they receive somewhere instead of sending it to another
 * contract. The way the data is stored is handled by the {@link OutputFormat}.
 * 
 * @param <KT> type of key of output key/value-pair
 * @param <VT> type of value of output key/value-pair
 */
public class GenericDataSink<KT extends Key, VT extends Value> extends Contract 
{
	private static String DEFAULT_NAME = "<Unnamed Generic Data Sink>";

	// --------------------------------------------------------------------------------------------
	
	protected final Class<? extends OutputFormat<KT, VT>> clazz;

	private List<Contract> input = new ArrayList<Contract>();

	private Order globalOrder = Order.NONE;

	private Order localOrder = Order.NONE;

	// --------------------------------------------------------------------------------------------

	/**
	 * Creates a GenericDataSink with the provided {@link OutputFormat} implementation
	 * and a default name.
	 * 
	 * @param c The {@link OutputFormat} implementation used to sink the data.
	 */
	public GenericDataSink(Class<? extends OutputFormat<KT, VT>> c) {
		this(c, DEFAULT_NAME);
	}
	
	/**
	 * Creates a GenericDataSink with the provided {@link OutputFormat} implementation 
	 * and the given name. 
	 * 
	 * @param c The {@link OutputFormat} implementation used to sink the data.
	 * @param name The given name for the sink, used in plans, logs and progress messages.
	 */
	public GenericDataSink(Class<? extends OutputFormat<KT, VT>> c, String name) {
		super(name);
		this.clazz = c;
	}

	/**
	 * Creates a GenericDataSink with the provided {@link OutputFormat} implementation the default name.
	 * It uses the given contract as its input.
	 * 
	 * @param c The {@link OutputFormat} implementation used to sink the data.
	 * @param input The contract to use as the input.
	 */
	public GenericDataSink(Class<? extends OutputFormat<KT, VT>> c, Contract input) {
		this(c, input, DEFAULT_NAME);
	}
	
	/**
	 * Creates a GenericDataSink with the provided {@link OutputFormat} implementation and the given name.
	 * It uses the given contract as its input.
	 * 
	 * @param c The {@link OutputFormat} implementation used to sink the data.
	 * @param input The contract to use as the input.
	 * @param name The given name for the sink, used in plans, logs and progress messages.
	 */
	public GenericDataSink(Class<? extends OutputFormat<KT, VT>> c, Contract input, String name) {
		this(c, name);
		addInput(input);
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
	 * Gets the order, in which the data sink writes its data globally. By default, this is <tt>NONE</tt>.
	 * 
	 * @return NONE, if the sink writes data in any order, or ASCENDING (resp. DESCENDING),
	 *         if the sink writes it data with a globally ascending (resp. descending) order.
	 */
	public Order getGlobalOrder() {
		return this.globalOrder;
	}
	
	/**
	 * Sets the order in which the sink must write its data. For any value other then <tt>NONE</tt>,
	 * this will cause the system to perform a global sort, or try to reuse an order from a
	 * previous operation.
	 * 
	 * @param globalOrder The order to write the data in.
	 */
	public void setGlobalOrder(Order globalOrder) {
		this.globalOrder = globalOrder;
	}

	/**
	 * Gets the order, in which the data sink writes its data locally. Local order means that
	 * with in each fragment of the file inside the distributed file system, the data is ordered,
	 * but not across file fragments.
	 * 
	 * @return NONE, if the sink writes data in any order, or ASCENDING (resp. DESCENDING),
	 *         if the sink writes it data with a local ascending (resp. descending) order.
	 */
	public Order getLocalOrder() {
		return this.localOrder;
	}
	
	/**
	 * Sets the order in which the sink must write its data within each fragment in the distributed
	 * file system. For any value other then <tt>NONE</tt>, this will cause the system to perform a
	 * local sort, or try to reuse an order from a previous operation.
	 * 
	 * @param localOrder The local order to write the data in.
	 */
	public void setLocalOrder(Order localOrder) {
		this.localOrder = localOrder;
	}
	
	/**
	 * Gets the class describing this sinks output format.
	 * 
	 * @return The output format class.
	 */
	public Class<? extends OutputFormat<KT, VT>> getFormatClass()
	{
		return this.clazz;
	}
	
	/**
	 * Gets the class describing the input format.
	 * <p>
	 * This method is basically identical to {@link #getFormatClass()}.
	 * 
	 * @return The class describing the input format.
	 * 
	 * @see eu.stratosphere.pact.common.contract.Contract#getUserCodeClass()
	 */
	@Override
	public Class<?> getUserCodeClass()
	{
		return this.clazz;
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
	 * @see eu.stratosphere.pact.common.plan.Visitable#accept(eu.stratosphere.pact.common.plan.Visitor)
	 */
	@Override
	public void accept(Visitor<Contract> visitor)
	{
		boolean descend = visitor.preVisit(this);
		if (descend) {
			for(Contract c : this.input) {
				c.accept(visitor);
			}
			visitor.postVisit(this);
		}
	}
}
