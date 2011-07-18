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

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.io.OutputFormat;
import eu.stratosphere.pact.common.plan.Visitor;
import eu.stratosphere.pact.common.stub.Stub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.Value;

/**
 * Contract for end nodes which produce no output. Typically the input is written
 * to a file, or materialized in another way.
 * 
 * @author Erik Nijkamp
 * @author Moritz Kaufmann
 */
public class DataSinkContract<KT extends Key, VT extends Value> extends Contract {
	private static String defaultName = "DataSink #";

	private static int nextID = 1;

	private Contract input;

	private Order globalOrder = Order.NONE;

	private Order localOrder = Order.NONE;

	protected final Class<? extends OutputFormat<KT, VT>> clazz;

	protected final String filePath;

	protected final Configuration formatParameters;

	/**
	 * Creates a new instance wrapping the given output format class.
	 * 
	 * @param c
	 *        Class implementing the desired OutputFormat
	 * @param file
	 *        Location where the output should be written to
	 * @param name
	 *        Name of the node
	 */
	public DataSinkContract(Class<? extends OutputFormat<KT, VT>> c, String file, String name) {
		super(name);

		clazz = c;
		filePath = file;
		formatParameters = new Configuration();
	}

	/**
	 * Creates a new instance wrapping the given output format class.
	 * 
	 * @param c
	 *        Class implementing the desired OutputFormat
	 * @param file
	 *        Location where the output should be written to
	 */
	public DataSinkContract(Class<? extends OutputFormat<KT, VT>> c, String file) {
		this(c, file, defaultName + (nextID++));
	}

	/**
	 * Returns the input or null if none is set
	 * 
	 * @return the contract's input contract
	 */
	public Contract getInput() {
		return input;
	}

	/**
	 * Connects the input to the task wrapped in this contract
	 * 
	 * @param input the contract's input contract
	 */
	public DataSinkContract<KT, VT> setInput(Contract input) {
		this.input = input;
		return this;
	}

	/**
	 * Gets the order, in which the data sink writes its data globally. By default, this is <tt>NONE</tt>.
	 * 
	 * @return NONE, if the sink writes data in any order, or ASCENDING (resp. DESCENDING),
	 *         if the sink writes it data with a globally ascending (resp. descending) order.
	 */
	public Order getGlobalOrder() {
		return globalOrder;
	}

	/**
	 * Sets the order in which the sink must write its data. For any value other then <tt>NONE</tt>,
	 * this will cause the system to perform a global sort, or try to reuse an order from a
	 * previous operation.
	 * 
	 * @param globalOrder
	 *        The order to write the data in.
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
		return localOrder;
	}
	
	/**
	 * Sets the order in which the sink must write its data within each fragment in the distributed
	 * file system. For any value other then <tt>NONE</tt>, this will cause the system to perform a
	 * local sort, or try to reuse an order from a previous operation.
	 * 
	 * @param localOrder
	 *        The local order to write the data in.
	 */
	public void setLocalOrder(Order localOrder) {
		this.localOrder = localOrder;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void accept(Visitor<Contract> visitor)
	{
		boolean descend = visitor.preVisit(this);
		if (descend) {
			if (input != null) {
				input.accept(visitor);
			}
			visitor.postVisit(this);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Class<? extends Stub<?, ?>> getStubClass() {
		return clazz;
	}

	/**
	 * Returns the configured file path where the output is
	 * written to.
	 * 
	 * @return The path to which the output shall be written
	 */
	public String getFilePath() {
		return filePath;
	}

	/**
	 * Configure the settings for the output format.
	 * 
	 * @param key The key of the format parameter.
	 * @param value The value of the format parameter.
	 */
	public void setFormatParameter(String key, String value) {
		formatParameters.setString(key, value);
	}

	/**
	 * Returns the parameters set for the OutputFormat
	 * 
	 * @return The configuration holding all format parameters.
	 */
	public Configuration getFormatParameters() {
		return formatParameters;
	}
}
