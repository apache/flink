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
package eu.stratosphere.pact.common.plan;

import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.Map;

import eu.stratosphere.dag.NodePrinter;
import eu.stratosphere.dag.DAGPrinter;
import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.contract.DataSinkContract;
import eu.stratosphere.pact.common.contract.DataSourceContract;
import eu.stratosphere.pact.common.type.base.PactJsonObject;
import eu.stratosphere.pact.common.type.base.PactNull;
import eu.stratosphere.pact.testing.ioformats.JsonInputFormat;
import eu.stratosphere.pact.testing.ioformats.JsonOutputFormat;

/**
 * 
 */
public class PactModule implements Visitable<Contract> {
	/**
	 * A collection of all outputs in the plan. Since the plan is traversed from the outputs to the sources, this
	 * collection must contain all the outputs.
	 */
	protected final DataSinkContract<PactNull, PactJsonObject>[] outputStubs;

	protected final DataSourceContract<PactNull, PactJsonObject>[] inputStubs;

	// ------------------------------------------------------------------------

	@SuppressWarnings("unchecked")
	public PactModule(int numberOfInputs, int numberOfOutputs) {
		this.inputStubs = new DataSourceContract[numberOfInputs];
		for (int index = 0; index < this.inputStubs.length; index++)
			this.inputStubs[index] = new DataSourceContract<PactNull, PactJsonObject>(JsonInputFormat.class,
				String.valueOf(index));
		this.outputStubs = new DataSinkContract[numberOfOutputs];
		for (int index = 0; index < this.outputStubs.length; index++)
			this.outputStubs[index] = new DataSinkContract<PactNull, PactJsonObject>(JsonOutputFormat.class,
				String.valueOf(index));
	}

	// ------------------------------------------------------------------------

	public DataSinkContract<PactNull, PactJsonObject> getOutput(int index) {
		return this.outputStubs[index];
	}

	public DataSourceContract<PactNull, PactJsonObject> getInput(int index) {
		return this.inputStubs[index];
	}

	public DataSourceContract<PactNull, PactJsonObject>[] getInputStubs() {
		return this.inputStubs;
	}

	public DataSinkContract<PactNull, PactJsonObject>[] getOutputStubs() {
		return this.outputStubs;
	}

	public void setOutput(int index, DataSinkContract<PactNull, PactJsonObject> contract) {
		this.outputStubs[index] = contract;
	}

	public void setInput(int index, DataSourceContract<PactNull, PactJsonObject> contract) {
		this.inputStubs[index] = contract;
	}

	public void validate() {
		for (int index = 0; index < this.outputStubs.length; index++)
			if (this.outputStubs[index].getInput() == null)
				throw new IllegalStateException(String.format("%d. output is not connected", index));

		final Collection<Contract> visitedContracts = this.getAllContracts();

		for (int index = 0; index < this.inputStubs.length; index++)
			if (!visitedContracts.contains(this.inputStubs[index]))
				throw new IllegalStateException(String.format("%d. input is not connected", index));
	}

	public Collection<Contract> getAllContracts() {
		final Map<Contract, Boolean> visitedContracts = new IdentityHashMap<Contract, Boolean>();
		this.accept(new Visitor<Contract>() {

			@Override
			public boolean preVisit(Contract visitable) {
				visitedContracts.put(visitable, Boolean.TRUE);
				return true;
			}

			@Override
			public void postVisit(Contract visitable) {
			}
		});
		return visitedContracts.keySet();
	}

	// ------------------------------------------------------------------------

	/**
	 * Traverses the pact plan, starting from the data outputs that were added to this program.
	 * 
	 * @see eu.stratosphere.pact.common.plan.Visitable#accept(eu.stratosphere.pact.common.plan.Visitor)
	 */
	@Override
	public void accept(Visitor<Contract> visitor) {
		for (Contract output : this.outputStubs)
			output.accept(visitor);
	}

	@Override
	public String toString() {
		return toString(this.outputStubs);
	}

	public static String toString(Contract[] sinks) {
		return new DAGPrinter<Contract>(new ContractNavigator(), sinks).toString(new NodePrinter<Contract>() {
			@Override
			public String toString(Contract node) {
				return String.format("%s [%s]", node.getClass().getSimpleName(), node.getName());
			}
		}, 80);
	}

}
