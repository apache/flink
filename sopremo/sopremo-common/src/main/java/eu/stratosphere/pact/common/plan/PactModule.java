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

import java.util.ArrayList;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import eu.stratosphere.dag.DAGPrinter;
import eu.stratosphere.dag.DAGTraverseListener;
import eu.stratosphere.dag.DependencyAwareDAGTraverser;
import eu.stratosphere.dag.NodePrinter;
import eu.stratosphere.pact.client.nephele.api.PactProgram;
import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.contract.DataSinkContract;
import eu.stratosphere.pact.common.contract.DataSourceContract;
import eu.stratosphere.pact.common.contract.DualInputContract;
import eu.stratosphere.pact.common.contract.SingleInputContract;
import eu.stratosphere.pact.common.type.base.PactJsonObject;
import eu.stratosphere.pact.common.type.base.PactNull;
import eu.stratosphere.pact.testing.ioformats.JsonInputFormat;
import eu.stratosphere.pact.testing.ioformats.JsonOutputFormat;

/**
 * The PactModule is a subgraph of a {@link PactProgram} with an arbitrary but well-defined number of inputs and
 * outputs. It is designed to facilitate modularization and thus to increase the maintainability of large
 * PactPrograms. While the interface of the module are the number of inputs and outputs, the actual implementation
 * consists of several interconnected {@link Contract}s that are connected to the inputs and outputs of the PactModule.
 */
public class PactModule implements Visitable<Contract> {
	/**
	 * The outputs of the module.
	 */
	private final DataSinkContract<?, ?>[] outputContracts;

	/**
	 * The inputs of the module.
	 */
	private final DataSourceContract<?, ?>[] inputContracts;

	/**
	 * Initializes a PactModule having the given number of inputs and outputs.
	 * 
	 * @param numberOfInputs
	 *        the number of inputs
	 * @param numberOfOutputs
	 *        the number of outputs.
	 */
	public PactModule(int numberOfInputs, int numberOfOutputs) {
		this.inputContracts = new DataSourceContract[numberOfInputs];
		for (int index = 0; index < this.inputContracts.length; index++)
			this.inputContracts[index] = new DataSourceContract<PactNull, PactJsonObject>(JsonInputFormat.class,
				String.valueOf(index));
		this.outputContracts = new DataSinkContract[numberOfOutputs];
		for (int index = 0; index < this.outputContracts.length; index++)
			this.outputContracts[index] = new DataSinkContract<PactNull, PactJsonObject>(JsonOutputFormat.class,
				String.valueOf(index));
	}

	/**
	 * Returns the output at the specified position.
	 * 
	 * @param index
	 *        the index of the output
	 * @return the output at the specified position
	 */
	public DataSinkContract<?, ?> getOutput(int index) {
		return this.outputContracts[index];
	}

	/**
	 * Returns the input at the specified position.
	 * 
	 * @param index
	 *        the index of the input
	 * @return the input at the specified position
	 */
	public DataSourceContract<?, ?> getInput(int index) {
		return this.inputContracts[index];
	}

	/**
	 * Returns all inputs of this PactModule.
	 * 
	 * @return all inputs
	 */
	public DataSourceContract<?, ?>[] getInputs() {
		return this.inputContracts;
	}

	/**
	 * Returns all outputs of this PactModule.
	 * 
	 * @return all outputs
	 */
	public DataSinkContract<?, ?>[] getOutputs() {
		return this.outputContracts;
	}

	/**
	 * Replaces the output at the specified position of the given contract. 
	 * 
	 * @param index
	 *        the index of the output
	 * @param contract
	 * @return the output at the specified position
	 */
	public void setOutput(int index, DataSinkContract<?, ?> contract) {
		this.outputContracts[index] = contract;
	}

	public void setInput(int index, DataSourceContract<?, ?> contract) {
		this.inputContracts[index] = contract;
	}

	public void validate() {
		for (int index = 0; index < this.outputContracts.length; index++)
			if (this.outputContracts[index].getInput() == null)
				throw new IllegalStateException(String.format("%d. output is not connected", index));

		final Collection<Contract> visitedContracts = this.getAllContracts();

		for (int index = 0; index < this.inputContracts.length; index++)
			if (!visitedContracts.contains(this.inputContracts[index]))
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

	public static PactModule valueOf(Contract... outputs) {
		final List<Contract> inputs = new ArrayList<Contract>();

		DependencyAwareDAGTraverser.INSTANCE.traverse(outputs, ContractNavigator.INSTANCE,
			new DAGTraverseListener<Contract>() {
				@Override
				public void nodeTraversed(Contract node) {
					if (node instanceof DataSourceContract<?, ?>)
						inputs.add(node);
					else if (node instanceof DataSinkContract<?, ?>
						&& ((DataSinkContract<?, ?>) node).getInput() == null)
						inputs.add(node);
					else if (node instanceof SingleInputContract<?, ?, ?, ?>
						&& ((SingleInputContract<?, ?, ?, ?>) node).getInput() == null)
						inputs.add(node);
					else if (node instanceof DualInputContract<?, ?, ?, ?, ?, ?>) {
						if (((DualInputContract<?, ?, ?, ?, ?, ?>) node).getFirstInput() == null)
							inputs.add(node);
						if (((DualInputContract<?, ?, ?, ?, ?, ?>) node).getSecondInput() == null)
							inputs.add(node);
					}
				};
			});

		PactModule module = new PactModule(inputs.size(), outputs.length);
		for (int index = 0; index < outputs.length; index++)
			if (outputs[index] instanceof DataSinkContract<?, ?>)
				module.setOutput(index, (DataSinkContract<?, ?>) outputs[index]);
			else
				module.getOutput(index).setInput(outputs[index]);

		for (int index = 0; index < inputs.size(); index++) {
			Contract node = inputs.get(index);
			if (node instanceof DataSourceContract<?, ?>)
				module.setInput(index, (DataSourceContract<?, ?>) node);
			else if (node instanceof DataSinkContract<?, ?>)
				((DataSinkContract<?, ?>) node).setInput(module.getInput(index));
			else if (node instanceof SingleInputContract<?, ?, ?, ?>)
				((SingleInputContract<?, ?, ?, ?>) node).setInput(module.getInput(index));
			else if (node instanceof DualInputContract<?, ?, ?, ?, ?, ?>) {
				if (((DualInputContract<?, ?, ?, ?, ?, ?>) node).getFirstInput() != null)
					((DualInputContract<?, ?, ?, ?, ?, ?>) node).setFirstInput(module.getInput(index));
				if (((DualInputContract<?, ?, ?, ?, ?, ?>) node).getSecondInput() != null)
					((DualInputContract<?, ?, ?, ?, ?, ?>) node).setSecondInput(module.getInput(index));
			}
		}
		return module;
	}

	// ------------------------------------------------------------------------

	/**
	 * Traverses the pact plan, starting from the data outputs that were added to this program.
	 * 
	 * @see eu.stratosphere.pact.common.plan.Visitable#accept(eu.stratosphere.pact.common.plan.Visitor)
	 */
	@Override
	public void accept(Visitor<Contract> visitor) {
		for (Contract output : this.outputContracts)
			output.accept(visitor);
	}

	@Override
	public String toString() {
		DAGPrinter<Contract> dagPrinter = new DAGPrinter<Contract>();
		dagPrinter.setNodePrinter(new NodePrinter<Contract>() {
			@Override
			public String toString(Contract node) {
				return String.format("%s [%s]", node.getClass().getSimpleName(), node.getName());
			}
		});
		dagPrinter.setWidth(80);
		return dagPrinter.toString(this.outputContracts, ContractNavigator.INSTANCE);
	}

}
