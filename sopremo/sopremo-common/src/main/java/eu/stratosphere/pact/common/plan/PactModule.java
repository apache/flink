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
import java.util.Arrays;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import eu.stratosphere.dag.GraphPrinter;
import eu.stratosphere.dag.GraphTraverseListener;
import eu.stratosphere.dag.DependencyAwareGraphTraverser;
import eu.stratosphere.dag.NodePrinter;
import eu.stratosphere.pact.client.nephele.api.PactProgram;
import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.contract.DataSinkContract;
import eu.stratosphere.pact.common.contract.DataSourceContract;
import eu.stratosphere.pact.common.contract.DualInputContract;
import eu.stratosphere.pact.common.contract.SingleInputContract;
import eu.stratosphere.pact.common.type.base.PactNull;
import eu.stratosphere.sopremo.pact.JsonInputFormat;
import eu.stratosphere.sopremo.pact.JsonOutputFormat;
import eu.stratosphere.sopremo.pact.PactJsonObject;

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
	 * {@link #outputContracts} + internal outputs
	 */
	private final List<DataSinkContract<?, ?>> allOutputContracts = new ArrayList<DataSinkContract<?, ?>>();

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
		this.allOutputContracts.addAll(Arrays.asList(this.outputContracts));
	}

	/**
	 * Traverses the pact plan, starting from the data outputs that were added to this program.
	 * 
	 * @see eu.stratosphere.pact.common.plan.Visitable#accept(eu.stratosphere.pact.common.plan.Visitor)
	 */
	@Override
	public void accept(final Visitor<Contract> visitor) {
		OneTimeVisitor<Contract> oneTimeVisitor = new OneTimeVisitor<Contract>(visitor);
		for (Contract output : this.allOutputContracts)
			output.accept(oneTimeVisitor);
	}

	/**
	 * Adds an additional {@link DataSinkContract} to the internal list of outputs. This new output is not part of the
	 * interface of the PactModule. It is only used to traverse the graph and access all nodes.<br>
	 * The function is needed for PactModules that fully or partially act as data sinks. Partial data sinks implement
	 * normal application logic in contracts but also include statistics or debug outputs.
	 * 
	 * @param output
	 *        the output to add internally
	 */
	public void addInternalOutput(DataSinkContract<?, ?> output) {
		this.allOutputContracts.add(output);
	}

	/**
	 * Returns all contracts that are either (internal) output contracts or included in the reference graph.
	 * 
	 * @return all contracts in this module
	 */
	public Collection<Contract> getAllContracts() {
		final Map<Contract, Boolean> visitedContracts = new IdentityHashMap<Contract, Boolean>();
		this.accept(new Visitor<Contract>() {

			@Override
			public void postVisit(Contract visitable) {
			}

			@Override
			public boolean preVisit(Contract visitable) {
				visitedContracts.put(visitable, Boolean.TRUE);
				return true;
			}
		});
		return visitedContracts.keySet();
	}

	/**
	 * Returns all (external) and internal output contracts.
	 * 
	 * @return all output contracts
	 */
	public DataSinkContract<?, ?>[] getAllOutputs() {
		return this.allOutputContracts.toArray(new DataSinkContract<?, ?>[this.allOutputContracts.size()]);
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
	 * Returns all outputs of this PactModule.
	 * 
	 * @return all outputs
	 */
	public DataSinkContract<?, ?>[] getOutputs() {
		return this.outputContracts;
	}

	@Override
	public String toString() {
		GraphPrinter<Contract> dagPrinter = new GraphPrinter<Contract>();
		dagPrinter.setNodePrinter(new NodePrinter<Contract>() {
			@Override
			public String toString(Contract node) {
				return String.format("%s [%s]", node.getClass().getSimpleName(), node.getName());
			}
		});
		dagPrinter.setWidth(80);
		return dagPrinter.toString(this.allOutputContracts, ContractNavigator.INSTANCE);
	}

	/**
	 * Checks whether all declared inputs and outputs are fully connected.
	 * 
	 * @throws IllegalStateException
	 *         if the module is invalid
	 */
	public void validate() {
		for (int index = 0; index < this.outputContracts.length; index++)
			if (this.outputContracts[index].getInput() == null)
				throw new IllegalStateException(String.format("%d. output is not connected", index));

		for (int index = this.outputContracts.length; index < this.allOutputContracts.size(); index++)
			if (this.allOutputContracts.get(index).getInput() == null)
				throw new IllegalStateException(String.format("%d. internal output is not connected", index
					- this.outputContracts.length));

		final Collection<Contract> visitedContracts = this.getAllContracts();

		for (int index = 0; index < this.inputContracts.length; index++)
			if (!visitedContracts.contains(this.inputContracts[index]))
				throw new IllegalStateException(String.format("%d. input is not connected", index));
	}

	/**
	 * Wraps the graph given by the sinks and referenced contracts in a PactModule.
	 * 
	 * @param sinks
	 *        all sinks that span the graph to wrap
	 * @return a PactModule representing the given graph
	 */
	public static PactModule valueOf(Contract... sinks) {
		final List<Contract> inputs = new ArrayList<Contract>();

		DependencyAwareGraphTraverser.INSTANCE.traverse(sinks, ContractNavigator.INSTANCE,
			new GraphTraverseListener<Contract>() {
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

		PactModule module = new PactModule(inputs.size(), sinks.length);
		for (int index = 0; index < sinks.length; index++)
			if (sinks[index] instanceof DataSinkContract<?, ?>)
				module.allOutputContracts.set(index,
					module.outputContracts[index] = (DataSinkContract<?, ?>) sinks[index]);
			else
				module.getOutput(index).setInput(sinks[index]);

		for (int index = 0; index < inputs.size(); index++) {
			Contract node = inputs.get(index);
			if (node instanceof DataSourceContract<?, ?>)
				module.inputContracts[index] = (DataSourceContract<?, ?>) node;
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

}
