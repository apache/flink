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
import java.util.List;

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
import eu.stratosphere.util.dag.AbstractSubGraph;
import eu.stratosphere.util.dag.GraphPrinter;
import eu.stratosphere.util.dag.GraphTraverseListener;
import eu.stratosphere.util.dag.NodePrinter;
import eu.stratosphere.util.dag.OneTimeTraverser;

/**
 * The PactModule is a subgraph of a {@link PactProgram} with an arbitrary but well-defined number of inputs and
 * outputs. It is designed to facilitate modularization and thus to increase the maintainability of large
 * PactPrograms. While the interface of the module are the number of inputs and outputs, the actual implementation
 * consists of several interconnected {@link Contract}s that are connected to the inputs and outputs of the PactModule.
 */
public class PactModule extends AbstractSubGraph<Contract, DataSourceContract<?, ?>, DataSinkContract<?, ?>> implements
		Visitable<Contract> {
	/**
	 * Initializes a PactModule having the given number of inputs and outputs.
	 * 
	 * @param numberOfInputs
	 *        the number of inputs
	 * @param numberOfOutputs
	 *        the number of outputs.
	 */
	public PactModule(int numberOfInputs, int numberOfOutputs) {
		super(new DataSourceContract[numberOfInputs], new DataSinkContract[numberOfOutputs], ContractNavigator.INSTANCE);
		for (int index = 0; index < this.inputNodes.length; index++)
			this.inputNodes[index] = new DataSourceContract<PactNull, PactJsonObject>(JsonInputFormat.class,
				String.valueOf(index));
		for (int index = 0; index < this.outputNodes.length; index++)
			this.outputNodes[index] = new DataSinkContract<PactNull, PactJsonObject>(JsonOutputFormat.class,
				String.valueOf(index));
	}

	/**
	 * Traverses the pact plan, starting from the data outputs that were added to this program.
	 * 
	 * @see eu.stratosphere.pact.common.plan.Visitable#accept(eu.stratosphere.pact.common.plan.Visitor)
	 */
	@Override
	public void accept(final Visitor<Contract> visitor) {
		OneTimeVisitor<Contract> oneTimeVisitor = new OneTimeVisitor<Contract>(visitor);
		for (Contract output : this.getAllOutputs())
			output.accept(oneTimeVisitor);
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
		return dagPrinter.toString(this.getAllOutputs(), ContractNavigator.INSTANCE);
	}

	//
	// /*
	// * (non-Javadoc)
	// * @see eu.stratosphere.pact.common.plan.Module#validate()
	// */
	// @Override
	// public void validate() {
	// for (int index = 0; index < this.outputContracts.length; index++)
	// if (this.outputContracts[index].getInput() == null)
	// throw new IllegalStateException(String.format("%d. output is not connected", index));
	//
	// for (int index = this.outputContracts.length; index < this.allOutputContracts.size(); index++)
	// if (this.allOutputContracts.get(index).getInput() == null)
	// throw new IllegalStateException(String.format("%d. internal output is not connected", index
	// - this.outputContracts.length));
	//
	// final Collection<Contract> visitedContracts = this.getAllContracts();
	//
	// for (int index = 0; index < this.inputContracts.length; index++)
	// if (!visitedContracts.contains(this.inputContracts[index]))
	// throw new IllegalStateException(String.format("%d. input is not connected", index));
	// }

	/**
	 * Wraps the graph given by the sinks and referenced contracts in a PactModule.
	 * 
	 * @param sinks
	 *        all sinks that span the graph to wrap
	 * @return a PactModule representing the given graph
	 */
	public static PactModule valueOf(Contract... sinks) {
		return valueOf(Arrays.asList(sinks));
	}

	/**
	 * Wraps the graph given by the sinks and referenced contracts in a PactModule.
	 * 
	 * @param sinks
	 *        all sinks that span the graph to wrap
	 * @return a PactModule representing the given graph
	 */
	public static PactModule valueOf(Collection<Contract> sinks) {
		final List<Contract> inputs = new ArrayList<Contract>();

		OneTimeTraverser.INSTANCE.traverse(sinks, ContractNavigator.INSTANCE,
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

		PactModule module = new PactModule(inputs.size(), sinks.size());
		int sinkIndex = 0;
		for (Contract sink : sinks) {
			if (sink instanceof DataSinkContract<?, ?>)
				module.outputNodes[sinkIndex] = (DataSinkContract<?, ?>) sink;
			else
				module.getOutput(sinkIndex).setInput(sink);
			sinkIndex++;
		}

		for (int index = 0; index < inputs.size(); index++) {
			Contract node = inputs.get(index);
			if (node instanceof DataSourceContract<?, ?>)
				module.inputNodes[index] = (DataSourceContract<?, ?>) node;
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
