package eu.stratosphere.sopremo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.contract.FileDataSinkContract;
import eu.stratosphere.pact.common.contract.FileDataSourceContract;
import eu.stratosphere.pact.common.plan.ContractUtil;
import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.util.dag.DependencyAwareGraphTraverser;
import eu.stratosphere.util.dag.GraphModule;
import eu.stratosphere.util.dag.GraphPrinter;
import eu.stratosphere.util.dag.GraphTraverseListener;
import eu.stratosphere.util.dag.OneTimeTraverser;

/**
 * Encapsulate a partial query in Sopremo and translates it to a {@link PactModule}.
 * 
 * @author Arvid Heise
 */
public class SopremoModule extends GraphModule<Operator<?>, Source, Sink> {

	/**
	 * Initializes a SopremoModule having the given name, number of inputs, and number of outputs.
	 * 
	 * @param name
	 *        the name of the SopremoModule
	 * @param numberOfInputs
	 *        the number of inputs
	 * @param numberOfOutputs
	 *        the number of outputs.
	 */
	public SopremoModule(final String name, final int numberOfInputs, final int numberOfOutputs) {
		super(name, new Source[numberOfInputs], new Sink[numberOfOutputs], OperatorNavigator.INSTANCE);
		for (int index = 0; index < this.outputNodes.length; index++)
			this.outputNodes[index] = new Sink(String.format("%s %d", name, index));
		for (int index = 0; index < this.inputNodes.length; index++)
			this.inputNodes[index] = new Source(String.format("%s %d", name, index));
	}

	/**
	 * Allows to embed this module in a graph of Sopremo operators.
	 * 
	 * @return an operator view of this SopremoModule
	 */
	public Operator<?> asOperator() {
		return new ModuleOperator(this.getOutputs().length, this.getInputs());
	}

	/**
	 * Converts the Sopremo module to a Pact module.
	 * 
	 * @param context
	 *        the evaluation context of the Pact contracts
	 * @return the converted Pact module
	 */
	public PactModule asPactModule(final EvaluationContext context) {
		return PactModule.valueOf(this.getName(), this.assemblePact(context));
	}

	//
	// public SopremoModule asElementary() {
	// return SopremoModule.valueOf(getName(), new ElementaryAssembler().assemble());
	// }

	/**
	 * Assembles the Pacts of the contained Sopremo operators and returns a list of all Pact sinks. These sinks may
	 * either be directly a {@link FileDataSinkContract} or an unconnected {@link Contract}.
	 * 
	 * @param context
	 *        the evaluation context of the Pact contracts
	 * @return a list of Pact sinks
	 */
	public Collection<Contract> assemblePact(final EvaluationContext context) {
		return new PactAssembler(context).assemble();
	}

	@Override
	public String toString() {
		final GraphPrinter<Operator<?>> graphPrinter = new GraphPrinter<Operator<?>>();
		graphPrinter.setWidth(40);
		return graphPrinter.toString(this.getAllOutputs(), OperatorNavigator.INSTANCE);
	}

	/**
	 * Wraps the graph given by the sinks and referenced contracts in a SopremoModule.
	 * 
	 * @param name
	 *        the name of the SopremoModule
	 * @param sinks
	 *        all sinks that span the graph to wrap
	 * @return a SopremoModule representing the given graph
	 */
	public static SopremoModule valueOf(final String name, final Collection<? extends Operator<?>> sinks) {
		final List<Operator<?>> inputs = new ArrayList<Operator<?>>();

		OneTimeTraverser.INSTANCE.traverse(sinks, OperatorNavigator.INSTANCE,
			new GraphTraverseListener<Operator<?>>() {
				@Override
				public void nodeTraversed(final Operator<?> node) {
					if (node instanceof Source)
						inputs.add(node);
					else
						for (final JsonStream input : node.getInputs())
							if (input == null)
								inputs.add(node);
				};
			});

		final SopremoModule module = new SopremoModule(name, inputs.size(), sinks.size());
		int sinkIndex = 0;
		for (final Operator<?> sink : sinks) {
			if (sink instanceof Sink)
				module.outputNodes[sinkIndex] = (Sink) sink;
			else
				module.getOutput(sinkIndex).setInput(0, sink);
			sinkIndex++;
		}

		for (int operatorIndex = 0, moduleIndex = 0; operatorIndex < inputs.size(); operatorIndex++) {
			final Operator<?> operator = inputs.get(operatorIndex);
			final List<JsonStream> operatorInputs = new ArrayList<JsonStream>(operator.getInputs());
			for (int inputIndex = 0; inputIndex < operatorInputs.size(); inputIndex++)
				if (operatorInputs.get(inputIndex) == null)
					operatorInputs.set(inputIndex, module.getInput(moduleIndex++).getOutput(0));
			operator.setInputs(operatorInputs);
		}
		return module;
	}

	/**
	 * Wraps the graph given by the sinks and referenced contracts in a SopremoModule.
	 * 
	 * @param name
	 *        the name of the SopremoModule
	 * @param sinks
	 *        all sinks that span the graph to wrap
	 * @return a SopremoModule representing the given graph
	 */
	public static SopremoModule valueOf(final String name, final Operator<?>... sinks) {
		return valueOf(name, Arrays.asList(sinks));
	}

	private final class ModuleOperator extends CompositeOperator<ModuleOperator> {
		/**
		 * 
		 */
		private static final long serialVersionUID = 632583661549969648L;

		private ModuleOperator(final int numberOfOutputs, final JsonStream[] inputs) {
			super(numberOfOutputs);
			this.setNumberOfInputs(inputs.length);
			this.setInputs(inputs);
		}

		@Override
		public SopremoModule asElementaryOperators() {
			return SopremoModule.this;
		}
	}

	/**
	 * Helper class needed to assemble a Pact program from the {@link PactModule}s of several {@link Operator<?>}s.
	 * 
	 * @author Arvid Heise
	 */
	private class PactAssembler {
		private final Map<Operator<?>, PactModule> modules = new IdentityHashMap<Operator<?>, PactModule>();

		private final Map<Operator<?>, Contract[]> operatorOutputs = new IdentityHashMap<Operator<?>, Contract[]>();

		private final EvaluationContext context;

		public PactAssembler(final EvaluationContext context) {
			this.context = context;
		}

		public Collection<Contract> assemble() {
			this.convertDAGToModules();

			this.connectModules();

			final List<Contract> pactSinks = this.findPACTSinks();

			return pactSinks;
		}

		private void connectModules() {
			for (final Entry<Operator<?>, PactModule> operatorModule : this.modules.entrySet()) {
				final Operator<?> operator = operatorModule.getKey();
				final PactModule module = operatorModule.getValue();

				for (final Contract contract : module.getReachableNodes()) {
					final Contract[] inputs = ContractUtil.getInputs(contract);
					for (int index = 0; index < inputs.length; index++)
						inputs[index] = this.findOutputtingPactInOperator(operator, inputs[index]);
					ContractUtil.setInputs(contract, inputs);
				}
			}
		}

		private void convertDAGToModules() {
			OneTimeTraverser.INSTANCE.traverse(SopremoModule.this.getAllOutputs(),
				OperatorNavigator.INSTANCE, new GraphTraverseListener<Operator<?>>() {
					@Override
					public void nodeTraversed(final Operator<?> node) {
						final PactModule module = node.asPactModule(PactAssembler.this.context);
						PactAssembler.this.modules.put(node, module);
						final FileDataSinkContract<?, ?>[] outputStubs = module.getOutputs();
						final Contract[] outputContracts = new Contract[outputStubs.length];
						for (int index = 0; index < outputStubs.length; index++)
							outputContracts[index] = outputStubs[index].getInput();
						PactAssembler.this.operatorOutputs.put(node, outputContracts);
					}
				});

			for (final PactModule module : this.modules.values())
				module.validate();
		}

		private Contract findOutputtingPactInOperator(final Operator<?> operator, final Contract o) {
			int inputIndex = -1;
			final FileDataSourceContract<?, ?>[] inputPacts = this.modules.get(operator).getInputs();
			for (int index = 0; index < inputPacts.length; index++)
				if (inputPacts[index] == o) {
					inputIndex = index;
					break;
				}

			if (inputIndex >= operator.getInputs().size() || inputIndex == -1)
				return o;
			Operator<?>.Output inputSource = operator.getInputs().get(inputIndex).getSource();
			Contract outputtingContract = this.operatorOutputs.get(inputSource.getOperator())[inputSource.getIndex()];
			if (outputtingContract instanceof FileDataSourceContract<?, ?>
				&& !(inputSource.getOperator() instanceof Source))
				outputtingContract = this.findOutputtingPactInOperator(inputSource.getOperator(), outputtingContract);
			return outputtingContract;
		}

		private List<Contract> findPACTSinks() {
			final List<Contract> pactSinks = new ArrayList<Contract>();
			for (final Operator<?> sink : SopremoModule.this.getAllOutputs()) {
				final FileDataSinkContract<?, ?>[] outputs = this.modules.get(sink).getAllOutputs();
				for (final FileDataSinkContract<?, ?> outputStub : outputs) {
					Contract output = outputStub;
					if (!(sink instanceof Sink))
						output = outputStub.getInput();
					pactSinks.add(output);
				}
			}
			return pactSinks;
		}
	}

	public SopremoModule asElementary() {
		return new ElementaryAssembler().assemble();
	}

	private class ElementaryAssembler {
		private final Map<Operator<?>, SopremoModule> modules = new IdentityHashMap<Operator<?>, SopremoModule>();

		// private final Map<Operator<?>, JsonStream[]> operatorOutputs = new IdentityHashMap<Operator<?>,
		// JsonStream[]>();

		public ElementaryAssembler() {
		}

		public SopremoModule assemble() {
			this.convertDAGToModules();

			this.connectModules();

			// Operator<?>[] outputs = getOutputs().clone();
			// List<Operator<?>> internal = new ArrayList<Operator<?>>(internalOutputNodes);
			// for (int i = 0; i < outputs.length; i++) {
			// outputs[i] = toElementary(outputs[i]);
			// }
			//
			// SopremoModule elementary = SopremoModule.valueOf(getName(), outputs);
			// for (int i = 0; i < internal.size(); i++) {
			// elementary.addInternalOutput((Sink) toElementary(internal.get(i)));
			// }
			return SopremoModule.this;
		}

		private void convertDAGToModules() {
			DependencyAwareGraphTraverser.INSTANCE.traverse(SopremoModule.this.getAllOutputs(),
				OperatorNavigator.INSTANCE, new GraphTraverseListener<Operator<?>>() {
					@Override
					public void nodeTraversed(final Operator<?> node) {
						SopremoModule elementaryModule = node.toElementaryOperators();
						ElementaryAssembler.this.modules.put(node, elementaryModule);
						// modules.put(node, new SopremoModule("test " + node.hashCode(), 0, 0));
					}
				});
		}

		private void connectModules() {
			for (final Entry<Operator<?>, SopremoModule> operatorModule : this.modules.entrySet()) {
				final Operator<?> operator = operatorModule.getKey();
				final SopremoModule module = operatorModule.getValue();

				final Map<JsonStream, JsonStream> operatorInputToModuleOutput = new IdentityHashMap<JsonStream, JsonStream>();

				for (int index = 0; index < operator.getInputs().size(); index++) {
					Operator<?>.Output inputSource = operator.getInput(index).getSource();
					final SopremoModule inputModule = this.modules.get(inputSource.getOperator());
					operatorInputToModuleOutput.put(module.getInput(0).getOutput(0),
						inputModule.getOutput(inputSource.getIndex()).getInput(0));
				}

				// final Source[] moduleInputs = module.getInputs();
				DependencyAwareGraphTraverser.INSTANCE.traverse(module.getAllOutputs(),
					OperatorNavigator.INSTANCE, new GraphTraverseListener<Operator<?>>() {
						@Override
						public void nodeTraversed(final Operator<?> innerNode) {
							List<JsonStream> innerNodeInputs = innerNode.getInputs();
							for (int index = 0; index < innerNodeInputs.size(); index++) {
								JsonStream moduleOutput = operatorInputToModuleOutput.get(innerNodeInputs
									.get(index));
								if (moduleOutput != null)
									innerNodeInputs.set(index, moduleOutput);
							}
							innerNode.setInputs(innerNodeInputs);
						}
					});
				// }
			}
		}
		// }
		//

		// public Collection<Operator<?>> assemble() {
		//
		// final List<Operator<?>> pactSinks = this.findPACTSinks();
		//
		// return pactSinks;
		// }
		//
		// private void connectModules() {
		// for (final Entry<Operator<?>, SopremoModule> operatorModule : this.modules.entrySet()) {
		//
		// for (final Operator<?> op : module.getReachableNodes()) {
		// final List<Output> inputs = op.getInputs();
		// for (int index = 0; index < inputs.size(); index++)
		// inputs.set(index, this.findOutputtingOperatorInModule(operator, inputs.get(index)));
		// op.setInputs(inputs);
		// }
		// }
		// }
		//
		//
		// private Output findOutputtingOperatorInModule(final Operator<?> operator, final Output output) {
		// int inputIndex = -1;
		// final Source[] inputs = this.modules.get(operator).getInputs();
		// for (int index = 0; index < inputs.length; index++)
		// if (inputs[index].getOutputs().contains(output)) {
		// inputIndex = index;
		// break;
		// }
		//
		// if (inputIndex >= operator.getInputs().size() || inputIndex == -1)
		// return output;
		// final Output input = operator.getInputs().get(inputIndex);
		// Output outputtingContract = this.operatorOutputs.get(input.getOperator())[input.getIndex()];
		// if (outputtingContract.getOperator() instanceof Source && !(input.getOperator() instanceof Source))
		// outputtingContract = this.findOutputtingOperatorInModule(input.getOperator(), outputtingContract);
		// return outputtingContract;
		// }
		//
		// private List<Operator<?>> findPACTSinks() {
		// final List<Operator<?>> pactSinks = new ArrayList<Operator<?>>();
		// for (final Operator<?> sink : SopremoModule.this.getAllOutputs()) {
		// final Sink[] outputs = this.modules.get(sink).getAllOutputs();
		// pactSinks.addAll(Arrays.asList(outputs));
		// }
		// return pactSinks;
		// }
		// }
	}
}
