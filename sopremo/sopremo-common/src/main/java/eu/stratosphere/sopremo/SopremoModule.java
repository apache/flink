package eu.stratosphere.sopremo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.plan.ContractUtil;
import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.serialization.Schema;
import eu.stratosphere.sopremo.serialization.SchemaFactory;
import eu.stratosphere.util.CollectionUtil;
import eu.stratosphere.util.ConversionIterable;
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

		private final Map<Operator<?>, List<List<Contract>>> operatorOutputs =
			new IdentityHashMap<Operator<?>, List<List<Contract>>>();

		private final EvaluationContext context;

		public PactAssembler(final EvaluationContext context) {
			this.context = new EvaluationContext(context);
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
					final List<List<Contract>> inputLists = ContractUtil.getInputs(contract);
					for (int listIndex = 0; listIndex < inputLists.size(); listIndex++) {
						final List<Contract> connectedInputs = new ArrayList<Contract>();
						final List<Contract> inputs = inputLists.get(listIndex);
						for (int inputIndex = 0; inputIndex < inputs.size(); inputIndex++)
							this.addOutputtingPactInOperator(operator, inputs.get(inputIndex), connectedInputs);
						inputLists.set(listIndex, connectedInputs);
					}
					ContractUtil.setInputs(contract, inputLists);
				}
			}
		}

		private void convertDAGToModules() {
//			final Schema schema = getSchema();
			OneTimeTraverser.INSTANCE.traverse(SopremoModule.this.getAllOutputs(),
				OperatorNavigator.INSTANCE, new GraphTraverseListener<Operator<?>>() {
					@Override
					public void nodeTraversed(final Operator<?> node) {
						// TODO: set schema
//						PactAssembler.this.context.setSchema(schema);
						final PactModule module = node.asPactModule(PactAssembler.this.context);
						PactAssembler.this.modules.put(node, module);
						final FileDataSink[] outputStubs = module.getOutputs();
						final List<List<Contract>> outputContracts = new ArrayList<List<Contract>>();
						for (int index = 0; index < outputStubs.length; index++)
							outputContracts.add(outputStubs[index].getInputs());
						PactAssembler.this.operatorOutputs.put(node, outputContracts);
					}
				});

			for (final PactModule module : this.modules.values())
				module.validate();
		}

		private void addOutputtingPactInOperator(final Operator<?> operator, final Contract o,
				final List<Contract> connectedInputs) {
			int inputIndex = -1;
			final FileDataSource[] inputPacts = this.modules.get(operator).getInputs();
			for (int index = 0; index < inputPacts.length; index++)
				if (inputPacts[index] == o) {
					inputIndex = index;
					break;
				}

			if (inputIndex >= operator.getInputs().size() || inputIndex == -1) {
				connectedInputs.add(o);
				return;
			}

			final Operator<?>.Output inputSource = operator.getInputs().get(inputIndex).getSource();
			final List<Contract> outputtingContracts = this.operatorOutputs.get(inputSource.getOperator()).get(
				inputSource.getIndex());
			for (final Contract outputtingContract : outputtingContracts)
				if (outputtingContract instanceof FileDataSource && !(inputSource.getOperator() instanceof Source))
					this.addOutputtingPactInOperator(inputSource.getOperator(), outputtingContract, connectedInputs);
				else
					connectedInputs.add(outputtingContract);
		}

		private List<Contract> findPACTSinks() {
			final List<Contract> pactSinks = new ArrayList<Contract>();
			for (final Operator<?> sink : SopremoModule.this.getAllOutputs()) {
				final FileDataSink[] outputs = this.modules.get(sink).getAllOutputs();
				for (final FileDataSink outputStub : outputs)
					if (sink instanceof Sink)
						pactSinks.add(outputStub);
					else
						pactSinks.addAll(outputStub.getInputs());
			}
			return pactSinks;
		}
	}

	public SopremoModule asElementary() {
		return new ElementaryAssembler().assemble();
	}

	private class ElementaryAssembler {
		private final Map<Operator<?>, SopremoModule> modules = new IdentityHashMap<Operator<?>, SopremoModule>();

		public ElementaryAssembler() {
		}

		public SopremoModule assemble() {
			this.convertDAGToModules();

			this.connectModules();

			return SopremoModule.this;
		}

		private void convertDAGToModules() {
			DependencyAwareGraphTraverser.INSTANCE.traverse(SopremoModule.this.getAllOutputs(),
				OperatorNavigator.INSTANCE, new GraphTraverseListener<Operator<?>>() {
					@Override
					public void nodeTraversed(final Operator<?> node) {
						final SopremoModule elementaryModule = node.toElementaryOperators();
						ElementaryAssembler.this.modules.put(node, elementaryModule);
					}
				});
		}

		private void connectModules() {
			for (final Entry<Operator<?>, SopremoModule> operatorModule : this.modules.entrySet()) {
				final Operator<?> operator = operatorModule.getKey();
				final SopremoModule module = operatorModule.getValue();

				final Map<JsonStream, JsonStream> operatorInputToModuleOutput =
					new IdentityHashMap<JsonStream, JsonStream>();

				for (int index = 0; index < operator.getInputs().size(); index++) {
					final Operator<?>.Output inputSource = operator.getInput(index).getSource();
					final SopremoModule inputModule = this.modules.get(inputSource.getOperator());
					operatorInputToModuleOutput.put(module.getInput(0).getOutput(0),
						inputModule.getOutput(inputSource.getIndex()).getInput(0));
				}

				DependencyAwareGraphTraverser.INSTANCE.traverse(module.getAllOutputs(),
					OperatorNavigator.INSTANCE, new GraphTraverseListener<Operator<?>>() {
						@Override
						public void nodeTraversed(final Operator<?> innerNode) {
							final List<JsonStream> innerNodeInputs = innerNode.getInputs();
							for (int index = 0; index < innerNodeInputs.size(); index++) {
								final JsonStream moduleOutput = operatorInputToModuleOutput.get(innerNodeInputs
									.get(index));
								if (moduleOutput != null)
									innerNodeInputs.set(index, moduleOutput);
							}
							innerNode.setInputs(innerNodeInputs);
						}
					});
			}
		}
	}

	/**
	 * @return
	 */
	public Schema getSchema(SchemaFactory schemaFactory) {
		Iterable<EvaluationExpression> keyExpressions =
			CollectionUtil.mergeUnique(new ConversionIterable<Operator<?>, Iterable<? extends EvaluationExpression>>(
				getReachableNodes()) {
				/*
				 * (non-Javadoc)
				 * @see eu.stratosphere.util.ConversionIterable#convert(java.lang.Object)
				 */
				@Override
				protected Iterable<? extends EvaluationExpression> convert(Operator<?> operator) {
					return operator.getKeyExpressions();
				}
			});
		return schemaFactory.create(keyExpressions);
	}
}
