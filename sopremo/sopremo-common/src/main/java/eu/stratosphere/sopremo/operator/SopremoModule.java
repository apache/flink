package eu.stratosphere.sopremo.operator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.ISerializableSopremoType;
import eu.stratosphere.sopremo.io.Sink;
import eu.stratosphere.sopremo.io.Source;
import eu.stratosphere.util.dag.GraphModule;
import eu.stratosphere.util.dag.GraphPrinter;
import eu.stratosphere.util.dag.GraphTraverseListener;
import eu.stratosphere.util.dag.OneTimeTraverser;

/**
 * Encapsulate a partial query in Sopremo and translates it to a {@link PactModule}.
 * 
 * @author Arvid Heise
 */
public class SopremoModule extends GraphModule<Operator<?>, Source, Sink> implements ISerializableSopremoType {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5038229456879714912L;

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
		super(name, numberOfInputs, numberOfOutputs, OperatorNavigator.INSTANCE);
		for (int index = 0; index < numberOfInputs; index++)
			setInput(index, new Source(String.format("%s %d", name, index)));
		for (int index = 0; index < numberOfOutputs; index++)
			setOutput(index, new Sink(String.format("%s %d", name, index)));
	}

	/**
	 * Allows to embed this module in a graph of Sopremo operators.
	 * 
	 * @return an operator view of this SopremoModule
	 */
	public Operator<?> asOperator() {
		return new ModuleOperator(this.getInputs(), this.getOutputs());
	}

	@Override
	public String toString() {
		final GraphPrinter<Operator<?>> graphPrinter = new GraphPrinter<Operator<?>>();
		graphPrinter.setWidth(40);
		return graphPrinter.toString(this.getAllOutputs(), OperatorNavigator.INSTANCE);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.ISopremoType#toString(java.lang.StringBuilder)
	 */
	@Override
	public void toString(StringBuilder builder) {
		builder.append(toString());
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
		final List<Operator<?>> inputs = findInputs(sinks);
		final SopremoModule module = new SopremoModule(name, inputs.size(), sinks.size());
		connectOutputs(module, sinks);
		connectInputs(module, inputs);
		return module;
	}

	public void embed(final Operator<?>... sinks) {
		embed(Arrays.asList(sinks));
	}
	
	public void embed(final Collection<? extends Operator<?>> sinks) {
		final List<Operator<?>> inputs = findInputs(sinks);
		if (inputs.size() != getNumInputs())
			throw new IllegalArgumentException(String.format("Expected %d instead of %d inputs", getNumInputs(),
					inputs.size()));
		connectOutputs(this, sinks);
		connectInputs(this, inputs);
	}

	protected static void connectInputs(final SopremoModule module, final List<Operator<?>> inputs) {
		for (int operatorIndex = 0, moduleIndex = 0; operatorIndex < inputs.size(); operatorIndex++) {
			final Operator<?> operator = inputs.get(operatorIndex);
			final List<JsonStream> operatorInputs = new ArrayList<JsonStream>(operator.getInputs());
			for (int inputIndex = 0; inputIndex < operatorInputs.size(); inputIndex++)
				if (operatorInputs.get(inputIndex) == null)
					operatorInputs.set(inputIndex, module.getInput(moduleIndex++).getOutput(0));
			operator.setInputs(operatorInputs);
		}
	}

	protected static void connectOutputs(final SopremoModule module, final Collection<? extends Operator<?>> sinks) {
		int sinkIndex = 0;
		for (final Operator<?> sink : sinks) {
			if (sink instanceof Sink)
				module.setOutput(sinkIndex, (Sink) sink);
			else
				module.getOutput(sinkIndex).setInput(0, sink);
			sinkIndex++;
		}
	}

	protected static List<Operator<?>> findInputs(final Collection<? extends Operator<?>> sinks) {
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
		return inputs;
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

		/**
		 * Initializes ModuleOperator.
		 * 
		 * @param inputs
		 * @param outputs
		 */
		public ModuleOperator(final List<Source> inputs, final List<Sink> outputs) {
			super(inputs.size(), outputs.size());
			this.setInputs(inputs);
			this.setOutputs(outputs);
		}
		
		@Override
		public void addImplementation(SopremoModule module, EvaluationContext context) {
			module.inputNodes.addAll(inputNodes);
			module.outputNodes.addAll(outputNodes);
			module.internalOutputNodes.addAll(internalOutputNodes);
		}
	}

	public ElementarySopremoModule asElementary(final EvaluationContext context) {
		return new ElementaryAssembler(context).assemble(this);
	}

	private static class ElementaryAssembler {
		private final Map<Operator<?>, ElementarySopremoModule> modules =
			new IdentityHashMap<Operator<?>, ElementarySopremoModule>();

		private final EvaluationContext context;

		public ElementaryAssembler(final EvaluationContext context) {
			this.context = context;
		}

		public ElementarySopremoModule assemble(final SopremoModule sopremoModule) {
			this.convertDAGToModules(sopremoModule);

			final int sinkCount = sopremoModule.getNumOutputs();
			final int sourceCount = sopremoModule.getNumInputs();
			final ElementarySopremoModule elementarySopremoModule =
				new ElementarySopremoModule(sopremoModule.getName(), sourceCount, sinkCount);
			// replace sources
			for (int sourceIndex = 0; sourceIndex < sourceCount; sourceIndex++) {
				final ElementarySopremoModule connectedInput = this.modules.get(sopremoModule.getInput(sourceIndex));
				// input has not been connect
				if (connectedInput == null)
					continue;
				connectedInput.getOutput(0).setInput(0, elementarySopremoModule.getInput(sourceIndex));
			}

			this.connectModules();

			for (int sinkIndex = 0; sinkIndex < sinkCount; sinkIndex++)
				elementarySopremoModule.getOutput(sinkIndex).setInput(0,
					this.modules.get(sopremoModule.getOutput(sinkIndex)).getInternalOutputNodes(0).getInput(0));
			for (final Sink sink : sopremoModule.getInternalOutputNodes())
				elementarySopremoModule.addInternalOutput(this.modules.get(sink).getInternalOutputNodes(0));

			return elementarySopremoModule;
		}

		private void convertDAGToModules(final SopremoModule sopremoModule) {
			OneTimeTraverser.INSTANCE.traverse(sopremoModule.getAllOutputs(),
				OperatorNavigator.INSTANCE, new GraphTraverseListener<Operator<?>>() {
					@Override
					public void nodeTraversed(final Operator<?> node) {
						final ElementarySopremoModule elementaryModule = node
							.asElementaryOperators(ElementaryAssembler.this.context);
						ElementaryAssembler.this.modules.put(node, elementaryModule);
					}
				});
		}

		private void connectModules() {
			for (final Entry<Operator<?>, ElementarySopremoModule> operatorModule : this.modules.entrySet()) {
				final Operator<?> operator = operatorModule.getKey();
				final ElementarySopremoModule module = operatorModule.getValue();

				final Map<JsonStream, JsonStream> operatorInputToModuleOutput =
					new IdentityHashMap<JsonStream, JsonStream>();

				for (int index = 0; index < operator.getInputs().size(); index++) {
					final JsonStream input = this.traceInput(operator, index);
					operatorInputToModuleOutput.put(module.getInput(index).getOutput(0), input);
				}

				OneTimeTraverser.INSTANCE.traverse(module.getAllOutputs(),
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

		protected JsonStream traceInput(final Operator<?> operator, final int index) {
			final Operator<?>.Output inputSource = operator.getInput(index).getSource();
			final ElementarySopremoModule inputModule = this.modules.get(inputSource.getOperator());
			final JsonStream input = inputModule.getOutput(inputSource.getIndex()).getInput(0);
			final Operator<?> inputOperator = input.getSource().getOperator();
			// check if the given output is directly connected to an input of the module
			if (inputOperator instanceof Source) {
				final List<Source> inputs = inputModule.getInputs();
				for (int i = 0; i < inputs.size(); i++)
					if (inputOperator == inputs.get(i)) {
						final JsonStream inputStream = operator.getInput(index);
						return this.traceInput(inputStream.getSource().getOperator(),
							inputStream.getSource().getIndex());
					}
			}
			return input;
		}
	}
}
