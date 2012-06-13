package eu.stratosphere.sopremo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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
		return new ModuleOperator(this.getInputs(), this.getOutputs());
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
		final List<Operator<?>> inputs = findInputs(sinks);
		final SopremoModule module = new SopremoModule(name, inputs.size(), sinks.size());
		connectOutputs(module, sinks);
		connectInputs(module, inputs);
		return module;
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
				module.outputNodes[sinkIndex] = (Sink) sink;
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
		public ModuleOperator(Source[] inputs, Sink[] outputs) {
			super(inputs.length, outputs.length);
			this.setInputs(inputs);
			this.setOutputs(outputs);
		}

		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.sopremo.Operator#toElementaryOperators()
		 */
		@Override
		public ElementarySopremoModule asElementaryOperators() {
			return SopremoModule.this.asElementary();
		}
	}

	public ElementarySopremoModule asElementary() {
		return new ElementaryAssembler().assemble(this);
	}

	private static class ElementaryAssembler {
		private final Map<Operator<?>, ElementarySopremoModule> modules =
			new IdentityHashMap<Operator<?>, ElementarySopremoModule>();

		public ElementaryAssembler() {
		}

		public ElementarySopremoModule assemble(SopremoModule sopremoModule) {
			this.convertDAGToModules(sopremoModule);

			final int sinkCount = sopremoModule.getOutputs().length;
			final int sourceCount = sopremoModule.getInputs().length;
			final ElementarySopremoModule elementarySopremoModule =
				new ElementarySopremoModule(sopremoModule.getName(), sourceCount, sinkCount);
			// replace sources
			for (int sourceIndex = 0; sourceIndex < sourceCount; sourceIndex++)
				this.modules.get(sopremoModule.getInput(sourceIndex)).getOutput(0)
					.setInput(0, elementarySopremoModule.getInput(sourceIndex));

			this.connectModules();

			for (int sinkIndex = 0; sinkIndex < sinkCount; sinkIndex++)
				elementarySopremoModule.getOutput(sinkIndex).setInput(0,
					this.modules.get(sopremoModule.getOutput(sinkIndex)).getInternalOutputNodes(0).getInput(0));
			for (Sink sink : sopremoModule.getInternalOutputNodes())
				elementarySopremoModule.addInternalOutput(this.modules.get(sink).getInternalOutputNodes(0));

			return elementarySopremoModule;
		}

		private void convertDAGToModules(SopremoModule sopremoModule) {
			OneTimeTraverser.INSTANCE.traverse(sopremoModule.getAllOutputs(),
				OperatorNavigator.INSTANCE, new GraphTraverseListener<Operator<?>>() {
					@Override
					public void nodeTraversed(final Operator<?> node) {
						final ElementarySopremoModule elementaryModule = node.asElementaryOperators();
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
					final JsonStream input = traceInput(operator, index);
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

		protected JsonStream traceInput(Operator<?> operator, int index) {
			final Operator<?>.Output inputSource = operator.getInput(index).getSource();
			final ElementarySopremoModule inputModule = this.modules.get(inputSource.getOperator());
			final JsonStream input = inputModule.getOutput(inputSource.getIndex()).getInput(0);
			final Operator<?> inputOperator = input.getSource().getOperator();
			// check if the given output is directly connected to an input of the module
			if (inputOperator instanceof Source) {
				final Source[] inputs = inputModule.getInputs();
				for (int i = 0; i < inputs.length; i++)
					if (inputOperator == inputs[i]) {
						final JsonStream inputStream = operator.getInput(index);
						return traceInput(inputStream.getSource().getOperator(), inputStream.getSource().getIndex());
					}
			}
			return input;
		}
	}
}
