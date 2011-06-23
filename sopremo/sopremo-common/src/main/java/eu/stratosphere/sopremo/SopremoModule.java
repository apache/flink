package eu.stratosphere.sopremo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.contract.DataSinkContract;
import eu.stratosphere.pact.common.contract.DataSourceContract;
import eu.stratosphere.pact.common.plan.ContractUtil;
import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.sopremo.Operator.Output;
import eu.stratosphere.sopremo.base.PersistenceType;
import eu.stratosphere.sopremo.base.Sink;
import eu.stratosphere.sopremo.base.Source;
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
public class SopremoModule extends GraphModule<Operator, Source, Sink> {

	/**
	 * Initializes a SopremoModule having the given number of inputs and outputs.
	 * 
	 * @param numberOfInputs
	 *        the number of inputs
	 * @param numberOfOutputs
	 *        the number of outputs.
	 */
	public SopremoModule(String name, int numberOfInputs, int numberOfOutputs) {
		super(name, new Source[numberOfInputs], new Sink[numberOfOutputs], OperatorNavigator.INSTANCE);
		for (int index = 0; index < this.outputNodes.length; index++)
			this.outputNodes[index] = new Sink(PersistenceType.HDFS, String.format("%s %d", name, index), null);
		for (int index = 0; index < this.inputNodes.length; index++)
			this.inputNodes[index] = new Source(PersistenceType.HDFS, String.format("%s %d", name, index));
	}

	/**
	 * Converts the Sopremo module to a Pact module.
	 * 
	 * @param context
	 *        the evaluation context of the Pact contracts
	 * @return the converted Pact module
	 */
	public PactModule asPactModule(EvaluationContext context) {
		return PactModule.valueOf(getName(), this.assemblePact(context));
	}

	/**
	 * Assembles the Pacts of the contained Sopremo operators and returns a list of all Pact sinks. These sinks may
	 * either be directly a {@link DataSinkContract} or an unconnected {@link Contract}.
	 * 
	 * @param context
	 *        the evaluation context of the Pact contracts
	 * @return a list of Pact sinks
	 */
	public Collection<Contract> assemblePact(EvaluationContext context) {
		return new PactAssembler(context).assemble();
	}

	public Operator asOperator() {
		return new ModuleOperator(getOutputs().length, getInputs());
	}

	@Override
	public String toString() {
		GraphPrinter<Operator> graphPrinter = new GraphPrinter<Operator>();
		graphPrinter.setWidth(40);
		return graphPrinter.toString(this.getAllOutputs(), OperatorNavigator.INSTANCE);
	}

	/**
	 * Wraps the graph given by the sinks and referenced contracts in a SopremoModule.
	 * 
	 * @param sinks
	 *        all sinks that span the graph to wrap
	 * @return a SopremoModule representing the given graph
	 */
	public static SopremoModule valueOf(String name, Collection<Operator> sinks) {
		final List<Operator> inputs = new ArrayList<Operator>();

		OneTimeTraverser.INSTANCE.traverse(sinks, OperatorNavigator.INSTANCE,
			new GraphTraverseListener<Operator>() {
				@Override
				public void nodeTraversed(Operator node) {
					if (node instanceof Source)
						inputs.add(node);
					else
						for (Operator.Output output : node.getInputs())
							if (output == null)
								inputs.add(node);
				};
			});

		SopremoModule module = new SopremoModule(name, inputs.size(), sinks.size());
		int sinkIndex = 0;
		for (Operator sink : sinks) {
			if (sink instanceof Sink)
				module.outputNodes[sinkIndex] = (Sink) sink;
			else
				module.getOutput(sinkIndex).setInput(0, sink);
			sinkIndex++;
		}

		for (int operatorIndex = 0, moduleIndex = 0; operatorIndex < inputs.size(); operatorIndex++) {
			Operator operator = inputs.get(operatorIndex);
			List<Output> operatorInputs = operator.getInputs();
			for (int inputIndex = 0; inputIndex < sinks.size(); inputIndex++)
				if (operatorInputs.get(inputIndex) == null)
					operatorInputs.set(inputIndex, module.getInput(moduleIndex++).getOutput(0));
			operator.setInputs(operatorInputs);
		}
		return module;
	}

	/**
	 * Wraps the graph given by the sinks and referenced contracts in a SopremoModule.
	 * 
	 * @param sinks
	 *        all sinks that span the graph to wrap
	 * @return a SopremoModule representing the given graph
	 */
	public static SopremoModule valueOf(String name, Operator... sinks) {
		return valueOf(name, Arrays.asList(sinks));
	}

	private final class ModuleOperator extends CompositeOperator {
		/**
		 * 
		 */
		private static final long serialVersionUID = 632583661549969648L;

		private ModuleOperator(int numberOfOutputs, JsonStream[] inputs) {
			super(numberOfOutputs, inputs);
		}

		@Override
		public SopremoModule asElementaryOperators() {
			return SopremoModule.this;
		}
	}

	/**
	 * Helper class needed to assemble a Pact program from the {@link PactModule}s of several {@link Operator}s.
	 * 
	 * @author Arvid Heise
	 */
	private class PactAssembler {
		private final Map<Operator, PactModule> modules = new IdentityHashMap<Operator, PactModule>();

		private final Map<Operator, Contract[]> operatorOutputs = new IdentityHashMap<Operator, Contract[]>();

		private final EvaluationContext context;

		public PactAssembler(EvaluationContext context) {
			this.context = context;
		}

		public Collection<Contract> assemble() {
			this.convertDAGToModules();

			this.connectModules();

			List<Contract> pactSinks = this.findPACTSinks();

			return pactSinks;
		}

		private void connectModules() {
			for (Entry<Operator, PactModule> operatorModule : this.modules.entrySet()) {
				Operator operator = operatorModule.getKey();
				PactModule module = operatorModule.getValue();

				for (Contract contract : module.getReachableNodes()) {
					Contract[] inputs = ContractUtil.getInputs(contract);
					for (int index = 0; index < inputs.length; index++)
						inputs[index] = this.findOutputtingPactInOperator(operator, inputs[index]);
					ContractUtil.setInputs(contract, inputs);
				}
			}
		}

		private void convertDAGToModules() {
			DependencyAwareGraphTraverser.INSTANCE.traverse(SopremoModule.this.getAllOutputs(),
				OperatorNavigator.INSTANCE, new GraphTraverseListener<Operator>() {
					@Override
					public void nodeTraversed(Operator node) {
						PactModule module = node.asPactModule(PactAssembler.this.context);
						PactAssembler.this.modules.put(node, module);
						DataSinkContract<?, ?>[] outputStubs = module.getOutputs();
						Contract[] outputContracts = new Contract[outputStubs.length];
						for (int index = 0; index < outputStubs.length; index++)
							outputContracts[index] = outputStubs[index].getInput();
						PactAssembler.this.operatorOutputs.put(node, outputContracts);
					}
				});

			for (PactModule module : this.modules.values())
				module.validate();
		}

		private Contract findOutputtingPactInOperator(Operator operator, Contract o) {
			int inputIndex = -1;
			DataSourceContract<?, ?>[] inputPacts = this.modules.get(operator).getInputs();
			for (int index = 0; index < inputPacts.length; index++)
				if (inputPacts[index] == o) {
					inputIndex = index;
					break;
				}

			if (inputIndex >= operator.getInputs().size() || inputIndex == -1)
				return o;
			Output input = operator.getInputs().get(inputIndex);
			Contract outputtingContract = this.operatorOutputs.get(input.getOperator())[input.getIndex()];
			if (outputtingContract instanceof DataSourceContract<?, ?> && !(input.getOperator() instanceof Source))
				outputtingContract = this.findOutputtingPactInOperator(input.getOperator(), outputtingContract);
			return outputtingContract;
		}

		private List<Contract> findPACTSinks() {
			List<Contract> pactSinks = new ArrayList<Contract>();
			for (Operator sink : SopremoModule.this.getAllOutputs()) {
				DataSinkContract<?, ?>[] outputs = this.modules.get(sink).getAllOutputs();
				for (DataSinkContract<?, ?> outputStub : outputs) {
					Contract output = outputStub;
					if (!(sink instanceof Sink))
						output = outputStub.getInput();
					pactSinks.add(output);
				}
			}
			return pactSinks;
		}
	}
}
