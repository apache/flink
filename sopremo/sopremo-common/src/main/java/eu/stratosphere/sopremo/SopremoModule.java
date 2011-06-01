package eu.stratosphere.sopremo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.contract.DataSinkContract;
import eu.stratosphere.pact.common.contract.DataSourceContract;
import eu.stratosphere.pact.common.contract.DualInputContract;
import eu.stratosphere.pact.common.contract.SingleInputContract;
import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.sopremo.Operator.Output;
import eu.stratosphere.sopremo.base.DataType;
import eu.stratosphere.sopremo.base.Sink;
import eu.stratosphere.sopremo.base.Source;
import eu.stratosphere.util.dag.AbstractSubGraph;
import eu.stratosphere.util.dag.DependencyAwareGraphTraverser;
import eu.stratosphere.util.dag.GraphPrinter;
import eu.stratosphere.util.dag.GraphTraverseListener;
import eu.stratosphere.util.dag.OneTimeTraverser;
import eu.stratosphere.util.dag.SubGraph;

/**
 * Encapsulate a partial query in Sopremo and translates it to a Pact {@link Plan}.
 * 
 * @author Arvid Heise
 */
public class SopremoModule extends AbstractSubGraph<Operator, Source, Sink> {

	/**
	 * Initializes a SopremoModule having the given number of inputs and outputs.
	 * 
	 * @param numberOfInputs
	 *        the number of inputs
	 * @param numberOfOutputs
	 *        the number of outputs.
	 */
	public SopremoModule(int numberOfInputs, int numberOfOutputs) {
		super(new Source[numberOfInputs], new Sink[numberOfOutputs], OperatorNavigator.INSTANCE);
		for (int index = 0; index < this.outputNodes.length; index++)
			this.outputNodes[index] = new Sink(DataType.HDFS, String.valueOf(index), null);
		for (int index = 0; index < this.inputNodes.length; index++)
			this.inputNodes[index] = new Source(DataType.HDFS, String.valueOf(index));
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public Plan asPactPlan(EvaluationContext context) {
		return new Plan((Collection) this.assemblePact(context));
	}

	public Collection<Contract> assemblePact(EvaluationContext context) {
		return new PactAssembler(context).assemble();
	}

	private static Contract[] getInputs(Contract contract) {
		if (contract instanceof SingleInputContract)
			return new Contract[] { ((SingleInputContract<?, ?, ?, ?>) contract).getInput() };
		if (contract instanceof DualInputContract)
			return new Contract[] { ((DualInputContract<?, ?, ?, ?, ?, ?>) contract).getFirstInput(),
				((DualInputContract<?, ?, ?, ?, ?, ?>) contract).getSecondInput() };
		if (contract instanceof DataSinkContract<?, ?>)
			return new Contract[] { ((DataSinkContract<?, ?>) contract).getInput() };
		return new Contract[0];
	}

	private static void setInputs(Contract contract, Contract[] inputs) {
		if (contract instanceof SingleInputContract)
			((SingleInputContract<?, ?, ?, ?>) contract).setInput(inputs[0]);
		else if (contract instanceof DualInputContract) {
			((DualInputContract<?, ?, ?, ?, ?, ?>) contract).setFirstInput(inputs[0]);
			((DualInputContract<?, ?, ?, ?, ?, ?>) contract).setSecondInput(inputs[1]);
		} else if (contract instanceof DataSinkContract)
			((DataSinkContract<?, ?>) contract).setInput(inputs[0]);
	}

	@Override
	public String toString() {
		return new GraphPrinter<Operator>().toString(this.getAllOutputs(), OperatorNavigator.INSTANCE);
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
					Contract[] inputs = SopremoModule.getInputs(contract);
					for (int index = 0; index < inputs.length; index++)
						inputs[index] = this.findOutputtingPactInOperator(operator, inputs[index]);
					SopremoModule.setInputs(contract, inputs);
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

			for (SubGraph module : this.modules.values())
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

	public static SopremoModule valueOf(Operator... sinks) {

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


		SopremoModule module = new SopremoModule(inputs.size(), sinks.length);
		for (int index = 0; index < sinks.length; index++)
			if (sinks[index] instanceof Sink)
				module.outputNodes[index] = (Sink) sinks[index];
			else
				module.getOutput(index).setInput(0, sinks[index]);
		
		for (int operatorIndex = 0, moduleIndex = 0; operatorIndex < inputs.size(); operatorIndex++) {
			Operator operator = inputs.get(operatorIndex);
			List<Output> operatorInputs = operator.getInputs();
			for (int inputIndex = 0; inputIndex < sinks.length; inputIndex++) 
				if(operatorInputs.get(inputIndex) == null)
					operatorInputs.set(inputIndex, module.getInput(moduleIndex++).getOutput(0));
			operator.setInputs(operatorInputs);
		}
		return module;
	}
}
