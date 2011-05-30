package eu.stratosphere.sopremo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import eu.stratosphere.dag.GraphPrinter;
import eu.stratosphere.dag.GraphTraverseListener;
import eu.stratosphere.dag.DependencyAwareGraphTraverser;
import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.contract.DataSinkContract;
import eu.stratosphere.pact.common.contract.DataSourceContract;
import eu.stratosphere.pact.common.contract.DualInputContract;
import eu.stratosphere.pact.common.contract.SingleInputContract;
import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.type.base.PactJsonObject;
import eu.stratosphere.pact.common.type.base.PactNull;
import eu.stratosphere.sopremo.Operator.Output;
import eu.stratosphere.sopremo.base.Sink;
import eu.stratosphere.sopremo.base.Source;
import eu.stratosphere.sopremo.function.BuiltinFunctions;

public class SopremoPlan {
	public class PactAssembler {
		final Map<Operator, PactModule> modules = new IdentityHashMap<Operator, PactModule>();

		final Map<Operator, Contract[]> operatorOutputs = new IdentityHashMap<Operator, Contract[]>();

		public PactAssembler() {
		}

		public Collection<Contract> assemble() {
			convertDAGToModules();

			connectModules();

			List<Contract> pactSinks = findPACTSinks();

			System.out.println(PactModule.valueOf(pactSinks.toArray(new Contract[0])));

			return pactSinks;
		}

		private void connectModules() {
			for (Entry<Operator, PactModule> operatorModule : modules.entrySet()) {
				Operator operator = operatorModule.getKey();
				PactModule module = operatorModule.getValue();

				Collection<Contract> contracts = module.getAllContracts();
				for (Contract contract : contracts) {
					Contract[] inputs = getInputs(contract);
					for (int index = 0; index < inputs.length; index++)
						inputs[index] = findOutputtingPactInOperator(operator, inputs[index]);
					setInputs(contract, inputs);
				}
			}

			// for (Entry<Operator, PactModule> operatorModule : modules.entrySet()) {
			// Operator operator = operatorModule.getKey();
			// PactModule module = operatorModule.getValue();
			// List<DataSourceContract<PactNull, PactJsonObject>> moduleInputs = Arrays.asList(module.getInputStubs());
			//
			// Collection<Contract> contracts = module.getAllContracts();
			// for (Contract contract : contracts) {
			// Contract[] inputs = this.getInputs(contract);
			// for (int index = 0; index < inputs.length; index++) {
			// int inputIndex = moduleInputs.indexOf(inputs[index]);
			// if (inputIndex != -1 && inputIndex < operator.getInputs().size()) {
			// Output input = operator.getInputs().get(inputIndex);
			// inputs[index] = operatorOutputs.get(input.getOperator())[input.getIndex()];
			// }
			// }
			// this.setInputs(contract, inputs);
			// }
			// }
		}

		private Contract findOutputtingPactInOperator(Operator operator, Contract o) {
			int inputIndex = -1;
			DataSourceContract<?, ?>[] inputPacts = modules.get(operator).getInputs();
			for (int index = 0; index < inputPacts.length; index++)
				if (inputPacts[index] == o) {
					inputIndex = index;
					break;
				}

			if (inputIndex >= operator.getInputs().size() || inputIndex == -1)
				return o;
			Output input = operator.getInputs().get(inputIndex);
			Contract outputtingContract = operatorOutputs.get(input.getOperator())[input.getIndex()];
			if (outputtingContract instanceof DataSourceContract<?, ?> && !(input.getOperator() instanceof Source)) {
				outputtingContract = findOutputtingPactInOperator(input.getOperator(), outputtingContract);
			}
			return outputtingContract;
		}

		private List<Contract> findPACTSinks() {
			List<Contract> pactSinks = new ArrayList<Contract>();
			for (Operator sink : sinks) {
				DataSinkContract<?, ?>[] outputs = modules.get(sink).getAllOutputs();
				for (DataSinkContract<?, ?> outputStub : outputs) {
					Contract output = outputStub;
					if (!(sink instanceof Sink))
						output = outputStub.getInput();
					pactSinks.add(output);
				}
			}
			return pactSinks;
		}

		private void convertDAGToModules() {
			DependencyAwareGraphTraverser.INSTANCE.traverse(sinks, OperatorNavigator.INSTANCE,
				new GraphTraverseListener<Operator>() {
					@Override
					public void nodeTraversed(Operator node) {
						PactModule module = node.asPactModule(SopremoPlan.this.context);
						modules.put(node, module);
						DataSinkContract<?, ?>[] outputStubs = module.getOutputs();
						Contract[] outputContracts = new Contract[outputStubs.length];
						for (int index = 0; index < outputStubs.length; index++)
							outputContracts[index] = outputStubs[index].getInput();
						operatorOutputs.put(node, outputContracts);
					}
				});

			for (PactModule module : modules.values())
				module.validate();
		}

	}

	private Collection<Operator> sinks;

	private EvaluationContext context = new EvaluationContext();

	public SopremoPlan(Operator... sinks) {
		this(Arrays.asList(sinks));
	}

	public SopremoPlan(Collection<Operator> sinks) {
		this.sinks = sinks;
		this.context.getFunctionRegistry().register(BuiltinFunctions.class);
	}

	public Collection<Operator> getSinks() {
		return this.sinks;
	}

	@Override
	public String toString() {
		return new GraphPrinter<Operator>().toString(sinks, OperatorNavigator.INSTANCE);
	}

	public List<Operator> getAllNodes() {
		final List<Operator> nodes = new ArrayList<Operator>();
		DependencyAwareGraphTraverser.INSTANCE.traverse(this.sinks, new OperatorNavigator(),
			new GraphTraverseListener<Operator>() {
				@Override
				public void nodeTraversed(Operator node) {
					nodes.add(node);
				}
			});
		return nodes;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public Plan asPactPlan() {
		return new Plan((Collection) this.assemblePact());
	}

	Collection<Contract> assemblePact() {
		return new PactAssembler().assemble();
	}

	private Contract[] getInputs(Contract contract) {
		if (contract instanceof SingleInputContract)
			return new Contract[] { ((SingleInputContract<?, ?, ?, ?>) contract).getInput() };
		if (contract instanceof DualInputContract)
			return new Contract[] { ((DualInputContract<?, ?, ?, ?, ?, ?>) contract).getFirstInput(),
				((DualInputContract<?, ?, ?, ?, ?, ?>) contract).getSecondInput() };
		if (contract instanceof DataSinkContract<?, ?>)
			return new Contract[] { ((DataSinkContract<?, ?>) contract).getInput() };
		return new Contract[0];
	}

	private void setInputs(Contract contract, Contract[] inputs) {
		if (contract instanceof SingleInputContract)
			((SingleInputContract<?, ?, ?, ?>) contract).setInput(inputs[0]);
		else if (contract instanceof DualInputContract) {
			((DualInputContract<?, ?, ?, ?, ?, ?>) contract).setFirstInput(inputs[0]);
			((DualInputContract<?, ?, ?, ?, ?, ?>) contract).setSecondInput(inputs[1]);
		} else if (contract instanceof DataSinkContract)
			((DataSinkContract<?, ?>) contract).setInput(inputs[0]);
	}

	public static void main(String[] args) throws IOException {
		// Operator a = new Operator("A");
		// Operator c = new Operator("C", a, new Operator("B"));
		// Operator e = new Operator("E", c, new Operator("D"));
		// Operator f = new Operator("F", c, e, a);
		// Plan plan = new Plan(f);
		//
		// new PlanPrinter(plan).print(System.out, 10);
	}

	// public static class Source extends Node {
	// public Source() {
	// super();
	// }
	// }
	//
	// public static class Sink extends Node {
	// public Sink(Node... inputs) {
	// super(inputs);
	// }
	// }
	//
	// public static class
}
