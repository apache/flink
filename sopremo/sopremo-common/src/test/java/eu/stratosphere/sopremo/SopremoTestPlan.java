package eu.stratosphere.sopremo;

import eu.stratosphere.pact.common.contract.DataSinkContract;
import eu.stratosphere.pact.common.contract.DataSourceContract;
import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.testing.TestPairs;
import eu.stratosphere.pact.testing.TestPlan;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.base.PersistenceType;
import eu.stratosphere.sopremo.base.Sink;
import eu.stratosphere.sopremo.base.Source;
import eu.stratosphere.sopremo.pact.PactJsonObject;

public class SopremoTestPlan {
	public static class MockupSource extends Source {

		private int index;

		public MockupSource(int index) {
			super(PersistenceType.HDFS, "mockup-input" + index);
			this.index = index;
		}

		@Override
		public PactModule asPactModule(EvaluationContext context) {
			PactModule pactModule = new PactModule(toString(), 0, 1);
			DataSourceContract contract = TestPlan.createDefaultSource(this.getInputName());
			pactModule.getOutput(0).setInput(contract);
			// pactModule.setInput(0, contract);
			return pactModule;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = super.hashCode();
			result = prime * result + this.index;
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (!super.equals(obj))
				return false;
			if (this.getClass() != obj.getClass())
				return false;
			MockupSource other = (MockupSource) obj;
			return this.index == other.index;
		}

		@Override
		public String toString() {
			return String.format("MockupSource [%s]", index);
		}
	}

	public static class MockupSink extends Sink {
		private int index;

		public MockupSink(int index) {
			super(PersistenceType.ADHOC, "mockup-output" + index, null);
			this.index = index;
		}

		@Override
		public PactModule asPactModule(EvaluationContext context) {
			PactModule pactModule = new PactModule(toString(), 1, 0);
			DataSinkContract contract = TestPlan.createDefaultSink(this.getOutputName());
			contract.setInput(pactModule.getInput(0));
			pactModule.addInternalOutput(contract);
			return pactModule;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = super.hashCode();
			result = prime * result + this.index;
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (!super.equals(obj))
				return false;
			if (this.getClass() != obj.getClass())
				return false;
			MockupSink other = (MockupSink) obj;
			return this.index == other.index;
		}

		@Override
		public String toString() {
			return String.format("MockupSink [%s]", index);
		}
	}

	public static class Input {
		private int index;

		private Operator operator;

		private TestPairs<PactJsonObject.Key, PactJsonObject> input = new TestPairs<PactJsonObject.Key, PactJsonObject>();

		public Input(int index) {
			this.index = index;
			this.operator = new MockupSource(index);
		}

		public int getIndex() {
			return this.index;
		}

		public Operator getOperator() {
			return this.operator;
		}

		public Input add(PactJsonObject object) {
			this.input.add(new KeyValuePair<PactJsonObject.Key, PactJsonObject>(PactJsonObject.Key.NULL, object));
			return this;
		}

		public Input setEmpty() {
			this.input.setEmpty();
			return this;
		}

		public void setOperator(Operator operator) {
			if (operator == null)
				throw new NullPointerException("operator must not be null");

			this.operator = operator;
		}

		public void prepare(TestPlan testPlan) {
			if (this.operator instanceof MockupSource)
				testPlan.getInput(this.index).add(this.input);
		}
	}

	public static class Output {
		private int index;

		private Sink operator;

		private TestPairs<PactJsonObject.Key, PactJsonObject> expected = new TestPairs<PactJsonObject.Key, PactJsonObject>();

		public Output(int index) {
			this.index = index;
			this.operator = new MockupSink(index);
		}

		public int getIndex() {
			return this.index;
		}

		public Sink getOperator() {
			return this.operator;
		}

		public Output add(PactJsonObject object) {
			this.expected.add(new KeyValuePair<PactJsonObject.Key, PactJsonObject>(PactJsonObject.Key.NULL, object));
			return this;
		}

		public Output setEmpty() {
			this.expected.setEmpty();
			return this;
		}

		public void setOperator(Sink operator) {
			if (operator == null)
				throw new NullPointerException("operator must not be null");

			this.operator = operator;
		}

		public void prepare(TestPlan testPlan) {
			if (this.operator instanceof MockupSink)
				testPlan.getExpectedOutput(this.index).add(
					(TestPairs<PactJsonObject.Key, PactJsonObject>) this.expected);
		}
	}

	private Input[] inputs;

	private Output[] outputs;

	public SopremoTestPlan(int numInputs, int numOutputs) {
		this.inputs = new Input[numInputs];
		for (int index = 0; index < numInputs; index++)
			this.inputs[index] = new Input(index);
		this.outputs = new Output[numOutputs];
		for (int index = 0; index < numOutputs; index++)
			this.outputs[index] = new Output(index);
	}

	public Input getInput(int index) {
		return this.inputs[index];
	}

	public Operator getInputOperator(int index) {
		return this.getInput(index).getOperator();
	}

	public Operator[] getInputOperators(int from, int to) {
		Operator[] operators = new Operator[to - from];
		for (int index = 0; index < operators.length; index++)
			operators[index] = this.getInputOperator(from + index);
		return operators;
	}

	public Sink[] getOutputOperators(int from, int to) {
		Sink[] operators = new Sink[to - from];
		for (int index = 0; index < operators.length; index++)
			operators[index] = this.getOutputOperator(from + index);
		return operators;
	}

	public Output getExpectedOutput(int index) {
		return this.outputs[index];
	}

	public Sink getOutputOperator(int index) {
		return this.getExpectedOutput(index).getOperator();
	}

	public void setOutputOperator(int index, Sink operator) {
		this.outputs[index].setOperator(operator);
	}

	public void setInputOperator(int index, Operator operator) {
		this.inputs[index].setOperator(operator);
	}

	public void run() {
		SopremoPlan sopremoPlan = new SopremoPlan(this.getOutputOperators(0, this.outputs.length));
		TestPlan testPlan = new TestPlan(sopremoPlan.assemblePact());
		for (Input input : this.inputs)
			input.prepare(testPlan);
		for (Output output : this.outputs)
			output.prepare(testPlan);
		testPlan.run();
	}
}
