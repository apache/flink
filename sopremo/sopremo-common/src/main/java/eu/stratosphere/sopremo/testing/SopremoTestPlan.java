package eu.stratosphere.sopremo.testing;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.testing.TestPlan;
import eu.stratosphere.pact.testing.TestRecords;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.JsonStream;
import eu.stratosphere.sopremo.JsonUtil;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.OperatorNavigator;
import eu.stratosphere.sopremo.Sink;
import eu.stratosphere.sopremo.SopremoModule;
import eu.stratosphere.sopremo.SopremoPlan;
import eu.stratosphere.sopremo.Source;
import eu.stratosphere.sopremo.pact.IOConstants;
import eu.stratosphere.sopremo.pact.JsonInputFormat;
import eu.stratosphere.sopremo.pact.RecordToJsonIterator;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.sopremo.serialization.Schema;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.NullNode;
import eu.stratosphere.util.dag.OneTimeTraverser;

public class SopremoTestPlan {
	private Input[] inputs;

	private ActualOutput[] actualOutputs;

	//
	// public static interface TestObjects extends Iterable<IJsonNode> {
	// public TestObjects add(IJsonNode object);
	//
	// public TestObjects setEmpty();
	// }

	private ExpectedOutput[] expectedOutputs;

	private transient TestPlan testPlan;

	private final EvaluationContext evaluationContext = new EvaluationContext();

	private boolean trace;

	public SopremoTestPlan(final int numInputs, final int numOutputs) {
		this.initInputsAndOutputs(numInputs, numOutputs);
	}

	public SopremoTestPlan(final Operator<?>... sinks) {
		final List<JsonStream> unconnectedOutputs = new ArrayList<JsonStream>();
		final List<Operator<?>> unconnectedInputs = new ArrayList<Operator<?>>();
		for (final Operator<?> operator : sinks) {
			unconnectedOutputs.addAll(operator.getOutputs());
			if (operator instanceof Sink)
				unconnectedOutputs.add(operator);
		}

		for (final Operator<?> operator : OneTimeTraverser.INSTANCE.getReachableNodes(sinks, OperatorNavigator.INSTANCE))
			if (operator instanceof Source)
				unconnectedInputs.add(operator);
			else
				for (final JsonStream input : operator.getInputs())
					if (input == null)
						unconnectedInputs.add(operator);

		this.inputs = new Input[unconnectedInputs.size()];
		for (int index = 0; index < this.inputs.length; index++) {
			this.inputs[index] = new Input(index);
			final Operator<?> unconnectedNode = unconnectedInputs.get(index);
			if (unconnectedNode instanceof Source)
				this.setInputOperator(index, (Source) unconnectedNode);
			else {
				final List<JsonStream> missingInputs = new ArrayList<JsonStream>(
					unconnectedNode.getInputs());
				for (int missingIndex = 0; missingIndex < missingInputs.size(); missingIndex++)
					if (missingInputs.get(missingIndex) == null) {
						missingInputs.set(missingIndex, this.inputs[index].getOperator().getOutput(0));
						break;
					}
				unconnectedNode.setInputs(missingInputs);
			}
		}
		this.actualOutputs = new ActualOutput[unconnectedOutputs.size()];
		this.expectedOutputs = new ExpectedOutput[unconnectedOutputs.size()];
		for (int index = 0; index < this.actualOutputs.length; index++) {
			this.actualOutputs[index] = new ActualOutput(index);
			if (unconnectedOutputs.get(index) instanceof Sink)
				this.actualOutputs[index].setOperator((Sink) unconnectedOutputs.get(index));
			else
				this.actualOutputs[index].getOperator().setInput(0, unconnectedOutputs.get(index));
			this.expectedOutputs[index] = new ExpectedOutput(index);
		}
	}

	public void trace() {
		this.trace = true;
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		final SopremoTestPlan other = (SopremoTestPlan) obj;
		return Arrays.equals(this.inputs, other.inputs) && Arrays.equals(this.expectedOutputs, other.expectedOutputs)
			&& Arrays.equals(this.actualOutputs, other.actualOutputs);
	}

	public ActualOutput getActualOutput(final int index) {
		return this.actualOutputs[index];
	}

	public ActualOutput getActualOutputForStream(final JsonStream stream) {
		for (final ActualOutput output : this.actualOutputs)
			if (output.getOperator().getInput(0) == stream.getSource())
				return output;
		return null;
	}

	public EvaluationContext getEvaluationContext() {
		return this.evaluationContext;
	}

	public ExpectedOutput getExpectedOutput(final int index) {
		return this.expectedOutputs[index];
	}

	public ExpectedOutput getExpectedOutputForStream(final JsonStream stream) {
		return this.expectedOutputs[this.getActualOutputForStream(stream).getIndex()];
	}

	public Input getInput(final int index) {
		return this.inputs[index];
	}

	public Input getInputForStream(final JsonStream stream) {
		for (final Input input : this.inputs)
			if (input.getOperator().getOutput(0) == stream.getSource())
				return input;
		return null;
	}

	public Source getInputOperator(final int index) {
		return this.getInput(index).getOperator();
	}

	public Source[] getInputOperators(final int from, final int to) {
		final Source[] operators = new Source[to - from];
		for (int index = 0; index < operators.length; index++)
			operators[index] = this.getInputOperator(from + index);
		return operators;
	}

	public Sink getOutputOperator(final int index) {
		return this.getActualOutput(index).getOperator();
	}

	public Sink[] getOutputOperators(final int from, final int to) {
		final Sink[] operators = new Sink[to - from];
		for (int index = 0; index < operators.length; index++)
			operators[index] = this.getOutputOperator(from + index);
		return operators;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(this.inputs);
		result = prime * result + Arrays.hashCode(this.actualOutputs);
		result = prime * result + Arrays.hashCode(this.expectedOutputs);
		return result;
	}

	protected void initInputsAndOutputs(final int numInputs, final int numOutputs) {
		this.inputs = new Input[numInputs];
		for (int index = 0; index < numInputs; index++)
			this.inputs[index] = new Input(index);
		this.expectedOutputs = new ExpectedOutput[numOutputs];
		for (int index = 0; index < numOutputs; index++)
			this.expectedOutputs[index] = new ExpectedOutput(index);
		this.actualOutputs = new ActualOutput[numOutputs];
		for (int index = 0; index < numOutputs; index++)
			this.actualOutputs[index] = new ActualOutput(index);
	}

	public void run() {
		final SopremoPlan sopremoPlan = new SopremoPlan();
		sopremoPlan.setContext(this.evaluationContext);
		sopremoPlan.setSinks(this.getOutputOperators(0, this.expectedOutputs.length));
		this.testPlan = new TestPlan(sopremoPlan.assemblePact());
		Schema schema = getSchema();
		for (final Input input : this.inputs)
			input.sync(this.testPlan, schema);
		for (final ExpectedOutput output : this.expectedOutputs)
			output.prepare(this.testPlan, schema);
		if (this.trace)
			SopremoUtil.trace();
		this.testPlan.run();
		if (this.trace)
			SopremoUtil.untrace();
		for (final ActualOutput output : this.actualOutputs)
			output.load(this.testPlan, schema);
	}

	public void setInputOperator(final int index, final Source operator) {
		this.inputs[index].setOperator(operator);
	}

	public void setOutputOperator(final int index, final Sink operator) {
		this.actualOutputs[index].setOperator(operator);

		final TestRecords testPairs = new TestRecords();
		testPairs.fromFile(JsonInputFormat.class, operator.getOutputName());
		for (final PactRecord record : testPairs)
			this.actualOutputs[index].add(Schema.Default.recordToJson(record, null));
		testPairs.close();
	}

	@Override
	public String toString() {
		return SopremoModule.valueOf("", this.getOutputOperators(0, this.actualOutputs.length)).toString();
	}

	public static class ActualOutput extends Channel<Sink, ActualOutput> {
		public ActualOutput(final int index) {
			super(new MockupSink(index), index);
		}

		void load(final TestPlan testPlan, Schema schema) {
			this.setEmpty();
			final TestRecords actualOutput = testPlan.getActualOutput(this.getIndex());
			for (final PactRecord record : actualOutput)
				this.add(schema.recordToJson(record, null));
			actualOutput.close();
		}

		public void assertEquals(final ActualOutput expectedValues) {
			this.getPairs().assertEquals(expectedValues.getPairs());
		}
	}

	static class Channel<O extends Operator<?>, C extends Channel<O, C>> {

		private final List<IJsonNode> values = new ArrayList<IJsonNode>();

		protected String file;

		protected O operator;

		private final int index;

		public Channel(final O operator, final int index) {
			this.operator = operator;
			this.index = index;
		}

		public C add(final IJsonNode value) {
			this.values.add(value);
			return (C) this;
		}

		@Deprecated
		public C add(final IJsonNode key, final IJsonNode value) {
			return add(JsonUtil.asArray(key, value));
		}

		public void load(final String path) {
			this.file = path;
		}

		public C addObject(final Object... fields) {
			return this.add(NullNode.getInstance(), JsonUtil.createObjectNode(fields));
		}

		public C addValue(final Object value) {
			return this.add(NullNode.getInstance(), JsonUtil.createValueNode(value));
		}

		public C addArray(final Object... values) {
			return this.add(NullNode.getInstance(), JsonUtil.createArrayNode(values));
		}

		@Override
		public boolean equals(final Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (!(obj instanceof Channel))
				return false;
			final Channel<?, ?> other = (Channel<?, ?>) obj;
			return this.pairs.equals(other.pairs);
		}

		int getIndex() {
			return this.index;
		}

		O getOperator() {
			return this.operator;
		}

		TestRecords getPairs() {
			return this.pairs;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + (this.pairs == null ? 0 : this.pairs.hashCode());
			return result;
		}

		public Iterator<IJsonNode> iterator() {
			final RecordToJsonIterator iterator = new RecordToJsonIterator(this.schema);
			iterator.setIterator(this.pairs.iterator());
			return iterator;
		}

		@SuppressWarnings("unchecked")
		public C setEmpty() {
			this.pairs.setEmpty();
			return (C) this;
		}

		void setOperator(final O operator) {
			if (operator == null)
				throw new NullPointerException("operator must not be null");

			this.operator = operator;
			this.setEmpty();
		}

		@Override
		public String toString() {
			return this.pairs.toString();
		}
	}
	
	static class Channel<O extends Operator<?>, C extends Channel<O, C>> {

		private final List<IJsonNode> values = new ArrayList<IJsonNode>();

		protected String file;

		protected O operator;

		private final int index;

		public Channel(final O operator, final int index) {
			this.operator = operator;
			this.index = index;
		}

		public C add(final IJsonNode value) {
			this.values.add(value);
			return (C) this;
		}

		@Deprecated
		public C add(final IJsonNode key, final IJsonNode value) {
			return add(JsonUtil.asArray(key, value));
		}

		public void load(final String path) {
			this.file = path;
		}

		public C addObject(final Object... fields) {
			return this.add(NullNode.getInstance(), JsonUtil.createObjectNode(fields));
		}

		public C addValue(final Object value) {
			return this.add(NullNode.getInstance(), JsonUtil.createValueNode(value));
		}

		public C addArray(final Object... values) {
			return this.add(NullNode.getInstance(), JsonUtil.createArrayNode(values));
		}

		@Override
		public boolean equals(final Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (!(obj instanceof Channel))
				return false;
			final Channel<?, ?> other = (Channel<?, ?>) obj;
			return this.pairs.equals(other.pairs);
		}

		int getIndex() {
			return this.index;
		}

		O getOperator() {
			return this.operator;
		}

		TestRecords getPairs() {
			return this.pairs;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + (this.pairs == null ? 0 : this.pairs.hashCode());
			return result;
		}

		public Iterator<IJsonNode> iterator() {
			final RecordToJsonIterator iterator = new RecordToJsonIterator(this.schema);
			iterator.setIterator(this.pairs.iterator());
			return iterator;
		}

		@SuppressWarnings("unchecked")
		public C setEmpty() {
			this.pairs.setEmpty();
			return (C) this;
		}

		void setOperator(final O operator) {
			if (operator == null)
				throw new NullPointerException("operator must not be null");

			this.operator = operator;
			this.setEmpty();
		}

		@Override
		public String toString() {
			return this.pairs.toString();
		}
	}

	public static class ExpectedOutput extends Channel<Source, ExpectedOutput> implements Iterable<IJsonNode> {
		public ExpectedOutput(final int index) {
			super(new MockupSource(index), index);
		}

		public void prepare(final TestPlan testPlan, Schema schema) {
			if (this.getOperator() instanceof MockupSource)
				testPlan.getExpectedOutput(this.getIndex(), schema.getPactSchema()).add(this.getPairs());
		}
	}

	public static class Input extends Channel<Source, Input> implements Iterable<IJsonNode> {
		public Input(final int index) {
			super(new MockupSource(index), index);
		}

		void sync(final TestPlan testPlan, Schema schema) {
			PactRecord record = null;
			if (operator.isAdhoc())
				for (final IJsonNode node : (IArrayNode) operator.getAdhocValues())
					testPlan.getInput(this.getIndex()).add(record = schema.jsonToRecord(node, record));
			else {
				Configuration configuration = new Configuration();
				SopremoUtil.serialize(configuration, IOConstants.SCHEMA, schema);
				testPlan.getInput(this.getIndex()).fromFile(JsonInputFormat.class, this.file, configuration);
			}
		}

		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.sopremo.testing.SopremoTestPlan.Channel#iterator()
		 */
		@Override
		public Iterator<IJsonNode> iterator() {

			if (operator.isAdhoc())
				return super.iterator();

			Schema schema = this.getSchema();
			final TestRecords testPairs = new TestRecords(schema.getPactSchema());
			testPairs.fromFile(JsonInputFormat.class, operator.getInputName(), configuration);
			for (final PactRecord record : testPairs)
				this.inputs[index].add(Schema.Default.recordToJson(record, null));
			testPairs.close();

		}
	}

	public static class MockupSink extends Sink {
		/**
		 * 
		 */
		private static final long serialVersionUID = -8095218927711236381L;

		private final int index;

		public MockupSink(final int index) {
			super("mockup-output" + index);
			this.index = index;
		}

		@Override
		public PactModule asPactModule(final EvaluationContext context) {
			final PactModule pactModule = new PactModule(this.toString(), 1, 0);
			final FileDataSink contract = TestPlan.createDefaultSink(this.getOutputName());
			contract.setInput(pactModule.getInput(0));
			pactModule.addInternalOutput(contract);
			SopremoUtil.serialize(contract.getParameters(), IOConstants.SCHEMA, context.getOutputSchema(0));
			return pactModule;
		}

		@Override
		public boolean equals(final Object obj) {
			if (this == obj)
				return true;
			if (!super.equals(obj))
				return false;
			if (this.getClass() != obj.getClass())
				return false;
			final MockupSink other = (MockupSink) obj;
			return this.index == other.index;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = super.hashCode();
			result = prime * result + this.index;
			return result;
		}

		@Override
		public String toString() {
			return String.format("MockupSink [%s]", this.index);
		}
	}

	public static class MockupSource extends Source {

		/**
		 * 
		 */
		private static final long serialVersionUID = -7149952920902388869L;

		private final int index;

		public MockupSource(final int index) {
			super("mockup-input" + index);
			this.index = index;
		}

		@Override
		public PactModule asPactModule(final EvaluationContext context) {
			final PactModule pactModule = new PactModule(this.toString(), 0, 1);
			final FileDataSource contract = TestPlan.createDefaultSource(this.getInputName());
			pactModule.getOutput(0).setInput(contract);
			// pactModule.setInput(0, contract);
			SopremoUtil.serialize(contract.getParameters(), IOConstants.SCHEMA, context.getInputSchema(0));
			return pactModule;
		}

		@Override
		public boolean equals(final Object obj) {
			if (this == obj)
				return true;
			if (!super.equals(obj))
				return false;
			if (this.getClass() != obj.getClass())
				return false;
			final MockupSource other = (MockupSource) obj;
			return this.index == other.index;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = super.hashCode();
			result = prime * result + this.index;
			return result;
		}

		@Override
		public String toString() {
			return String.format("MockupSource [%s]", this.index);
		}
	}

	/**
	 * @return
	 */
	public Schema getSchema() {
		return Schema.Default;
	}

}
