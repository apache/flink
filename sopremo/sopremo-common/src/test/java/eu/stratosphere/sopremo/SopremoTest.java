package eu.stratosphere.sopremo;

import java.util.ArrayList;
import java.util.List;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

import eu.stratosphere.pact.common.contract.DataSourceContract;
import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.type.base.PactJsonObject;
import eu.stratosphere.pact.common.type.base.PactNull;
import eu.stratosphere.pact.testing.TestPlan;
import eu.stratosphere.pact.testing.TestPlanTestCase;
import eu.stratosphere.pact.testing.ioformats.JsonInputFormat;
import eu.stratosphere.sopremo.Operator.Output;
import eu.stratosphere.sopremo.expressions.ArrayAccess;
import eu.stratosphere.sopremo.expressions.ArrayCreation;
import eu.stratosphere.sopremo.expressions.Constant;
import eu.stratosphere.sopremo.expressions.EvaluableExpression;
import eu.stratosphere.sopremo.expressions.FieldAccess;
import eu.stratosphere.sopremo.expressions.Input;
import eu.stratosphere.sopremo.expressions.ObjectCreation;
import eu.stratosphere.sopremo.expressions.Path;
import eu.stratosphere.sopremo.expressions.ValueAssignment;
import eu.stratosphere.sopremo.operator.DataType;
import eu.stratosphere.sopremo.operator.Source;

public class SopremoTest extends TestPlanTestCase {
	public class MockupSource extends Source {
		private Operator operator;

		private int index;

		public MockupSource(Operator operator, int index) {
			super(DataType.ADHOC, operator.getName() + "-input" + index);
			this.operator = operator;
			this.index = index;
		}

		@Override
		public PactModule asPactModule() {
			PactModule pactModule = new PactModule(1, 1);
			DataSourceContract contract = TestPlan.createDefaultSource(getInputName());
			pactModule.getOutput(0).setInput(contract);
			pactModule.setInput(0, contract);
			return pactModule;
		}
	}

	public static EvaluableExpression createJsonArray(Object... constants) {
		EvaluableExpression[] elements = new EvaluableExpression[constants.length];
		for (int index = 0; index < elements.length; index++)
			if (constants[index] instanceof EvaluableExpression)
				elements[index] = (EvaluableExpression) constants[index];
			else
				elements[index] = new Constant(constants[index]);
		return new ArrayCreation(elements);
	}

	public static EvaluableExpression createObject(Object... fields) {
		if (fields.length % 2 != 0)
			throw new IllegalArgumentException();
		ValueAssignment[] assignments = new ValueAssignment[fields.length / 2];
		for (int index = 0; index < assignments.length; index++) {
			assignments[index] = new ValueAssignment(fields[2 * index].toString(), new Constant(fields[2 * index + 1]));
		}
		return new ObjectCreation(assignments);
	}

	public static Path createPath(String... parts) {
		List<EvaluableExpression> fragments = new ArrayList<EvaluableExpression>();
		for (int index = 0; index < parts.length; index++) {
			EvaluableExpression segment;
			if (parts[index].equals("$"))
				segment = new Input(0);
			else if (parts[index].matches("[0-9]+"))
				segment = new Input(Integer.parseInt(parts[index]));
			else if (parts[index].matches("\\[.*\\]")) {
				if (parts[index].charAt(1) == '*')
					segment = new ArrayAccess();
				else if (parts[index].contains(":")) {
					int delim = parts[index].indexOf(":");
					segment = new ArrayAccess(Integer.parseInt(parts[index].substring(1, delim)),
						Integer.parseInt(parts[index].substring(delim + 1, parts[index].length() - 1)));
				} else
					segment = new ArrayAccess(Integer.parseInt(parts[index].substring(1, parts[index].length() - 1)));
			} else
				segment = new FieldAccess(parts[index]);
			fragments.add(segment);
		}
		return new Path(fragments);
	}

	protected TestPlan createTestPlan(SopremoPlan sopremoPlan) {
		for (Operator operator : sopremoPlan.getAllNodes()) {
			List<Output> inputs = operator.getInputs();
			for (int index = 0; index < inputs.size(); index++) {
				if (inputs.get(index) == null)
					inputs.set(index, new MockupSource(operator, index).getOutput(0));
			}
			operator.setInputs(inputs);
		}

		TestPlan testPlan = new TestPlan(sopremoPlan.assemblePact());
		return testPlan;
	}

	protected static JsonNodeFactory NODE_FACTORY = JsonNodeFactory.instance;

	protected static ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static KeyValuePair<Key, Value> createJsonObject(Object... fields) {
		if (fields.length % 2 != 0)
			throw new IllegalArgumentException();
		ObjectNode objectNode = NODE_FACTORY.objectNode();
		for (int index = 0; index < fields.length; index += 2) {
			objectNode.put(fields[index].toString(), OBJECT_MAPPER.valueToTree(fields[index + 1]));
		}
		return new KeyValuePair<Key, Value>(PactNull.INSTANCE, new PactJsonObject(objectNode));
	}
}
