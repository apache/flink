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
import eu.stratosphere.sopremo.Operator.Output;
import eu.stratosphere.sopremo.expressions.ArrayAccess;
import eu.stratosphere.sopremo.expressions.ArrayCreation;
import eu.stratosphere.sopremo.expressions.Constant;
import eu.stratosphere.sopremo.expressions.EvaluableExpression;
import eu.stratosphere.sopremo.expressions.FieldAccess;
import eu.stratosphere.sopremo.expressions.Input;
import eu.stratosphere.sopremo.expressions.Path;
import eu.stratosphere.sopremo.operator.DataType;
import eu.stratosphere.sopremo.operator.Source;

public class SopremoTest extends TestPlanTestCase {

	public static Evaluable createJsonArray(Object... constants) {
		Evaluable[] elements = new Evaluable[constants.length];
		for (int index = 0; index < elements.length; index++)
			if (constants[index] instanceof EvaluableExpression)
				elements[index] = (Evaluable) constants[index];
			else
				elements[index] = new Constant(constants[index]);
		return new ArrayCreation(elements);
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

	// protected TestPlan createTestPlan(SopremoPlan sopremoPlan) {
	// for (Operator operator : sopremoPlan.getAllNodes()) {
	// List<Output> inputs = operator.getInputs();
	// for (int index = 0; index < inputs.size(); index++)
	// if (inputs.get(index) == null)
	// inputs.set(index, new MockupSource(operator, index).getOutput(0));
	// operator.setInputs(inputs);
	// }
	//
	// TestPlan testPlan = new TestPlan(sopremoPlan.assemblePact());
	// return testPlan;
	// }

	protected static JsonNodeFactory NODE_FACTORY = JsonNodeFactory.instance;

	protected static ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static PactJsonObject createJsonObject(Object... fields) {
		if (fields.length % 2 != 0)
			throw new IllegalArgumentException();
		ObjectNode objectNode = NODE_FACTORY.objectNode();
		for (int index = 0; index < fields.length; index += 2)
			objectNode.put(fields[index].toString(), OBJECT_MAPPER.valueToTree(fields[index + 1]));
		return new PactJsonObject(objectNode);
	}

	public static PactJsonObject createJsonValue(Object value) {
		return new PactJsonObject(OBJECT_MAPPER.valueToTree(value));
	}
}
