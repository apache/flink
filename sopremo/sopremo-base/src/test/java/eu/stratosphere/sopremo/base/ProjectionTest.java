package eu.stratosphere.sopremo.base;
import static eu.stratosphere.sopremo.JsonUtil.createObjectNode;
import static eu.stratosphere.sopremo.JsonUtil.createPath;

import org.junit.Test;

import eu.stratosphere.sopremo.SopremoTest;
import eu.stratosphere.sopremo.expressions.ArithmeticExpression;
import eu.stratosphere.sopremo.expressions.ArithmeticExpression.ArithmeticOperator;
import eu.stratosphere.sopremo.expressions.ObjectCreation;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;
import eu.stratosphere.sopremo.testing.SopremoTestPlan;

public class ProjectionTest extends SopremoTest<Projection> {
	@Override
	protected Projection createDefaultInstance(final int index) {
		final ObjectCreation transformation = new ObjectCreation();
		transformation.addMapping("field", createPath(String.valueOf(index)));
		return new Projection().withValueTransformation(transformation);
	}

	@Test
	public void shouldProjectSomeFields() {
		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(1, 1);

		final ObjectCreation transformation = new ObjectCreation();
		transformation.addMapping("sum", new ArithmeticExpression(createPath("a"),
			ArithmeticOperator.ADDITION, createPath("b")));
		sopremoPlan.getOutputOperator(0).setInputs(
			new Projection().
				withValueTransformation(transformation).
				withInputs(sopremoPlan.getInputOperator(0)));
		Object[] fields = { "a", 1, "b", 4 };
		Object[] fields1 = { "a", 2, "b", 5 };
		Object[] fields2 = { "a", -1, "b", 4 };

		sopremoPlan.getInput(0).
			add((JsonNode) createObjectNode(fields)).
			add((JsonNode) createObjectNode(fields1)).
			add((JsonNode) createObjectNode(fields2));
		Object[] fields3 = { "sum", 5 };
		Object[] fields4 = { "sum", 7 };
		Object[] fields5 = { "sum", 3 };
		sopremoPlan.getExpectedOutput(0).
			add((JsonNode) createObjectNode(fields3)).
			add((JsonNode) createObjectNode(fields4)).
			add((JsonNode) createObjectNode(fields5));

		sopremoPlan.run();
	}
}
