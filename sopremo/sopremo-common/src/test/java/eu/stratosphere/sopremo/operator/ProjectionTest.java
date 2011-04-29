package eu.stratosphere.sopremo.operator;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.testing.TestPlan;
import eu.stratosphere.sopremo.SopremoPlan;
import eu.stratosphere.sopremo.SopremoTest;
import eu.stratosphere.sopremo.expressions.Arithmetic;
import eu.stratosphere.sopremo.expressions.Arithmetic.ArithmeticOperator;
import eu.stratosphere.sopremo.expressions.ArrayAccess;
import eu.stratosphere.sopremo.expressions.ArrayCreation;
import eu.stratosphere.sopremo.expressions.Constant;
import eu.stratosphere.sopremo.expressions.EvaluableExpression;
import eu.stratosphere.sopremo.expressions.FieldAccess;
import eu.stratosphere.sopremo.expressions.Input;
import eu.stratosphere.sopremo.expressions.ObjectCreation;
import eu.stratosphere.sopremo.expressions.Path;
import eu.stratosphere.sopremo.expressions.Transformation;
import eu.stratosphere.sopremo.expressions.ValueAssignment;

public class ProjectionTest extends SopremoTest {
	@Test
	public void shouldProjectSomeFields() {
		Transformation transformation = new Transformation();
		transformation.addMapping(new ValueAssignment("c", new Arithmetic(createPath("$", "a"),
			ArithmeticOperator.MULTIPLY, createPath("$", "b"))));
		SopremoPlan sopremoPlan = new SopremoPlan(new Projection(transformation, null));

		TestPlan testPlan = createTestPlan(sopremoPlan);
		testPlan.getInput().add(createJsonObject("a", 1, "b", 3)).add(createJsonObject("a", 2, "b", 4));
		testPlan.getExpectedOutput().add(createJsonObject("c", 3)).add(createJsonObject("c", 8));
		testPlan.run();
		System.out.println(testPlan.getActualOutput());
	}
}
