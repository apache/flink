package eu.stratosphere.sopremo.operator;

import org.junit.Test;

import eu.stratosphere.pact.testing.TestPlan;
import eu.stratosphere.sopremo.Comparison;
import eu.stratosphere.sopremo.Comparison.BinaryOperator;
import eu.stratosphere.sopremo.ElementExpression.Quantor;
import eu.stratosphere.sopremo.Condition;
import eu.stratosphere.sopremo.ElementExpression;
import eu.stratosphere.sopremo.SopremoPlan;
import eu.stratosphere.sopremo.SopremoTest;
import eu.stratosphere.sopremo.SopremoTestPlan;
import eu.stratosphere.sopremo.expressions.Transformation;

public class JoinTest extends SopremoTest {

	@Test
	public void shouldPerformEquiJoin() {
		SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		Condition condition = new Condition(new Comparison(createPath("0", "id"), BinaryOperator.EQUAL, createPath("1",
			"userid")));
		Join join = new Join(Transformation.CONCATENATION, condition, sopremoPlan.getInputOperators(0, 2));
		sopremoPlan.getOutputOperator(0).setInputOperators(join);

		sopremoPlan.getInput(0).
			add(createJsonObject("name", "Jon Doe", "password", "asdf1234", "id", 1)).
			add(createJsonObject("name", "Jane Doe", "password", "qwertyui", "id", 2)).
			add(createJsonObject("name", "Max Mustermann", "password", "q1w2e3r4", "id", 3));
		sopremoPlan.getInput(1).
			add(createJsonObject("userid", 1, "url", "code.google.com/p/jaql/")).
			add(createJsonObject("userid", 2, "url", "www.cnn.com")).
			add(createJsonObject("userid", 1, "url", "java.sun.com/javase/6/docs/api/"));
		sopremoPlan.getOutput(0).
			addExpected(
				createJsonObject("name", "Jon Doe", "password", "asdf1234", "id", 1, "userid", 1, "url",
					"code.google.com/p/jaql/")).
			addExpected(
				createJsonObject("name", "Jon Doe", "password", "asdf1234", "id", 1, "userid", 1, "url",
					"java.sun.com/javase/6/docs/api/")).
			addExpected(
				createJsonObject("name", "Jane Doe", "password", "qwertyui", "id", 2, "userid", 2, "url",
					"www.cnn.com"));

		sopremoPlan.run();
	}

	@Test
	public void shouldPerformLeftOuterJoin() {
		SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		Condition condition = new Condition(new Comparison(createPath("0", "id"), BinaryOperator.EQUAL, createPath("1",
			"userid")));
		Join join = new Join(Transformation.CONCATENATION, condition, sopremoPlan.getInputOperators(0, 2));
		// here we set outer join flag
		join.setOuterJoin(sopremoPlan.getInputOperator(0));
		sopremoPlan.getOutputOperator(0).setInputOperators(join);

		sopremoPlan.getInput(0).
			add(createJsonObject("name", "Jon Doe", "password", "asdf1234", "id", 1)).
			add(createJsonObject("name", "Jane Doe", "password", "qwertyui", "id", 2)).
			add(createJsonObject("name", "Max Mustermann", "password", "q1w2e3r4", "id", 3));
		sopremoPlan.getInput(1).
			add(createJsonObject("userid", 1, "url", "code.google.com/p/jaql/")).
			add(createJsonObject("userid", 2, "url", "www.cnn.com")).
			add(createJsonObject("userid", 1, "url", "java.sun.com/javase/6/docs/api/"));
		sopremoPlan.getOutput(0).
			addExpected(
				createJsonObject("name", "Jon Doe", "password", "asdf1234", "id", 1, "userid", 1, "url",
					"code.google.com/p/jaql/")).
			addExpected(
				createJsonObject("name", "Jon Doe", "password", "asdf1234", "id", 1, "userid", 1, "url",
					"java.sun.com/javase/6/docs/api/")).
			addExpected(
				createJsonObject("name", "Jane Doe", "password", "qwertyui", "id", 2, "userid", 2, "url",
					"www.cnn.com")).
			addExpected(
				createJsonObject("name", "Max Mustermann", "password", "q1w2e3r4", "id", 3));

		sopremoPlan.run();
	}

	@Test
	public void shouldPerformRightOuterJoin() {
		SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		Condition condition = new Condition(new Comparison(createPath("0", "id"), BinaryOperator.EQUAL, createPath("1",
			"userid")));
		Join join = new Join(Transformation.CONCATENATION, condition, sopremoPlan.getInputOperators(0, 2));
		// here we set outer join flag
		join.setOuterJoin(sopremoPlan.getInputOperator(1));
		sopremoPlan.getOutputOperator(0).setInputOperators(join);

		sopremoPlan.getInput(0).
			add(createJsonObject("name", "Jon Doe", "password", "asdf1234", "id", 1)).
			add(createJsonObject("name", "Jane Doe", "password", "qwertyui", "id", 2)).
			add(createJsonObject("name", "Max Mustermann", "password", "q1w2e3r4", "id", 3));
		sopremoPlan.getInput(1).
			add(createJsonObject("userid", 1, "url", "code.google.com/p/jaql/")).
			add(createJsonObject("userid", 2, "url", "www.cnn.com")).
			add(createJsonObject("userid", 4, "url", "www.nbc.com")).
			add(createJsonObject("userid", 1, "url", "java.sun.com/javase/6/docs/api/"));
		sopremoPlan.getOutput(0).
			addExpected(
				createJsonObject("name", "Jon Doe", "password", "asdf1234", "id", 1, "userid", 1, "url",
					"code.google.com/p/jaql/")).
			addExpected(
				createJsonObject("name", "Jon Doe", "password", "asdf1234", "id", 1, "userid", 1, "url",
					"java.sun.com/javase/6/docs/api/")).
			addExpected(
				createJsonObject("name", "Jane Doe", "password", "qwertyui", "id", 2, "userid", 2, "url",
					"www.cnn.com")).
			addExpected(
				createJsonObject("userid", 4, "url", "www.nbc.com"));

		sopremoPlan.run();
	}

	@Test
	public void shouldPerformFullOuterJoin() {
		SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		Condition condition = new Condition(new Comparison(createPath("0", "id"), BinaryOperator.EQUAL, createPath("1",
			"userid")));
		Join join = new Join(Transformation.CONCATENATION, condition, sopremoPlan.getInputOperators(0, 2));
		// here we set outer join flag
		join.setOuterJoin(sopremoPlan.getInputOperators(0, 2));
		sopremoPlan.getOutputOperator(0).setInputOperators(join);

		sopremoPlan.getInput(0).
			add(createJsonObject("name", "Jon Doe", "password", "asdf1234", "id", 1)).
			add(createJsonObject("name", "Jane Doe", "password", "qwertyui", "id", 2)).
			add(createJsonObject("name", "Max Mustermann", "password", "q1w2e3r4", "id", 3));
		sopremoPlan.getInput(1).
			add(createJsonObject("userid", 1, "url", "code.google.com/p/jaql/")).
			add(createJsonObject("userid", 2, "url", "www.cnn.com")).
			add(createJsonObject("userid", 4, "url", "www.nbc.com")).
			add(createJsonObject("userid", 1, "url", "java.sun.com/javase/6/docs/api/"));
		sopremoPlan.getOutput(0).
			addExpected(
				createJsonObject("name", "Jon Doe", "password", "asdf1234", "id", 1, "userid", 1, "url",
					"code.google.com/p/jaql/")).
			addExpected(
				createJsonObject("name", "Jon Doe", "password", "asdf1234", "id", 1, "userid", 1, "url",
					"java.sun.com/javase/6/docs/api/")).
			addExpected(
				createJsonObject("name", "Jane Doe", "password", "qwertyui", "id", 2, "userid", 2, "url",
					"www.cnn.com")).
			addExpected(
				createJsonObject("name", "Max Mustermann", "password", "q1w2e3r4", "id", 3)).
			addExpected(
				createJsonObject("userid", 4, "url", "www.nbc.com"));

		sopremoPlan.run();
	}

	@Test
	public void shouldPerformThetaJoin() {
		SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		Condition condition = new Condition(new Comparison(createPath("0", "id"), BinaryOperator.LESS, createPath("1",
			"userid")));
		Join join = new Join(Transformation.CONCATENATION, condition, sopremoPlan.getInputOperators(0, 2));
		// here we set outer join flag
		join.setOuterJoin(sopremoPlan.getInputOperators(0, 2));
		sopremoPlan.getOutputOperator(0).setInputOperators(join);

		sopremoPlan.getInput(0).
			add(createJsonObject("name", "Jon Doe", "password", "asdf1234", "id", 1)).
			add(createJsonObject("name", "Jane Doe", "password", "qwertyui", "id", 2)).
			add(createJsonObject("name", "Max Mustermann", "password", "q1w2e3r4", "id", 3));
		sopremoPlan.getInput(1).
			add(createJsonObject("userid", 1, "url", "code.google.com/p/jaql/")).
			add(createJsonObject("userid", 2, "url", "www.cnn.com")).
			add(createJsonObject("userid", 4, "url", "www.nbc.com")).
			add(createJsonObject("userid", 1, "url", "java.sun.com/javase/6/docs/api/"));
		sopremoPlan
			.getOutput(0)
			.
			addExpected(
				createJsonObject("name", "Jon Doe", "password", "asdf1234", "id", 1, "userid", 2, "url",
					"www.cnn.com"))
			.
			addExpected(
				createJsonObject("name", "Jon Doe", "password", "asdf1234", "id", 1, "userid", 4, "url",
					"www.nbc.com"))
			.
			addExpected(
				createJsonObject("name", "Jane Doe", "password", "qwertyui", "id", 2, "userid", 4, "url",
					"www.nbc.com"))
			.
			addExpected(
				createJsonObject("name", "Max Mustermann", "password", "q1w2e3r4", "id", 3, "userid", 4, "url",
					"www.nbc.com"));

		sopremoPlan.run();
	}

	@Test
	public void shouldPerformAntiJoin() {
		SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		Condition condition = new Condition(new ElementExpression(createPath("0", "DeptName"), Quantor.EXISTS_NOT_IN,
			createPath("1", "Name")));
		Join join = new Join(Transformation.CONCATENATION, condition, sopremoPlan.getInputOperators(0, 2));
		// here we set outer join flag
		join.setOuterJoin(sopremoPlan.getInputOperators(0, 2));
		sopremoPlan.getOutputOperator(0).setInputOperators(join);

		sopremoPlan.getInput(0).
			add(createJsonObject("Name", "Harry", "EmpId", 3415, "DeptName", "Finance")).
			add(createJsonObject("Name", "Sally", "EmpId", 2241, "DeptName", "Sales")).
			add(createJsonObject("Name", "George", "EmpId", 3401, "DeptName", "Finance")).
			add(createJsonObject("Name", "Harriet", "EmpId", 2202, "DeptName", "Production"));
		sopremoPlan.getInput(1).
			add(createJsonObject("Name", "Sales", "Manager", "Harriet")).
			add(createJsonObject("Name", "Production", "Manager", "Charles"));
		sopremoPlan.getOutput(0).
			addExpected(createJsonObject("Name", "Harry", "EmpId", 3415, "DeptName", "Finance")).
			addExpected(createJsonObject("Name", "George", "EmpId", 3401, "DeptName", "Finance"));

		sopremoPlan.run();
	}

	@Test
	public void shouldPerformSemiJoin() {
		SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		Condition condition = new Condition(new ElementExpression(createPath("0", "DeptName"), Quantor.EXISTS_IN,
			createPath("1", "Name")));
		Join join = new Join(Transformation.CONCATENATION, condition, sopremoPlan.getInputOperators(0, 2));
		// here we set outer join flag
		join.setOuterJoin(sopremoPlan.getInputOperators(0, 2));
		sopremoPlan.getOutputOperator(0).setInputOperators(join);

		sopremoPlan.getInput(0).
			add(createJsonObject("Name", "Harry", "EmpId", 3415, "DeptName", "Finance")).
			add(createJsonObject("Name", "Sally", "EmpId", 2241, "DeptName", "Sales")).
			add(createJsonObject("Name", "George", "EmpId", 3401, "DeptName", "Finance")).
			add(createJsonObject("Name", "Harriet", "EmpId", 2202, "DeptName", "Production"));
		sopremoPlan.getInput(1).
			add(createJsonObject("Name", "Sales", "Manager", "Harriet")).
			add(createJsonObject("Name", "Production", "Manager", "Charles"));
		sopremoPlan.getOutput(0).
			addExpected(createJsonObject("Name", "Sally", "EmpId", 2241, "DeptName", "Sales")).
			addExpected(createJsonObject("Name", "Harriet", "EmpId", 2202, "DeptName", "Production"));

		sopremoPlan.run();
	}
}
