package eu.stratosphere.sopremo.operator;

import org.junit.Test;

import eu.stratosphere.sopremo.SopremoTest;
import eu.stratosphere.sopremo.SopremoTestPlan;
import eu.stratosphere.sopremo.base.Join;
import eu.stratosphere.sopremo.expressions.Comparison;
import eu.stratosphere.sopremo.expressions.Condition;
import eu.stratosphere.sopremo.expressions.ElementInSetExpression;
import eu.stratosphere.sopremo.expressions.ObjectCreation;
import eu.stratosphere.sopremo.expressions.Comparison.BinaryOperator;
import eu.stratosphere.sopremo.expressions.ElementInSetExpression.Quantor;

public class JoinTest extends SopremoTest {

	@Test
	public void shouldPerformEquiJoin() {
		SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		Condition condition = new Condition(new Comparison(createPath("0", "id"), BinaryOperator.EQUAL, createPath("1",
			"userid")));
		Join join = new Join(ObjectCreation.CONCATENATION, condition, sopremoPlan.getInputOperators(0, 2));
		sopremoPlan.getOutputOperator(0).setInputOperators(join);

		sopremoPlan.getInput(0).
			add(createJsonObject("name", "Jon Doe", "password", "asdf1234", "id", 1)).
			add(createJsonObject("name", "Jane Doe", "password", "qwertyui", "id", 2)).
			add(createJsonObject("name", "Max Mustermann", "password", "q1w2e3r4", "id", 3));
		sopremoPlan.getInput(1).
			add(createJsonObject("userid", 1, "url", "code.google.com/p/jaql/")).
			add(createJsonObject("userid", 2, "url", "www.cnn.com")).
			add(createJsonObject("userid", 1, "url", "java.sun.com/javase/6/docs/api/"));
		sopremoPlan.getExpectedOutput(0).
			add(
				createJsonObject("name", "Jon Doe", "password", "asdf1234", "id", 1, "userid", 1, "url",
					"code.google.com/p/jaql/")).
			add(
				createJsonObject("name", "Jon Doe", "password", "asdf1234", "id", 1, "userid", 1, "url",
					"java.sun.com/javase/6/docs/api/")).
			add(
				createJsonObject("name", "Jane Doe", "password", "qwertyui", "id", 2, "userid", 2, "url",
					"www.cnn.com"));

		sopremoPlan.run();
	}

	@Test
	public void shouldPerformLeftOuterJoin() {
		SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		Condition condition = new Condition(new Comparison(createPath("0", "id"), BinaryOperator.EQUAL, createPath("1",
			"userid")));
		Join join = new Join(ObjectCreation.CONCATENATION, condition, sopremoPlan.getInputOperators(0, 2));
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
		sopremoPlan.getExpectedOutput(0).
			add(
				createJsonObject("name", "Jon Doe", "password", "asdf1234", "id", 1, "userid", 1, "url",
					"code.google.com/p/jaql/")).
			add(
				createJsonObject("name", "Jon Doe", "password", "asdf1234", "id", 1, "userid", 1, "url",
					"java.sun.com/javase/6/docs/api/")).
			add(
				createJsonObject("name", "Jane Doe", "password", "qwertyui", "id", 2, "userid", 2, "url",
					"www.cnn.com")).
			add(
				createJsonObject("name", "Max Mustermann", "password", "q1w2e3r4", "id", 3));

		sopremoPlan.run();
	}

	@Test
	public void shouldPerformRightOuterJoin() {
		SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		Condition condition = new Condition(new Comparison(createPath("0", "id"), BinaryOperator.EQUAL, createPath("1",
			"userid")));
		Join join = new Join(ObjectCreation.CONCATENATION, condition, sopremoPlan.getInputOperators(0, 2));
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
		sopremoPlan.getExpectedOutput(0).
			add(
				createJsonObject("name", "Jon Doe", "password", "asdf1234", "id", 1, "userid", 1, "url",
					"code.google.com/p/jaql/")).
			add(
				createJsonObject("name", "Jon Doe", "password", "asdf1234", "id", 1, "userid", 1, "url",
					"java.sun.com/javase/6/docs/api/")).
			add(
				createJsonObject("name", "Jane Doe", "password", "qwertyui", "id", 2, "userid", 2, "url",
					"www.cnn.com")).
			add(
				createJsonObject("userid", 4, "url", "www.nbc.com"));

		sopremoPlan.run();
	}

	@Test
	public void shouldPerformFullOuterJoin() {
		SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		Condition condition = new Condition(new Comparison(createPath("0", "id"), BinaryOperator.EQUAL, createPath("1",
			"userid")));
		Join join = new Join(ObjectCreation.CONCATENATION, condition, sopremoPlan.getInputOperators(0, 2));
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
		sopremoPlan.getExpectedOutput(0).
			add(
				createJsonObject("name", "Jon Doe", "password", "asdf1234", "id", 1, "userid", 1, "url",
					"code.google.com/p/jaql/")).
			add(
				createJsonObject("name", "Jon Doe", "password", "asdf1234", "id", 1, "userid", 1, "url",
					"java.sun.com/javase/6/docs/api/")).
			add(
				createJsonObject("name", "Jane Doe", "password", "qwertyui", "id", 2, "userid", 2, "url",
					"www.cnn.com")).
			add(
				createJsonObject("name", "Max Mustermann", "password", "q1w2e3r4", "id", 3)).
			add(
				createJsonObject("userid", 4, "url", "www.nbc.com"));

		sopremoPlan.run();
	}

	@Test
	public void shouldPerformThetaJoin() {
		SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		Condition condition = new Condition(new Comparison(createPath("0", "id"), BinaryOperator.LESS, createPath("1",
			"userid")));
		Join join = new Join(ObjectCreation.CONCATENATION, condition, sopremoPlan.getInputOperators(0, 2));
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
			.getExpectedOutput(0)
			.
			add(
				createJsonObject("name", "Jon Doe", "password", "asdf1234", "id", 1, "userid", 2, "url",
					"www.cnn.com"))
			.
			add(
				createJsonObject("name", "Jon Doe", "password", "asdf1234", "id", 1, "userid", 4, "url",
					"www.nbc.com"))
			.
			add(
				createJsonObject("name", "Jane Doe", "password", "qwertyui", "id", 2, "userid", 4, "url",
					"www.nbc.com"))
			.
			add(
				createJsonObject("name", "Max Mustermann", "password", "q1w2e3r4", "id", 3, "userid", 4, "url",
					"www.nbc.com"));

		sopremoPlan.run();
	}

	@Test
	public void shouldPerformAntiJoin() {
		SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		Condition condition = new Condition(new ElementInSetExpression(createPath("0", "DeptName"), Quantor.EXISTS_NOT_IN,
			createPath("1", "Name")));
		Join join = new Join(ObjectCreation.CONCATENATION, condition, sopremoPlan.getInputOperators(0, 2));
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
		sopremoPlan.getExpectedOutput(0).
			add(createJsonObject("Name", "Harry", "EmpId", 3415, "DeptName", "Finance")).
			add(createJsonObject("Name", "George", "EmpId", 3401, "DeptName", "Finance"));

		sopremoPlan.run();
	}

	@Test
	public void shouldPerformSemiJoin() {
		SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		Condition condition = new Condition(new ElementInSetExpression(createPath("0", "DeptName"), Quantor.EXISTS_IN,
			createPath("1", "Name")));
		Join join = new Join(ObjectCreation.CONCATENATION, condition, sopremoPlan.getInputOperators(0, 2));
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
		sopremoPlan.getExpectedOutput(0).
			add(createJsonObject("Name", "Sally", "EmpId", 2241, "DeptName", "Sales")).
			add(createJsonObject("Name", "Harriet", "EmpId", 2202, "DeptName", "Production"));

		sopremoPlan.run();
	}
}
