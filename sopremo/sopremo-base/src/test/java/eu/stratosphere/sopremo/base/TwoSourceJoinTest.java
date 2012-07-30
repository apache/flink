package eu.stratosphere.sopremo.base;

import static eu.stratosphere.sopremo.type.JsonUtil.createPath;

import org.junit.Test;

import eu.stratosphere.sopremo.SopremoTest;
import eu.stratosphere.sopremo.expressions.ArrayCreation;
import eu.stratosphere.sopremo.expressions.BinaryBooleanExpression;
import eu.stratosphere.sopremo.expressions.ComparativeExpression;
import eu.stratosphere.sopremo.expressions.ComparativeExpression.BinaryOperator;
import eu.stratosphere.sopremo.expressions.ElementInSetExpression;
import eu.stratosphere.sopremo.expressions.ElementInSetExpression.Quantor;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.JsonStreamExpression;
import eu.stratosphere.sopremo.testing.SopremoTestPlan;

public class TwoSourceJoinTest extends SopremoTest<TwoSourceJoin> {
	@Test
	public void shouldPerformAntiTwoSourceJoin() {
		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		final BinaryBooleanExpression condition = new ElementInSetExpression(
			createPath("0", "DeptName"), Quantor.EXISTS_NOT_IN, createPath("1", "Name"));
		final TwoSourceJoin join = new TwoSourceJoin().withCondition(condition);
		join.setInputs(sopremoPlan.getInputOperators(0, 2));
		sopremoPlan.getOutputOperator(0).setInputs(join);
		sopremoPlan.getInput(0).
			addObject("Name", "Harry", "EmpId", 3415, "DeptName", "Finance").
			addObject("Name", "Sally", "EmpId", 2241, "DeptName", "Sales").
			addObject("Name", "George", "EmpId", 3401, "DeptName", "Finance").
			addObject("Name", "Harriet", "EmpId", 2202, "DeptName", "Production");
		sopremoPlan.getInput(1).
			addObject("Name", "Sales", "Manager", "Harriet").
			addObject("Name", "Production", "Manager", "Charles");
		sopremoPlan.getExpectedOutput(0).
			addObject("Name", "Harry", "EmpId", 3415, "DeptName", "Finance").
			addObject("Name", "George", "EmpId", 3401, "DeptName", "Finance");

		sopremoPlan.run();
	}

	@Test
	public void shouldPerformAntiTwoSourceJoinWithReversedInputs() {
		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		final BinaryBooleanExpression condition = new ElementInSetExpression(
			createPath("1", "DeptName"), Quantor.EXISTS_NOT_IN, createPath("0", "Name"));
		final TwoSourceJoin join = new TwoSourceJoin().withCondition(condition);
		join.setInputs(sopremoPlan.getInputOperators(0, 2));
		sopremoPlan.getOutputOperator(0).setInputs(join);
		sopremoPlan.getInput(1).
			addObject("Name", "Harry", "EmpId", 3415, "DeptName", "Finance").
			addObject("Name", "Sally", "EmpId", 2241, "DeptName", "Sales").
			addObject("Name", "George", "EmpId", 3401, "DeptName", "Finance").
			addObject("Name", "Harriet", "EmpId", 2202, "DeptName", "Production");
		sopremoPlan.getInput(0).
			addObject("Name", "Sales", "Manager", "Harriet").
			addObject("Name", "Production", "Manager", "Charles");
		sopremoPlan.getExpectedOutput(0).
			addObject("Name", "Harry", "EmpId", 3415, "DeptName", "Finance").
			addObject("Name", "George", "EmpId", 3401, "DeptName", "Finance");

		sopremoPlan.run();
	}

	@Test
	public void shouldPerformEquiTwoSourceJoinWithReversedInputs() {
		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		final BinaryBooleanExpression condition = new ComparativeExpression(createPath("1", "id"),
			BinaryOperator.EQUAL, createPath("0", "userid"));
		final TwoSourceJoin join = new TwoSourceJoin().withCondition(condition);
		join.setInputs(sopremoPlan.getInputOperators(0, 2));
		sopremoPlan.getOutputOperator(0).setInputs(join);
		sopremoPlan.getInput(1).
			addObject("name", "Jon Doe", "password", "asdf1234", "id", 1).
			addObject("name", "Jane Doe", "password", "qwertyui", "id", 2).
			addObject("name", "Max Mustermann", "password", "q1w2e3r4", "id", 3);
		sopremoPlan.getInput(0).
			addObject("userid", 1, "url", "code.google.com/p/jaql/").
			addObject("userid", 2, "url", "www.cnn.com").
			addObject("userid", 1, "url", "java.sun.com/javase/6/docs/api/");
		sopremoPlan.getExpectedOutput(0).
			addObject("name", "Jon Doe", "password", "asdf1234", "id", 1, "userid", 1, "url", "code.google.com/p/jaql/").
			addObject("name", "Jon Doe", "password", "asdf1234", "id", 1, "userid", 1, "url",
				"java.sun.com/javase/6/docs/api/").
			addObject("name", "Jane Doe", "password", "qwertyui", "id", 2, "userid", 2, "url", "www.cnn.com");

		sopremoPlan.run();
	}

	@Test
	public void shouldPerformEquiTwoSourceJoin() {
		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		final BinaryBooleanExpression condition = new ComparativeExpression(createPath("0", "id"),
			BinaryOperator.EQUAL, createPath("1", "userid"));
		final TwoSourceJoin join = new TwoSourceJoin().withCondition(condition);
		join.setInputs(sopremoPlan.getInputOperators(0, 2));
		sopremoPlan.getOutputOperator(0).setInputs(join);
		sopremoPlan.getInput(0).
			addObject("name", "Jon Doe", "password", "asdf1234", "id", 1).
			addObject("name", "Jane Doe", "password", "qwertyui", "id", 2).
			addObject("name", "Max Mustermann", "password", "q1w2e3r4", "id", 3);
		sopremoPlan.getInput(1).
			addObject("userid", 1, "url", "code.google.com/p/jaql/").
			addObject("userid", 2, "url", "www.cnn.com").
			addObject("userid", 1, "url", "java.sun.com/javase/6/docs/api/");
		sopremoPlan.getExpectedOutput(0).
			addObject("name", "Jon Doe", "password", "asdf1234", "id", 1, "userid", 1, "url", "code.google.com/p/jaql/").
			addObject("name", "Jon Doe", "password", "asdf1234", "id", 1, "userid", 1, "url",
				"java.sun.com/javase/6/docs/api/").
			addObject("name", "Jane Doe", "password", "qwertyui", "id", 2, "userid", 2, "url", "www.cnn.com");

		sopremoPlan.run();
	}

	@Test
	public void shouldPerformFullOuterTwoSourceJoin() {
		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		// here we set outer join flag
		final EvaluationExpression leftTwoSourceJoinKey = createPath("0", "id");
		final EvaluationExpression rightTwoSourceJoinKey = createPath("1", "userid");
		final BinaryBooleanExpression condition = new ComparativeExpression(leftTwoSourceJoinKey,
			BinaryOperator.EQUAL, rightTwoSourceJoinKey);
		final TwoSourceJoin join = new TwoSourceJoin().withCondition(condition).
			withOuterJoinSources(new ArrayCreation(new JsonStreamExpression(0), new JsonStreamExpression(1)));
		join.setInputs(sopremoPlan.getInputOperators(0, 2));
		sopremoPlan.getOutputOperator(0).setInputs(join);
		sopremoPlan.getInput(0).
			addObject("name", "Jon Doe", "password", "asdf1234", "id", 1).
			addObject("name", "Jane Doe", "password", "qwertyui", "id", 2).
			addObject("name", "Max Mustermann", "password", "q1w2e3r4", "id", 3);
		sopremoPlan.getInput(1).
			addObject("userid", 1, "url", "code.google.com/p/jaql/").
			addObject("userid", 2, "url", "www.cnn.com").
			addObject("userid", 4, "url", "www.nbc.com").
			addObject("userid", 1, "url", "java.sun.com/javase/6/docs/api/");
		sopremoPlan.getExpectedOutput(0).
			addObject("name", "Jon Doe", "password", "asdf1234", "id", 1, "userid", 1, "url", "code.google.com/p/jaql/").
			addObject("name", "Jon Doe", "password", "asdf1234", "id", 1, "userid", 1, "url",
				"java.sun.com/javase/6/docs/api/").
			addObject("name", "Jane Doe", "password", "qwertyui", "id", 2, "userid", 2, "url", "www.cnn.com").
			addObject("name", "Max Mustermann", "password", "q1w2e3r4", "id", 3).
			addObject("userid", 4, "url", "www.nbc.com");

		sopremoPlan.run();
	}

	@Test
	public void shouldPerformLeftOuterTwoSourceJoin() {
		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		// here we set outer join flag
		final EvaluationExpression leftTwoSourceJoinKey = createPath("0", "id");
		final BinaryBooleanExpression condition = new ComparativeExpression(leftTwoSourceJoinKey,
			BinaryOperator.EQUAL, createPath("1", "userid"));
		final TwoSourceJoin join = new TwoSourceJoin().withCondition(condition).
			withOuterJoinSources(new JsonStreamExpression(0));
		join.setInputs(sopremoPlan.getInputOperators(0, 2));
		sopremoPlan.getOutputOperator(0).setInputs(join);
		sopremoPlan.getInput(0).
			addObject("name", "Jon Doe", "password", "asdf1234", "id", 1).
			addObject("name", "Jane Doe", "password", "qwertyui", "id", 2).
			addObject("name", "Max Mustermann", "password", "q1w2e3r4", "id", 3);
		sopremoPlan.getInput(1).
			addObject("userid", 1, "url", "code.google.com/p/jaql/").
			addObject("userid", 2, "url", "www.cnn.com").
			addObject("userid", 1, "url", "java.sun.com/javase/6/docs/api/");
		sopremoPlan.getExpectedOutput(0).
			addObject("name", "Jon Doe", "password", "asdf1234", "id", 1, "userid", 1, "url", "code.google.com/p/jaql/").
			addObject("name", "Jon Doe", "password", "asdf1234", "id", 1, "userid", 1, "url",
				"java.sun.com/javase/6/docs/api/").
			addObject("name", "Jane Doe", "password", "qwertyui", "id", 2, "userid", 2, "url", "www.cnn.com").
			addObject("name", "Max Mustermann", "password", "q1w2e3r4", "id", 3);

		sopremoPlan.run();
	}

	@Test
	public void shouldPerformRightOuterTwoSourceJoin() {
		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		// here we set outer join flag
		final EvaluationExpression rightTwoSourceJoinKey = createPath("1", "userid");
		final BinaryBooleanExpression condition = new ComparativeExpression(createPath("0", "id"),
			BinaryOperator.EQUAL, rightTwoSourceJoinKey);
		final TwoSourceJoin join = new TwoSourceJoin().withCondition(condition).
			withOuterJoinSources(new JsonStreamExpression(1));
		join.setInputs(sopremoPlan.getInputOperators(0, 2));
		sopremoPlan.getOutputOperator(0).setInputs(join);
		sopremoPlan.getInput(0).
			addObject("name", "Jon Doe", "password", "asdf1234", "id", 1).
			addObject("name", "Jane Doe", "password", "qwertyui", "id", 2).
			addObject("name", "Max Mustermann", "password", "q1w2e3r4", "id", 3);
		sopremoPlan.getInput(1).
			addObject("userid", 1, "url", "code.google.com/p/jaql/").
			addObject("userid", 2, "url", "www.cnn.com").
			addObject("userid", 4, "url", "www.nbc.com").
			addObject("userid", 1, "url", "java.sun.com/javase/6/docs/api/");
		sopremoPlan.getExpectedOutput(0).
			addObject("name", "Jon Doe", "password", "asdf1234", "id", 1, "userid", 1, "url", "code.google.com/p/jaql/").
			addObject("name", "Jon Doe", "password", "asdf1234", "id", 1, "userid", 1, "url",
				"java.sun.com/javase/6/docs/api/").
			addObject("name", "Jane Doe", "password", "qwertyui", "id", 2, "userid", 2, "url", "www.cnn.com").
			addObject("userid", 4, "url", "www.nbc.com");

		sopremoPlan.run();
	}

	@Test
	public void shouldPerformSemiTwoSourceJoin() {
		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		final BinaryBooleanExpression condition = new ElementInSetExpression(createPath("0",
			"DeptName"), Quantor.EXISTS_IN, createPath("1", "Name"));
		final TwoSourceJoin join = new TwoSourceJoin().withCondition(condition);
		join.setInputs(sopremoPlan.getInputOperators(0, 2));
		sopremoPlan.getOutputOperator(0).setInputs(join);
		sopremoPlan.getInput(0).
			addObject("Name", "Harry", "EmpId", 3415, "DeptName", "Finance").
			addObject("Name", "Sally", "EmpId", 2241, "DeptName", "Sales").
			addObject("Name", "George", "EmpId", 3401, "DeptName", "Finance").
			addObject("Name", "Harriet", "EmpId", 2202, "DeptName", "Production");
		sopremoPlan.getInput(1).
			addObject("Name", "Sales", "Manager", "Harriet").
			addObject("Name", "Production", "Manager", "Charles");
		sopremoPlan.getExpectedOutput(0).
			addObject("Name", "Sally", "EmpId", 2241, "DeptName", "Sales").
			addObject("Name", "Harriet", "EmpId", 2202, "DeptName", "Production");

		sopremoPlan.run();
	}

	@Test
	public void shouldPerformThetaTwoSourceJoin() {
		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		final BinaryBooleanExpression condition = new ComparativeExpression(createPath("0", "id"),
			BinaryOperator.LESS, createPath("1", "userid"));
		final TwoSourceJoin join = new TwoSourceJoin().withCondition(condition);
		join.setInputs(sopremoPlan.getInputOperators(0, 2));
		sopremoPlan.getOutputOperator(0).setInputs(join);
		sopremoPlan.getInput(0).
			addObject("name", "Jon Doe", "password", "asdf1234", "id", 1).
			addObject("name", "Jane Doe", "password", "qwertyui", "id", 2).
			addObject("name", "Max Mustermann", "password", "q1w2e3r4", "id", 3);
		sopremoPlan.getInput(1).
			addObject("userid", 1, "url", "code.google.com/p/jaql/").
			addObject("userid", 2, "url", "www.cnn.com").
			addObject("userid", 4, "url", "www.nbc.com").
			addObject("userid", 1, "url", "java.sun.com/javase/6/docs/api/");
		sopremoPlan.getExpectedOutput(0).
			addObject("name", "Jon Doe", "password", "asdf1234", "id", 1, "userid", 2, "url", "www.cnn.com").
			addObject("name", "Jon Doe", "password", "asdf1234", "id", 1, "userid", 4, "url", "www.nbc.com").
			addObject("name", "Jane Doe", "password", "qwertyui", "id", 2, "userid", 4, "url", "www.nbc.com").
			addObject("name", "Max Mustermann", "password", "q1w2e3r4", "id", 3, "userid", 4, "url", "www.nbc.com");

		sopremoPlan.run();
	}

	@Test
	public void shouldPerformFullOuterTwoSourceJoinWithReversedInputs() {
		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		// here we set outer join flag
		final EvaluationExpression leftTwoSourceJoinKey = createPath("1", "id");
		final EvaluationExpression rightTwoSourceJoinKey = createPath("0", "userid");
		final BinaryBooleanExpression condition = new ComparativeExpression(leftTwoSourceJoinKey,
			BinaryOperator.EQUAL, rightTwoSourceJoinKey);
		final TwoSourceJoin join = new TwoSourceJoin().withCondition(condition).
			withOuterJoinSources(new ArrayCreation(new JsonStreamExpression(0), new JsonStreamExpression(1)));
		join.setInputs(sopremoPlan.getInputOperators(0, 2));
		sopremoPlan.getOutputOperator(0).setInputs(join);
		sopremoPlan.getInput(1).
			addObject("name", "Jon Doe", "password", "asdf1234", "id", 1).
			addObject("name", "Jane Doe", "password", "qwertyui", "id", 2).
			addObject("name", "Max Mustermann", "password", "q1w2e3r4", "id", 3);
		sopremoPlan.getInput(0).
			addObject("userid", 1, "url", "code.google.com/p/jaql/").
			addObject("userid", 2, "url", "www.cnn.com").
			addObject("userid", 4, "url", "www.nbc.com").
			addObject("userid", 1, "url", "java.sun.com/javase/6/docs/api/");
		sopremoPlan.getExpectedOutput(0).
			addObject("name", "Jon Doe", "password", "asdf1234", "id", 1, "userid", 1, "url", "code.google.com/p/jaql/").
			addObject("name", "Jon Doe", "password", "asdf1234", "id", 1, "userid", 1, "url",
				"java.sun.com/javase/6/docs/api/").
			addObject("name", "Jane Doe", "password", "qwertyui", "id", 2, "userid", 2, "url", "www.cnn.com").
			addObject("name", "Max Mustermann", "password", "q1w2e3r4", "id", 3).
			addObject("userid", 4, "url", "www.nbc.com");

		sopremoPlan.run();
	}

	@Test
	public void shouldPerformLeftOuterTwoSourceJoinWithReversedInputs() {
		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		// here we set outer join flag
		final EvaluationExpression leftTwoSourceJoinKey = createPath("1", "id");
		final BinaryBooleanExpression condition = new ComparativeExpression(leftTwoSourceJoinKey,
			BinaryOperator.EQUAL, createPath("0", "userid"));
		final TwoSourceJoin join = new TwoSourceJoin().withCondition(condition).
			withOuterJoinSources(new JsonStreamExpression(1));
		join.setInputs(sopremoPlan.getInputOperators(0, 2));
		sopremoPlan.getOutputOperator(0).setInputs(join);
		sopremoPlan.getInput(1).
			addObject("name", "Jon Doe", "password", "asdf1234", "id", 1).
			addObject("name", "Jane Doe", "password", "qwertyui", "id", 2).
			addObject("name", "Max Mustermann", "password", "q1w2e3r4", "id", 3);
		sopremoPlan.getInput(0).
			addObject("userid", 1, "url", "code.google.com/p/jaql/").
			addObject("userid", 2, "url", "www.cnn.com").
			addObject("userid", 1, "url", "java.sun.com/javase/6/docs/api/");
		sopremoPlan.getExpectedOutput(0).
			addObject("name", "Jon Doe", "password", "asdf1234", "id", 1, "userid", 1, "url", "code.google.com/p/jaql/").
			addObject("name", "Jon Doe", "password", "asdf1234", "id", 1, "userid", 1, "url",
				"java.sun.com/javase/6/docs/api/").
			addObject("name", "Jane Doe", "password", "qwertyui", "id", 2, "userid", 2, "url", "www.cnn.com").
			addObject("name", "Max Mustermann", "password", "q1w2e3r4", "id", 3);

		sopremoPlan.run();
	}

	@Test
	public void shouldPerformRightOuterTwoSourceJoinWithReversedInputs() {
		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		// here we set outer join flag
		final EvaluationExpression rightTwoSourceJoinKey = createPath("0", "userid");
		final BinaryBooleanExpression condition = new ComparativeExpression(createPath("1", "id"),
			BinaryOperator.EQUAL, rightTwoSourceJoinKey);
		final TwoSourceJoin join = new TwoSourceJoin().withCondition(condition).
			withOuterJoinSources(new JsonStreamExpression(0));
		join.setInputs(sopremoPlan.getInputOperators(0, 2));
		sopremoPlan.getOutputOperator(0).setInputs(join);
		sopremoPlan.getInput(1).
			addObject("name", "Jon Doe", "password", "asdf1234", "id", 1).
			addObject("name", "Jane Doe", "password", "qwertyui", "id", 2).
			addObject("name", "Max Mustermann", "password", "q1w2e3r4", "id", 3);
		sopremoPlan.getInput(0).
			addObject("userid", 1, "url", "code.google.com/p/jaql/").
			addObject("userid", 2, "url", "www.cnn.com").
			addObject("userid", 4, "url", "www.nbc.com").
			addObject("userid", 1, "url", "java.sun.com/javase/6/docs/api/");
		sopremoPlan.getExpectedOutput(0).
			addObject("name", "Jon Doe", "password", "asdf1234", "id", 1, "userid", 1, "url", "code.google.com/p/jaql/").
			addObject("name", "Jon Doe", "password", "asdf1234", "id", 1, "userid", 1, "url",
				"java.sun.com/javase/6/docs/api/").
			addObject("name", "Jane Doe", "password", "qwertyui", "id", 2, "userid", 2, "url", "www.cnn.com").
			addObject("userid", 4, "url", "www.nbc.com");

		sopremoPlan.run();
	}

	@Test
	public void shouldPerformSemiTwoSourceJoinWithReversedInputs() {
		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		final BinaryBooleanExpression condition = new ElementInSetExpression(createPath("1",
			"DeptName"), Quantor.EXISTS_IN, createPath("0", "Name"));
		final TwoSourceJoin join = new TwoSourceJoin().withCondition(condition);
		join.setInputs(sopremoPlan.getInputOperators(0, 2));
		sopremoPlan.getOutputOperator(0).setInputs(join);
		sopremoPlan.getInput(1).
			addObject("Name", "Harry", "EmpId", 3415, "DeptName", "Finance").
			addObject("Name", "Sally", "EmpId", 2241, "DeptName", "Sales").
			addObject("Name", "George", "EmpId", 3401, "DeptName", "Finance").
			addObject("Name", "Harriet", "EmpId", 2202, "DeptName", "Production");
		sopremoPlan.getInput(0).
			addObject("Name", "Sales", "Manager", "Harriet").
			addObject("Name", "Production", "Manager", "Charles");
		sopremoPlan.getExpectedOutput(0).
			addObject("Name", "Sally", "EmpId", 2241, "DeptName", "Sales").
			addObject("Name", "Harriet", "EmpId", 2202, "DeptName", "Production");

		sopremoPlan.run();
	}

	@Test
	public void shouldPerformThetaTwoSourceJoinWithReversedInputs() {
		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		final BinaryBooleanExpression condition = new ComparativeExpression(createPath("1", "id"),
			BinaryOperator.LESS, createPath("0", "userid"));
		final TwoSourceJoin join = new TwoSourceJoin().withCondition(condition);
		join.setInputs(sopremoPlan.getInputOperators(0, 2));
		sopremoPlan.getOutputOperator(0).setInputs(join);
		sopremoPlan.getInput(1).
			addObject("name", "Jon Doe", "password", "asdf1234", "id", 1).
			addObject("name", "Jane Doe", "password", "qwertyui", "id", 2).
			addObject("name", "Max Mustermann", "password", "q1w2e3r4", "id", 3);
		sopremoPlan.getInput(0).
			addObject("userid", 1, "url", "code.google.com/p/jaql/").
			addObject("userid", 2, "url", "www.cnn.com").
			addObject("userid", 4, "url", "www.nbc.com").
			addObject("userid", 1, "url", "java.sun.com/javase/6/docs/api/");
		sopremoPlan.getExpectedOutput(0).
			addObject("name", "Jon Doe", "password", "asdf1234", "id", 1, "userid", 2, "url", "www.cnn.com").
			addObject("name", "Jon Doe", "password", "asdf1234", "id", 1, "userid", 4, "url", "www.nbc.com").
			addObject("name", "Jane Doe", "password", "qwertyui", "id", 2, "userid", 4, "url", "www.nbc.com").
			addObject("name", "Max Mustermann", "password", "q1w2e3r4", "id", 3, "userid", 4, "url", "www.nbc.com");

		sopremoPlan.run();
	}

}
