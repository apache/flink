package eu.stratosphere.simple.jaql;

import org.junit.Test;

import eu.stratosphere.sopremo.Comparison;
import eu.stratosphere.sopremo.Comparison.BinaryOperator;
import eu.stratosphere.sopremo.Condition;
import eu.stratosphere.sopremo.Condition.Combination;
import eu.stratosphere.sopremo.UnaryExpression;
import eu.stratosphere.sopremo.expressions.Constant;
import eu.stratosphere.sopremo.expressions.Input;
import eu.stratosphere.sopremo.operator.DataType;
import eu.stratosphere.sopremo.operator.Selection;
import eu.stratosphere.sopremo.operator.Sink;
import eu.stratosphere.sopremo.operator.Source;

public class FilterTest extends ParserTestCase {
	private static String employeeJaql() {
		return "employees = [  {name: \"Jon Doe\", income: 20000, mgr: false},  {name: \"Vince Wayne\", income: 32500, mgr: false},  {name: \"Jane Dean\", income: 72000, mgr: true},  {name: \"Alex Smith\", income: 25000, mgr: false} ]; ";
	}

	private static Source employeeSource() {
		return new Source(createJsonArray(
			createObject("name", "Jon Doe", "income", 20000L, "mgr", false),
			createObject("name", "Vince Wayne", "income", 32500L, "mgr", false),
			createObject("name", "Jane Dean", "income", 72000L, "mgr", true),
			createObject("name", "Alex Smith", "income", 25000L, "mgr", false)));
	}

	@Test
	public void shouldParseAdhocInputFilter() {
		Condition selectionCondition = new Condition(new Comparison(new Input(0), BinaryOperator.EQUAL,
			new Constant(2L)));
		assertParseResult(new Selection(selectionCondition, new Source(createJsonArray(1L, 2L, 3L))),
			"[1, 2, 3] -> filter $ == 2");
	}

	@Test
	public void shouldParseCombinedFilter() {
		Condition selectionCondition = new Condition(Combination.OR,
			new UnaryExpression(createPath("$", "mgr")),
			new Comparison(createPath("$", "income"), BinaryOperator.GREATER, new Constant(30000L)));
		assertParseResult(new Selection(selectionCondition, employeeSource()),
			employeeJaql() + "employees -> filter $.mgr or $.income > 30000;");
	}

	@Test
	public void shouldParseCombinedFilterWithIterationVariable() {
		Condition selectionCondition = new Condition(Combination.OR,
			new UnaryExpression(createPath("$", "mgr")),
			new Comparison(createPath("$", "income"), BinaryOperator.GREATER, new Constant(30000L)));
		assertParseResult(new Selection(selectionCondition, employeeSource()),
			employeeJaql() + "employees -> filter each emp emp.mgr or emp.income > 30000;");
	}

	@Test
	public void shouldParseCombinedFilterWithIterationVariableAndParens() {
		Condition selectionCondition = new Condition(Combination.OR,
			new UnaryExpression(createPath("$", "mgr")),
			new Comparison(createPath("$", "income"), BinaryOperator.GREATER, new Constant(30000L)));
		assertParseResult(new Selection(selectionCondition, employeeSource()),
			employeeJaql() + "employees -> filter each emp (emp.mgr or emp.income > 30000);");
	}

	@Test
	public void shouldParseFilterPipeline() {
		Condition selectionCondition = new Condition(new Comparison(createPath("$", "name"),
			BinaryOperator.NOT_EQUAL, new Constant("")));
		Selection selection = new Selection(selectionCondition, new Source(DataType.HDFS, "in.json"));
		assertParseResult(new Sink(DataType.HDFS, "out.json", selection),
			"read(hdfs(\"in.json\")) -> filter $.name != \"\" -> write(hdfs(\"out.json\"))");
	}
}
