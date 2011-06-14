package eu.stratosphere.simple.jaql;

import org.junit.Test;

import eu.stratosphere.sopremo.base.DataType;
import eu.stratosphere.sopremo.base.Selection;
import eu.stratosphere.sopremo.base.Sink;
import eu.stratosphere.sopremo.base.Source;
import eu.stratosphere.sopremo.expressions.ComparativeExpression;
import eu.stratosphere.sopremo.expressions.ComparativeExpression.BinaryOperator;
import eu.stratosphere.sopremo.expressions.ConditionalExpression;
import eu.stratosphere.sopremo.expressions.ConditionalExpression.Combination;
import eu.stratosphere.sopremo.expressions.ConstantExpression;
import eu.stratosphere.sopremo.expressions.InputSelection;
import eu.stratosphere.sopremo.expressions.UnaryExpression;

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
		ConditionalExpression selectionCondition = new ConditionalExpression(new ComparativeExpression(new InputSelection(0), BinaryOperator.EQUAL,
			new ConstantExpression(2L)));
		assertParseResult(new Selection(selectionCondition, new Source(createJsonArray(1L, 2L, 3L))),
			"[1, 2, 3] -> filter $ == 2");
	}

	@Test
	public void shouldParseCombinedFilter() {
		ConditionalExpression selectionCondition = new ConditionalExpression(Combination.OR,
			new UnaryExpression(createPath("$", "mgr")),
			new ComparativeExpression(createPath("$", "income"), BinaryOperator.GREATER, new ConstantExpression(30000L)));
		assertParseResult(new Selection(selectionCondition, employeeSource()),
			employeeJaql() + "employees -> filter $.mgr or $.income > 30000;");
	}

	@Test
	public void shouldParseCombinedFilterWithIterationVariable() {
		ConditionalExpression selectionCondition = new ConditionalExpression(Combination.OR,
			new UnaryExpression(createPath("$", "mgr")),
			new ComparativeExpression(createPath("$", "income"), BinaryOperator.GREATER, new ConstantExpression(30000L)));
		assertParseResult(new Selection(selectionCondition, employeeSource()),
			employeeJaql() + "employees -> filter each emp emp.mgr or emp.income > 30000;");
	}

	@Test
	public void shouldParseCombinedFilterWithIterationVariableAndParens() {
		ConditionalExpression selectionCondition = new ConditionalExpression(Combination.OR,
			new UnaryExpression(createPath("$", "mgr")),
			new ComparativeExpression(createPath("$", "income"), BinaryOperator.GREATER, new ConstantExpression(30000L)));
		assertParseResult(new Selection(selectionCondition, employeeSource()),
			employeeJaql() + "employees -> filter each emp (emp.mgr or emp.income > 30000);");
	}

	@Test
	public void shouldParseFilterPipeline() {
		ConditionalExpression selectionCondition = new ConditionalExpression(new ComparativeExpression(createPath("$", "name"),
			BinaryOperator.NOT_EQUAL, new ConstantExpression("")));
		Selection selection = new Selection(selectionCondition, new Source(DataType.HDFS, "in.json"));
		assertParseResult(new Sink(DataType.HDFS, "out.json", selection),
			"read(hdfs(\"in.json\")) -> filter $.name != \"\" -> write(hdfs(\"out.json\"))");
	}
}
