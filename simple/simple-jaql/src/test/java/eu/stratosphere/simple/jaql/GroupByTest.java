package eu.stratosphere.simple.jaql;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import eu.stratosphere.dag.TraverseListener;
import eu.stratosphere.dag.Traverser;
import eu.stratosphere.sopremo.BooleanExpression;
import eu.stratosphere.sopremo.Condition;
import eu.stratosphere.sopremo.JsonPath;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.OperatorNavigator;
import eu.stratosphere.sopremo.BooleanExpression.BinaryOperator;
import eu.stratosphere.sopremo.JsonPath.ArrayCreation;
import eu.stratosphere.sopremo.Plan;
import eu.stratosphere.sopremo.Transformation;
import eu.stratosphere.sopremo.ValueAssignment;
import eu.stratosphere.sopremo.operator.Aggregation;
import eu.stratosphere.sopremo.operator.DataType;
import eu.stratosphere.sopremo.operator.Selection;
import eu.stratosphere.sopremo.operator.Sink;
import eu.stratosphere.sopremo.operator.Source;

public class GroupByTest extends ParserTestCase {
	@Test
	public void shouldParseSimpleGroupBy() {
		Transformation transformation = new Transformation();
		transformation.addMapping(new ValueAssignment(new JsonPath.Function("count", new JsonPath.Input(0))));
		assertParseResult(new Aggregation(transformation, Aggregation.NO_GROUPING, employeeSource()), employeeJaql()
			+ "employees -> group into count($);");
	}

	public String employeeJaql() {
		return "employees = [   {id:1, dept: 1, income:12000},   {id:2, dept: 1, income:13000},   {id:3, dept: 2, income:15000},   {id:4, dept: 1, income:10000},   {id:5, dept: 3, income:8000},   {id:6, dept: 2, income:5000},   {id:7, dept: 1, income:24000} ]; ";
	}

	public Source employeeSource() {
		return new Source(createJsonArray(
			createObject("id", 1L, "dept", 1L, "income", 12000L),
			createObject("id", 2L, "dept", 1L, "income", 13000L),
			createObject("id", 3L, "dept", 2L, "income", 15000L),
			createObject("id", 4L, "dept", 1L, "income", 10000L),
			createObject("id", 5L, "dept", 3L, "income", 8000L),
			createObject("id", 6L, "dept", 2L, "income", 5000L),
			createObject("id", 7L, "dept", 1L, "income", 24000L)));
	}

	@Test
	public void shouldParseGroupByWithSingleSource() {
		Transformation transformation = new Transformation();
		transformation.addMapping(new ValueAssignment("d", createPath("$", "d")));
		transformation.addMapping(new ValueAssignment("total",
			new JsonPath.Function("sum", createPath("$", "[*]", "income"))));
		assertParseResult(new Aggregation(transformation, Arrays.asList(createPath("$", "dept")), employeeSource()),
			employeeJaql() + "employees -> group by d = $.dept into {d, total: sum($[*].income)};");
	}

	@Test
	public void shouldParseGroupByWithSingleSourceAndRenaming() {
		Transformation transformation = new Transformation();
		transformation.addMapping(new ValueAssignment("d", createPath("$", "d")));
		transformation.addMapping(new ValueAssignment("total",
			new JsonPath.Function("sum", createPath("$", "[*]", "income"))));
		assertParseResult(
			new Aggregation(transformation, Arrays.asList(createPath("$", "dept")), employeeSource()),
			employeeJaql()
				+ "employees -> group each emp by d = emp.dept as deptEmps into {d, total: sum(deptEmps[*].income)};");
	}

	@Test
	public void shouldParseGroupByWithMultipleSources() {

		Source depts = new Source(createJsonArray(
			createObject("did", 1L, "name", "development"),
			createObject("did", 2L, "name", "marketing"),
			createObject("did", 3L, "name", "sales")));
		String deptJaql = "depts = [   {did: 1, name: \"development\"},   {did: 2, name: \"marketing\"},   {did: 3, name: \"sales\"} ]; ";

		Transformation transformation = new Transformation();
		transformation.addMapping(new ValueAssignment("dept", createPath("0", "dept")));
		transformation.addMapping(new ValueAssignment("deptName", createPath("1", "[0]", "name")));
		transformation.addMapping(new ValueAssignment("emps", createPath("0", "[*]", "id")));
		transformation.addMapping(new ValueAssignment("numEmps",
			new JsonPath.Function("count", createPath("0"))));
		assertParseResult(
			new Aggregation(transformation, Arrays.asList(createPath("0", "dept"), createPath("1", "did")), employeeSource(), depts),
			employeeJaql() + deptJaql +
				"group employees by g = $.dept as es, " +
				"depts     by g = $.did  as ds " +
				"into { dept: g, deptName: ds[0].name, emps: es[*].id, numEmps: count(es) };");
	}

}
