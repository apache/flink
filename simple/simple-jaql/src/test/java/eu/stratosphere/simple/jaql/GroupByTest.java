package eu.stratosphere.simple.jaql;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.util.Collection;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import eu.stratosphere.dag.TraverseListener;
import eu.stratosphere.dag.Traverser;
import eu.stratosphere.sopremo.Comparison;
import eu.stratosphere.sopremo.Condition;
import eu.stratosphere.sopremo.JsonPath;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.OperatorNavigator;
import eu.stratosphere.sopremo.Comparison.BinaryOperator;
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
	// test if this script does still work in jaql >0.5
	// @Test
	// public void shouldParseSimpleGroupBy() {
	// Source source = new Source(createJsonArray(createObject("a", 1L, "b", 2L), createObject("a", 3L, "b", 4L)));
	// assertParseResult(new Aggregation(null, null, source), "[{a: 1, b: 2}, {a: 3, b: 4}] -> group into count($)");
	// }

	@Test
	public void shouldParseGroupByWithSingleSource() {
		Source source = new Source(createJsonArray(createObject("a", 1L, "b", 2L), createObject("a", 1L, "b", 4L)));
		Transformation transformation = new Transformation();
		transformation.addMapping(new ValueAssignment("a", createPath("$", "a")));
		transformation.addMapping(new ValueAssignment("total",
			new JsonPath.Function("sum", createPath("$", "[*]", "b"))));
		assertParseResult(new Aggregation(transformation, createPath("$", "a"), source),
			"[{a: 1, b: 2}, {a: 1, b: 4}] -> group by a = $.a into { a, total: sum($[*].b) }");
	}
}
