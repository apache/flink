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
import eu.stratosphere.sopremo.operator.DataType;
import eu.stratosphere.sopremo.operator.Selection;
import eu.stratosphere.sopremo.operator.Sink;
import eu.stratosphere.sopremo.operator.Source;

public class FilterTest extends ParserTestCase {
	@Test
	public void shouldParseSimpleFilter() {
		Condition selectionCondition = new Condition(new Comparison(new JsonPath.Input(0), BinaryOperator.EQUAL,
			new JsonPath.Constant(2L)));
		assertParseResult(new Selection(selectionCondition, new Source(createJsonArray(1L, 2L, 3L))),
			"[1, 2, 3] -> filter $ == 2");
	}

	@Test
	public void shouldParseFilterPipeline() {
		Condition selectionCondition = new Condition(new Comparison(new JsonPath.Input(0), BinaryOperator.NOT_EQUAL,
			new JsonPath.Constant("")));
		Selection selection = new Selection(selectionCondition, new Source(DataType.HDFS, "in.json"));
		assertParseResult(new Sink(DataType.HDFS, "out.json", selection),
			"read(hdfs(\"in.json\")) -> filter $.name != \"\" -> write(hdfs(\"out.json\"))");
	}
}
