package eu.stratosphere.sopremo;

import static eu.stratosphere.sopremo.JsonUtil.createArrayNode;
import static eu.stratosphere.sopremo.JsonUtil.createCompactArray;
import static eu.stratosphere.sopremo.JsonUtil.createValueNode;

import java.math.BigDecimal;

import junit.framework.Assert;

import org.junit.Test;

import eu.stratosphere.sopremo.jsondatamodel.DoubleNode;
import eu.stratosphere.sopremo.jsondatamodel.IntNode;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;
import eu.stratosphere.sopremo.jsondatamodel.TextNode;

/**
 * Tests {@link BuiltinFunctions}
 */
public class BuiltinFunctionsTest {

	protected EvaluationContext context = new EvaluationContext();

	/**
	 * 
	 */
	@Test
	public void shouldCoerceDataWhenSumming() {
		Assert.assertEquals(
			6.4,
			((DoubleNode) BuiltinFunctions.sum(createArrayNode(1.1, 2, new BigDecimal("3.3")))).getDoubleValue(), 0.01);
	}

	/**
	 * 
	 */
	@Test
	public void shouldConcatenateObjects() {
		Assert.assertEquals(
			createValueNode("bla1blu2"),
			BuiltinFunctions.concat(new JsonNode[] {
				createValueNode("bla"),
				createValueNode(1),
				createValueNode("blu"),
				createValueNode(2), }));
	}

	/**
	 * 
	 */
	@Test
	public void shouldConcatenateStrings() {
		Assert.assertEquals(
			createValueNode("blabliblu"),
			BuiltinFunctions.concat(new JsonNode[] {
				createValueNode("bla"),
				createValueNode("bli"),
				createValueNode("blu") }));
	}

	/**
	 * 
	 */
	@Test
	public void shouldCountCompactArray() {
		Assert.assertEquals(
			createValueNode(3),
			BuiltinFunctions.count(createCompactArray(1, 2, 3)));
	}

	/**
	 * 
	 */
	@Test
	public void shouldCountNormalArray() {
		Assert.assertEquals(
			createValueNode(3),
			BuiltinFunctions.count(createArrayNode(1, 2, 3)));
	}

	/**
	 * 
	 */
	@Test
	public void shouldCountStreamArray() {
		Assert.assertEquals(
			createValueNode(3),
			BuiltinFunctions.count(JsonUtil.createArrayNode(1, 2, 3)));
	}

	/**
	 * 
	 */
	@Test
	public void shouldCountZeroForEmptyArray() {
		Assert.assertEquals(
			createValueNode(0),
			BuiltinFunctions.count(createArrayNode()));
	}

	// /**
	// *
	// */
	// @Test(expected = ClassCastException.class)
	// public void shouldFailIfIncompatible() {
	// BuiltinFunctions.sort(createArrayNode(3.14, 4, 1.2, 2));
	// }

	/**
	 * 
	 */
	@Test(expected = ClassCastException.class)
	public void shouldFailToSumIfNonNumbers() {
		BuiltinFunctions.sum(createArrayNode("test"));
	}

	/**
	 * 
	 */
	@Test
	public void shouldReturnEmptyStringWhenConcatenatingEmptyArray() {
		Assert.assertEquals(
			createValueNode(""), BuiltinFunctions.concat(new JsonNode[0]));
	}

	/**
	 * 
	 */
	@Test
	public void shouldSortArrays() {
		Assert.assertEquals(
			createArrayNode(new Number[] { 1, 2.4 }, new Number[] { 1, 3.4 }, new Number[] { 2, 2.4 },
				new Number[] { 2, 2.4, 3 }),
			BuiltinFunctions.sort(createArrayNode(new Number[] { 1, 3.4 }, new Number[] { 2, 2.4 },
				new Number[] { 1, 2.4 }, new Number[] { 2, 2.4, 3 })));
	}

	/**
	 * 
	 */
	@Test
	public void shouldSortDoubles() {
		Assert.assertEquals(
			createArrayNode(1.2, 2.0, 3.14, 4.5),
			BuiltinFunctions.sort(createArrayNode(3.14, 4.5, 1.2, 2.0)));
	}

	/**
	 * 
	 */
	@Test
	public void shouldSortEmptyArray() {
		Assert.assertEquals(
			createArrayNode(),
			BuiltinFunctions.sort(createArrayNode()));
	}

	/**
	 * 
	 */
	@Test
	public void shouldSortIntegers() {
		Assert.assertEquals(
			createArrayNode(1, 2, 3, 4),
			BuiltinFunctions.sort(createArrayNode(3, 4, 1, 2)));
	}

	/**
	 * 
	 */
	@Test
	public void shouldSumDoubles() {
		Assert.assertEquals(
			6.6,
			((DoubleNode) BuiltinFunctions.sum(createArrayNode(1.1, 2.2, 3.3))).getDoubleValue(), 0.01);
	}

	/**
	 * 
	 */
	@Test
	public void shouldSumEmptyArrayToZero() {
		Assert.assertEquals(
			createValueNode(0),
			BuiltinFunctions.sum(createArrayNode()));
	}

	/**
	 * 
	 */
	@Test
	public void shouldSumIntegers() {
		Assert.assertEquals(
			createValueNode(6),
			BuiltinFunctions.sum(createArrayNode(1, 2, 3)));
	}

	/**
	 * 
	 */
	@Test
	public void shouldUnionAllCompactArrays() {
		Assert.assertEquals(
			createArrayNode(1, 2, 3, 4, 5, 6),
			BuiltinFunctions.unionAll(createCompactArray(1, 2, 3), createCompactArray(4, 5),
				createCompactArray(6)));
	}

	/**
	 * Very rare case...
	 */
	@Test
	public void shouldUnionAllMixedArrayTypes() {
		Assert.assertEquals(
			JsonUtil.createArrayNode(1, 2, 3, 4, 5, 6),
			BuiltinFunctions.unionAll(createArrayNode(1, 2, 3), createCompactArray(4, 5),
				JsonUtil.createArrayNode(6)));
	}

	/**
	 * 
	 */
	@Test
	public void shouldUnionAllNormalArrays() {
		Assert.assertEquals(
			createArrayNode(1, 2, 3, 4, 5, 6),
			BuiltinFunctions.unionAll(createArrayNode(1, 2, 3), createArrayNode(4, 5),
				createArrayNode(6)));
	}

	/**
	 * 
	 */
	@Test
	public void shouldUnionAllStreamArrays() {
		Assert.assertEquals(
			JsonUtil.createArrayNode(1, 2, 3, 4, 5, 6),
			BuiltinFunctions.unionAll(JsonUtil.createArrayNode(1, 2, 3), JsonUtil.createArrayNode(4, 5),
				JsonUtil.createArrayNode(6)));
	}

	@Test
	public void shouldCalculateAvg() {
		BuiltinFunctions.AVERAGE.initialize();
		BuiltinFunctions.AVERAGE.aggregate(IntNode.valueOf(50), this.context);
		BuiltinFunctions.AVERAGE.aggregate(IntNode.valueOf(25), this.context);
		BuiltinFunctions.AVERAGE.aggregate(IntNode.valueOf(75), this.context);

		Assert.assertEquals(DoubleNode.valueOf(50.0), BuiltinFunctions.AVERAGE.getFinalAggregate());
	}

	@Test
	public void shouldCalculateAvgWithDifferentNodes() {
		BuiltinFunctions.AVERAGE.initialize();

		for (int i = 1; i < 500; i++)
			BuiltinFunctions.AVERAGE.aggregate(i % 2 == 0 ? IntNode.valueOf(i) : DoubleNode.valueOf(i),
				this.context);

		Assert.assertEquals(DoubleNode.valueOf(250), BuiltinFunctions.AVERAGE.getFinalAggregate());
	}

	// Assertion failed: Expected <NaN> but was: <NaN>
	// @Test
	// public void shouldReturnNanIfAvgNotAggregated() {
	// BuiltinFunctions.AVERAGE.initialize();
	//
	// Assert.assertEquals(DoubleNode.valueOf(Double.NaN),
	// BuiltinFunctions.AVERAGE.getFinalAggregate());
	// }

	@Test
	public void shouldReturnCorrectSubstring() {
		final JsonNode result = BuiltinFunctions.substring(TextNode.valueOf("0123456789"), IntNode.valueOf(3),
			IntNode.valueOf(6));

		Assert.assertEquals(TextNode.valueOf("345"), result);
	}

	@Test
	public void shouldCreateRightCamelCaseRepresentation() {
		Assert.assertEquals(
			TextNode.valueOf("This Is Just A Test !!!"),
			BuiltinFunctions.camelCase(TextNode.valueOf("this iS JusT a TEST !!!")));
	}
}
