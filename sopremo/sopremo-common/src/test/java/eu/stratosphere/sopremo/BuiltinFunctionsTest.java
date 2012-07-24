package eu.stratosphere.sopremo;

import static eu.stratosphere.sopremo.type.JsonUtil.createArrayNode;
import static eu.stratosphere.sopremo.type.JsonUtil.createCompactArray;
import static eu.stratosphere.sopremo.type.JsonUtil.createValueNode;

import java.math.BigDecimal;

import junit.framework.Assert;

import org.junit.Test;

import eu.stratosphere.sopremo.type.DoubleNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.INumericNode;
import eu.stratosphere.sopremo.type.IntNode;
import eu.stratosphere.sopremo.type.JsonUtil;
import eu.stratosphere.sopremo.type.TextNode;

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
			((DoubleNode) DefaultFunctions.sum(createArrayNode(1.1, 2, new BigDecimal("3.3")))).getDoubleValue(), 0.01);
	}

	/**
	 * 
	 */
	@Test
	public void shouldConcatenateObjects() {
		Assert.assertEquals(
			createValueNode("bla1blu2"),
			DefaultFunctions.concat(new IJsonNode[] {
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
			DefaultFunctions.concat(new IJsonNode[] {
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
			DefaultFunctions.count(createCompactArray(1, 2, 3)));
	}

	/**
	 * 
	 */
	@Test
	public void shouldCountNormalArray() {
		Assert.assertEquals(
			createValueNode(3),
			DefaultFunctions.count(createArrayNode(1, 2, 3)));
	}

	/**
	 * 
	 */
	@Test
	public void shouldCountStreamArray() {
		Assert.assertEquals(
			createValueNode(3),
			DefaultFunctions.count(JsonUtil.createArrayNode(1, 2, 3)));
	}

	/**
	 * 
	 */
	@Test
	public void shouldCountZeroForEmptyArray() {
		Assert.assertEquals(
			createValueNode(0),
			DefaultFunctions.count(createArrayNode()));
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
		DefaultFunctions.sum(createArrayNode("test"));
	}

	/**
	 * 
	 */
	@Test
	public void shouldReturnEmptyStringWhenConcatenatingEmptyArray() {
		Assert.assertEquals(
			createValueNode(""), DefaultFunctions.concat(new IJsonNode[0]));
	}

	/**
	 * 
	 */
	@Test
	public void shouldSortArrays() {
		Assert.assertEquals(
			createArrayNode(new Number[] { 1, 2.4 }, new Number[] { 1, 3.4 }, new Number[] { 2, 2.4 },
				new Number[] { 2, 2.4, 3 }),
			DefaultFunctions.sort(createArrayNode(new Number[] { 1, 3.4 }, new Number[] { 2, 2.4 },
				new Number[] { 1, 2.4 }, new Number[] { 2, 2.4, 3 })));
	}

	/**
	 * 
	 */
	@Test
	public void shouldSortDoubles() {
		Assert.assertEquals(
			createArrayNode(1.2, 2.0, 3.14, 4.5),
			DefaultFunctions.sort(createArrayNode(3.14, 4.5, 1.2, 2.0)));
	}

	/**
	 * 
	 */
	@Test
	public void shouldSortEmptyArray() {
		Assert.assertEquals(
			createArrayNode(),
			DefaultFunctions.sort(createArrayNode()));
	}

	/**
	 * 
	 */
	@Test
	public void shouldSortIntegers() {
		Assert.assertEquals(
			createArrayNode(1, 2, 3, 4),
			DefaultFunctions.sort(createArrayNode(3, 4, 1, 2)));
	}

	/**
	 * 
	 */
	@Test
	public void shouldSumDoubles() {
		Assert.assertEquals(
			6.6,
			((DoubleNode) DefaultFunctions.sum(createArrayNode(1.1, 2.2, 3.3))).getDoubleValue(), 0.01);
	}

	/**
	 * 
	 */
	@Test
	public void shouldSumEmptyArrayToZero() {
		Assert.assertEquals(
			createValueNode(0),
			DefaultFunctions.sum(createArrayNode()));
	}

	/**
	 * 
	 */
	@Test
	public void shouldSumIntegers() {
		Assert.assertEquals(
			createValueNode(6),
			DefaultFunctions.sum(createArrayNode(1, 2, 3)));
	}

	/**
	 * 
	 */
	@Test
	public void shouldUnionAllCompactArrays() {
		Assert.assertEquals(
			createArrayNode(1, 2, 3, 4, 5, 6),
			DefaultFunctions.unionAll(createCompactArray(1, 2, 3), createCompactArray(4, 5),
				createCompactArray(6)));
	}

	/**
	 * Very rare case...
	 */
	@Test
	public void shouldUnionAllMixedArrayTypes() {
		Assert.assertEquals(
			JsonUtil.createArrayNode(1, 2, 3, 4, 5, 6),
			DefaultFunctions.unionAll(createArrayNode(1, 2, 3), createCompactArray(4, 5),
				JsonUtil.createArrayNode(6)));
	}

	/**
	 * 
	 */
	@Test
	public void shouldUnionAllNormalArrays() {
		Assert.assertEquals(
			createArrayNode(1, 2, 3, 4, 5, 6),
			DefaultFunctions.unionAll(createArrayNode(1, 2, 3), createArrayNode(4, 5),
				createArrayNode(6)));
	}

	/**
	 * 
	 */
	@Test
	public void shouldUnionAllStreamArrays() {
		Assert.assertEquals(
			JsonUtil.createArrayNode(1, 2, 3, 4, 5, 6),
			DefaultFunctions.unionAll(JsonUtil.createArrayNode(1, 2, 3), JsonUtil.createArrayNode(4, 5),
				JsonUtil.createArrayNode(6)));
	}

	@Test
	public void shouldCalculateAvg() {
		final IJsonNode aggregator = DefaultFunctions.AVERAGE.initialize(null);
		DefaultFunctions.AVERAGE.aggregate(IntNode.valueOf(50), aggregator, this.context);
		DefaultFunctions.AVERAGE.aggregate(IntNode.valueOf(25), aggregator, this.context);
		DefaultFunctions.AVERAGE.aggregate(IntNode.valueOf(75), aggregator, this.context);

		final IJsonNode result = DefaultFunctions.AVERAGE.getFinalAggregate(aggregator, null);
		Assert.assertTrue(result instanceof INumericNode);
		Assert.assertEquals(50.0, ((INumericNode) result).getDoubleValue());
	}

	@Test
	public void shouldCalculateAvgWithDifferentNodes() {
		IJsonNode aggregator = DefaultFunctions.AVERAGE.initialize(null);

		for (int i = 1; i < 500; i++)
			aggregator =
				DefaultFunctions.AVERAGE.aggregate(i % 2 == 0 ? IntNode.valueOf(i) : DoubleNode.valueOf(i), aggregator,
					this.context);

		Assert.assertEquals(DoubleNode.valueOf(250), DefaultFunctions.AVERAGE.getFinalAggregate(aggregator, null));
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
		final IJsonNode result = DefaultFunctions.substring(TextNode.valueOf("0123456789"), IntNode.valueOf(3),
			IntNode.valueOf(6));

		Assert.assertEquals(TextNode.valueOf("345"), result);
	}

	@Test
	public void shouldCreateRightCamelCaseRepresentation() {
		Assert.assertEquals(
			TextNode.valueOf("This Is Just A Test !!!"),
			DefaultFunctions.camelCase(TextNode.valueOf("this iS JusT a TEST !!!")));
	}
}
