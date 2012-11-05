package eu.stratosphere.sopremo;

import static eu.stratosphere.sopremo.type.JsonUtil.createArrayNode;
import static eu.stratosphere.sopremo.type.JsonUtil.createCompactArray;
import static eu.stratosphere.sopremo.type.JsonUtil.createValueNode;

import java.math.BigDecimal;

import junit.framework.Assert;

import org.junit.Test;

import eu.stratosphere.sopremo.aggregation.Aggregation;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.DoubleNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.INumericNode;
import eu.stratosphere.sopremo.type.IntNode;
import eu.stratosphere.sopremo.type.JavaToJsonMapper;
import eu.stratosphere.sopremo.type.JsonUtil;
import eu.stratosphere.sopremo.type.TextNode;

/**
 * Tests {@link BuiltinFunctions}
 */
public class CoreFunctionsTest {

	protected EvaluationContext context = new EvaluationContext();

	/**
	 * 
	 */
	@Test
	public void shouldCoerceDataWhenSumming() {
		shouldAggregate(6.4, CoreFunctions.SUM, 1.1, 2, new BigDecimal("3.3"));
	}

	/**
	 * 
	 */
	@Test(expected = ClassCastException.class)
	public void shouldNotConcatenateObjects() {
		shouldAggregate("bla1blu2", CoreFunctions.CONCAT, "bla", 1, "blu", 2);
	}

	/**
	 * 
	 */
	@Test
	public void shouldConcatenateStrings() {
		shouldAggregate("blabliblu", CoreFunctions.CONCAT, "bla", "bli", "blu");
	}

	/**
	 * 
	 */
	@Test
	public void shouldCountNormalArray() {
		shouldAggregate(3, CoreFunctions.COUNT, 1, 2, 3);
	}

	/**
	 * 
	 */
	@Test
	public void shouldCountZeroForEmptyArray() {
		shouldAggregate(0, CoreFunctions.COUNT);
	}

	/**
	 * 
	 */
	@Test(expected = ClassCastException.class)
	public void shouldFailToSumIfNonNumbers() {
		shouldAggregate(null, CoreFunctions.SUM, "test");
	}

	/**
	 * 
	 */
	@Test
	public void shouldReturnEmptyStringWhenConcatenatingEmptyArray() {
		TextNode result = new TextNode();
		CoreFunctions.concat(result, new IJsonNode[0]);
		Assert.assertEquals(createValueNode(""), result);
	}

	/**
	 * 
	 */
	@Test
	public void shouldSortArrays() {
		final IArrayNode expected =
			createArrayNode(new Number[] { 1, 2.4 }, new Number[] { 1, 3.4 }, new Number[] { 2, 2.4 },
				new Number[] { 2, 2.4, 3 });
		shouldAggregate(expected, CoreFunctions.SORT, new Number[] { 1, 3.4 }, new Number[] { 2, 2.4 },
			new Number[] { 1, 2.4 }, new Number[] { 2, 2.4, 3 });
	}

	/**
	 * 
	 */
	@Test
	public void shouldSortDoubles() {
		shouldAggregate(createArrayNode(1.2, 2.0, 3.14, 4.5), CoreFunctions.SORT, 3.14, 4.5, 1.2, 2.0);
	}

	/**
	 * 
	 */
	@Test
	public void shouldSortEmptyArray() {
		shouldAggregate(createArrayNode(), CoreFunctions.SORT);
	}

	/**
	 * 
	 */
	@Test
	public void shouldSortIntegers() {
		shouldAggregate(createArrayNode(1, 2, 3, 4), CoreFunctions.SORT, 3, 4, 1, 2);
	}

	/**
	 * 
	 */
	@Test
	public void shouldSumDoubles() {
		shouldAggregate(6.6, CoreFunctions.SUM, 1.1, 2.2, 3.3);
	}

	/**
	 * 
	 */
	@Test
	public void shouldSumEmptyArrayToZero() {
		shouldAggregate(0, CoreFunctions.SUM);
	}

	/**
	 * 
	 */
	@Test
	public void shouldSumIntegers() {
		shouldAggregate(6, CoreFunctions.SUM, 1, 2, 3);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void shouldAggregate(Object expected, Aggregation function, Object... items) {
		IJsonNode aggregator = function.initialize(null);

		for (Object item : items)
			aggregator = function.aggregate(JavaToJsonMapper.INSTANCE.valueToTree(item), aggregator, this.context);

		final IJsonNode result = function.getFinalAggregate(aggregator, null);
		Assert.assertEquals(JavaToJsonMapper.INSTANCE.valueToTree(expected), result);

	}

	/**
	 * 
	 */
	@Test
	public void shouldUnionAllCompactArrays() {
		final IArrayNode expectedResult = createArrayNode(1, 2, 3, 4, 5, 6);
		final ArrayNode result = new ArrayNode();
		CoreFunctions.unionAll(result, createCompactArray(1, 2, 3), createCompactArray(4, 5), createCompactArray(6));
		Assert.assertEquals(expectedResult, result);
	}

	/**
	 * Very rare case...
	 */
	@Test
	public void shouldUnionAllMixedArrayTypes() {
		final IArrayNode expectedResult = createArrayNode(1, 2, 3, 4, 5, 6);
		final ArrayNode result = new ArrayNode();
		CoreFunctions.unionAll(result, createArrayNode(1, 2, 3), createCompactArray(4, 5), JsonUtil.createArrayNode(6));
		Assert.assertEquals(expectedResult, result);
	}

	/**
	 * 
	 */
	@Test
	public void shouldUnionAllNormalArrays() {
		final IArrayNode expectedResult = createArrayNode(1, 2, 3, 4, 5, 6);
		final ArrayNode result = new ArrayNode();
		CoreFunctions.unionAll(result, createArrayNode(1, 2, 3), createArrayNode(4, 5, 6));
		Assert.assertEquals(expectedResult, result);
	}

//	/**
//	 * 
//	 */
//	@Test
//	public void shouldUnionAllStreamArrays() {
//		Assert.assertEquals(
//			JsonUtil.createArrayNode(1, 2, 3, 4, 5, 6),
//			CoreFunctions.unionAll(JsonUtil.createArrayNode(1, 2, 3), JsonUtil.createArrayNode(4, 5),
//				JsonUtil.createArrayNode(6)));
//	}

	@Test
	public void shouldCalculateAvg() {
		CoreFunctions.AverageState aggregator = CoreFunctions.MEAN.initialize(null);
		CoreFunctions.MEAN.aggregate(IntNode.valueOf(50), aggregator, this.context);
		CoreFunctions.MEAN.aggregate(IntNode.valueOf(25), aggregator, this.context);
		CoreFunctions.MEAN.aggregate(IntNode.valueOf(75), aggregator, this.context);

		final IJsonNode result = CoreFunctions.MEAN.getFinalAggregate(aggregator, null);
		Assert.assertTrue(result instanceof INumericNode);
		Assert.assertEquals(50.0, ((INumericNode) result).getDoubleValue());
	}

	@Test
	public void shouldCalculateAvgWithDifferentNodes() {
		CoreFunctions.AverageState aggregator = CoreFunctions.MEAN.initialize(null);

		for (int i = 1; i < 500; i++)
			aggregator = CoreFunctions.MEAN.aggregate(
				i % 2 == 0 ? IntNode.valueOf(i) : DoubleNode.valueOf(i), aggregator, this.context);

		Assert.assertEquals(DoubleNode.valueOf(250), CoreFunctions.MEAN.getFinalAggregate(aggregator, null));
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
		TextNode result = new TextNode();
		CoreFunctions.substring(result, TextNode.valueOf("0123456789"), IntNode.valueOf(3), IntNode.valueOf(6));

		Assert.assertEquals(TextNode.valueOf("345"), result);
	}

	@Test
	public void shouldCreateRightCamelCaseRepresentation() {
		TextNode result = new TextNode();
		CoreFunctions.camelCase(result, TextNode.valueOf("this iS JusT a TEST !!!"));
		Assert.assertEquals(TextNode.valueOf("This Is Just A Test !!!"), result);
	}
}
