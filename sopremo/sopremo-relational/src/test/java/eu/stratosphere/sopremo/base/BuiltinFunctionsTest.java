package eu.stratosphere.sopremo.base;

import java.math.BigDecimal;

import junit.framework.Assert;

import org.codehaus.jackson.JsonNode;
import org.junit.Test;

import eu.stratosphere.sopremo.SopremoTest;

/**
 * Tests {@link BuiltinFunctions}
 */
public class BuiltinFunctionsTest {

	/**
	 * 
	 */
	@Test
	public void shouldCoerceDataWhenSumming() {
		Assert.assertEquals(
			6.4,
			BuiltinFunctions.sum(SopremoTest.createArrayNode(1.1, 2, new BigDecimal("3.3"))).getDoubleValue(), 0.01);
	}

	/**
	 * 
	 */
	@Test
	public void shouldConcatenateObjects() {
		Assert.assertEquals(
			SopremoTest.createValueNode("bla1blu2"),
			BuiltinFunctions.concat(new JsonNode[] {
				SopremoTest.createValueNode("bla"),
				SopremoTest.createValueNode(1),
				SopremoTest.createValueNode("blu"),
				SopremoTest.createValueNode(2), }));
	}

	/**
	 * 
	 */
	@Test
	public void shouldConcatenateStrings() {
		Assert.assertEquals(
			SopremoTest.createValueNode("blabliblu"),
			BuiltinFunctions.concat(new JsonNode[] {
				SopremoTest.createValueNode("bla"),
				SopremoTest.createValueNode("bli"),
				SopremoTest.createValueNode("blu") }));
	}

	/**
	 * 
	 */
	@Test
	public void shouldCountCompactArray() {
		Assert.assertEquals(
			SopremoTest.createValueNode(3),
			BuiltinFunctions.count(SopremoTest.createCompactArray(1, 2, 3)));
	}

	/**
	 * 
	 */
	@Test
	public void shouldCountNormalArray() {
		Assert.assertEquals(
			SopremoTest.createValueNode(3),
			BuiltinFunctions.count(SopremoTest.createArrayNode(1, 2, 3)));
	}

	/**
	 * 
	 */
	@Test
	public void shouldCountStreamArray() {
		Assert.assertEquals(
			SopremoTest.createValueNode(3),
			BuiltinFunctions.count(SopremoTest.createStreamArray(1, 2, 3)));
	}

	/**
	 * 
	 */
	@Test
	public void shouldCountZeroForEmptyArray() {
		Assert.assertEquals(
			SopremoTest.createValueNode(0),
			BuiltinFunctions.count(SopremoTest.createArrayNode()));
	}

//	/**
//	 * 
//	 */
//	@Test(expected = ClassCastException.class)
//	public void shouldFailIfIncompatible() {
//		BuiltinFunctions.sort(SopremoTest.createArrayNode(3.14, 4, 1.2, 2));
//	}

	/**
	 * 
	 */
	@Test(expected = ClassCastException.class)
	public void shouldFailToSumIfNonNumbers() {
		BuiltinFunctions.sum(SopremoTest.createArrayNode("test"));
	}

	/**
	 * 
	 */
	@Test
	public void shouldReturnEmptyStringWhenConcatenatingEmptyArray() {
		Assert.assertEquals(
			SopremoTest.createValueNode(""), BuiltinFunctions.concat(new JsonNode[0]));
	}

	/**
	 * 
	 */
	@Test
	public void shouldSortArrays() {
		Assert.assertEquals(
			SopremoTest.createArrayNode(new Number[] { 1, 2.4 }, new Number[] { 1, 3.4 }, new Number[] { 2, 2.4 },
				new Number[] { 2, 2.4, 3 }),
			BuiltinFunctions.sort(SopremoTest.createArrayNode(new Number[] { 1, 3.4 }, new Number[] { 2, 2.4 },
				new Number[] { 1, 2.4 }, new Number[] { 2, 2.4, 3 })));
	}

	/**
	 * 
	 */
	@Test
	public void shouldSortDoubles() {
		Assert.assertEquals(
			SopremoTest.createArrayNode(1.2, 2.0, 3.14, 4.5),
			BuiltinFunctions.sort(SopremoTest.createArrayNode(3.14, 4.5, 1.2, 2.0)));
	}

	/**
	 * 
	 */
	@Test
	public void shouldSortEmptyArray() {
		Assert.assertEquals(
			SopremoTest.createArrayNode(),
			BuiltinFunctions.sort(SopremoTest.createArrayNode()));
	}

	/**
	 * 
	 */
	@Test
	public void shouldSortIntegers() {
		Assert.assertEquals(
			SopremoTest.createArrayNode(1, 2, 3, 4),
			BuiltinFunctions.sort(SopremoTest.createArrayNode(3, 4, 1, 2)));
	}

	/**
	 * 
	 */
	@Test
	public void shouldSumDoubles() {
		Assert.assertEquals(
			6.6,
			BuiltinFunctions.sum(SopremoTest.createArrayNode(1.1, 2.2, 3.3)).getDoubleValue(), 0.01);
	}

	/**
	 * 
	 */
	@Test
	public void shouldSumEmptyArrayToZero() {
		Assert.assertEquals(
			SopremoTest.createValueNode(0),
			BuiltinFunctions.sum(SopremoTest.createArrayNode()));
	}

	/**
	 * 
	 */
	@Test
	public void shouldSumIntegers() {
		Assert.assertEquals(
			SopremoTest.createValueNode(6),
			BuiltinFunctions.sum(SopremoTest.createArrayNode(1, 2, 3)));
	}

	/**
	 * 
	 */
	@Test
	public void shouldUnionAllCompactArrays() {
		Assert.assertEquals(
			SopremoTest.createArrayNode(1, 2, 3, 4, 5, 6),
			BuiltinFunctions.unionAll(SopremoTest.createCompactArray(1, 2, 3), SopremoTest.createCompactArray(4, 5),
				SopremoTest.createCompactArray(6)));
	}

	/**
	 * Very rare case...
	 */
	@Test
	public void shouldUnionAllMixedArrayTypes() {
		Assert.assertEquals(
			SopremoTest.createStreamArray(1, 2, 3, 4, 5, 6),
			BuiltinFunctions.unionAll(SopremoTest.createArrayNode(1, 2, 3), SopremoTest.createCompactArray(4, 5),
				SopremoTest.createStreamArray(6)));
	}

	/**
	 * 
	 */
	@Test
	public void shouldUnionAllNormalArrays() {
		Assert.assertEquals(
			SopremoTest.createArrayNode(1, 2, 3, 4, 5, 6),
			BuiltinFunctions.unionAll(SopremoTest.createArrayNode(1, 2, 3), SopremoTest.createArrayNode(4, 5),
				SopremoTest.createArrayNode(6)));
	}

	/**
	 * 
	 */
	@Test
	public void shouldUnionAllStreamArrays() {
		Assert.assertEquals(
			SopremoTest.createStreamArray(1, 2, 3, 4, 5, 6),
			BuiltinFunctions.unionAll(SopremoTest.createStreamArray(1, 2, 3), SopremoTest.createStreamArray(4, 5),
				SopremoTest.createStreamArray(6)));
	}
}
