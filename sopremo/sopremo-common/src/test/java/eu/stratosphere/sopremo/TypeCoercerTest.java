package eu.stratosphere.sopremo;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.BigIntegerNode;
import eu.stratosphere.sopremo.type.BooleanNode;
import eu.stratosphere.sopremo.type.DecimalNode;
import eu.stratosphere.sopremo.type.DoubleNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IntNode;
import eu.stratosphere.sopremo.type.LongNode;
import eu.stratosphere.sopremo.type.AbstractNumericNode;
import eu.stratosphere.sopremo.type.ObjectNode;
import eu.stratosphere.sopremo.type.TextNode;

@RunWith(Parameterized.class)
public class TypeCoercerTest {
	private static final Object CONVERSION_ERROR = "Error";

	private final IJsonNode value;

	private final Class<? extends IJsonNode> targetType;

	private final Object expectedResult;

	public TypeCoercerTest(final IJsonNode value, final Class<? extends IJsonNode> targetType,
			final Object expectedResult) {
		this.value = value;
		this.targetType = targetType;
		this.expectedResult = expectedResult;
	}

	@Test
	public void shouldPerformTheCoercionAsExpected() {
		try {
			final IJsonNode result = TypeCoercer.INSTANCE.coerce(this.value, null, this.targetType);
			Assert.assertTrue(String.format("%s->%s=%s", this.value.getClass(), this.targetType, result.getClass()),
				this.targetType.isInstance(result));
			Assert.assertEquals(String.format("%s->%s=%s", this.value, this.targetType, result), this.expectedResult,
				result);
		} catch (final CoercionException e) {
			Assert.assertTrue(String.format("%s->%s=Exception", this.value, this.targetType),
				this.expectedResult == CONVERSION_ERROR);
		}
	}

	@Parameters
	public static List<Object[]> combinations() {
		return Arrays.asList(new Object[][] {
			{ BooleanNode.TRUE, BooleanNode.class, BooleanNode.TRUE },
			{ BooleanNode.FALSE, BooleanNode.class, BooleanNode.FALSE },
			{ BooleanNode.TRUE, TextNode.class, TextNode.valueOf("true") },
			{ BooleanNode.FALSE, TextNode.class, TextNode.valueOf("false") },
			{ BooleanNode.TRUE, AbstractNumericNode.class, IntNode.valueOf(1) },
			{ BooleanNode.FALSE, AbstractNumericNode.class, IntNode.valueOf(0) },
			{ BooleanNode.TRUE, IntNode.class, IntNode.valueOf(1) },
			{ BooleanNode.FALSE, IntNode.class, IntNode.valueOf(0) },
			{ BooleanNode.TRUE, DoubleNode.class, DoubleNode.valueOf(1) },
			{ BooleanNode.TRUE, LongNode.class, LongNode.valueOf(1) },
			{ BooleanNode.TRUE, BigIntegerNode.class, BigIntegerNode.valueOf(BigInteger.valueOf(1)) },
			{ BooleanNode.TRUE, DecimalNode.class, DecimalNode.valueOf(BigDecimal.valueOf(1)) },
			{ BooleanNode.TRUE, ArrayNode.class, createArray(BooleanNode.TRUE) },
			{ BooleanNode.TRUE, ObjectNode.class, CONVERSION_ERROR },

			{ TextNode.valueOf("true"), BooleanNode.class, BooleanNode.TRUE },
			// paradox but we do not parse the string but check if it is empty
			{ TextNode.valueOf("false"), BooleanNode.class, BooleanNode.TRUE },
			{ TextNode.valueOf("string"), TextNode.class, TextNode.valueOf("string") },
			{ TextNode.valueOf("12.34"), AbstractNumericNode.class, DecimalNode.valueOf(new BigDecimal("12.34")) },
			{ TextNode.valueOf("12"), IntNode.class, IntNode.valueOf(12) },
			{ TextNode.valueOf("12"), LongNode.class, LongNode.valueOf(12) },
			{ TextNode.valueOf("12.5"), DoubleNode.class, DoubleNode.valueOf(12.5) },
			{ TextNode.valueOf("12"), BigIntegerNode.class, BigIntegerNode.valueOf(BigInteger.valueOf(12)) },
			{ TextNode.valueOf("12.34"), DecimalNode.class, DecimalNode.valueOf(new BigDecimal("12.34")) },
			{ TextNode.valueOf("bla"), ArrayNode.class, createArray(TextNode.valueOf("bla")) },
			{ TextNode.valueOf("bla"), AbstractNumericNode.class, CONVERSION_ERROR },
			{ TextNode.valueOf("bla"), IntNode.class, CONVERSION_ERROR },
			{ TextNode.valueOf("bla"), DoubleNode.class, CONVERSION_ERROR },
			{ TextNode.valueOf("bla"), LongNode.class, CONVERSION_ERROR },
			{ TextNode.valueOf("bla"), BigIntegerNode.class, CONVERSION_ERROR },
			{ TextNode.valueOf("bla"), DecimalNode.class, CONVERSION_ERROR },
			{ TextNode.valueOf("bla"), ObjectNode.class, CONVERSION_ERROR },
			{ TextNode.valueOf("12.34"), IntNode.class, CONVERSION_ERROR },
			{ TextNode.valueOf("12.34"), LongNode.class, CONVERSION_ERROR },
			{ TextNode.valueOf("12.34"), BigIntegerNode.class, CONVERSION_ERROR },

			{ IntNode.valueOf(1), BooleanNode.class, BooleanNode.TRUE },
			{ IntNode.valueOf(0), BooleanNode.class, BooleanNode.FALSE },
			{ LongNode.valueOf(1), BooleanNode.class, BooleanNode.TRUE },
			{ DoubleNode.valueOf(0), BooleanNode.class, BooleanNode.FALSE },
			{ IntNode.valueOf(12), TextNode.class, TextNode.valueOf("12") },
			{ LongNode.valueOf(Long.MAX_VALUE), TextNode.class, TextNode.valueOf(String.valueOf(Long.MAX_VALUE)) },
			{ DoubleNode.valueOf(12.34), TextNode.class, TextNode.valueOf(String.valueOf(12.34)) },
			{ IntNode.valueOf(12), LongNode.class, LongNode.valueOf(12) },
			{ DoubleNode.valueOf(12.5), IntNode.class, IntNode.valueOf(12) },
			{ IntNode.valueOf(12), ArrayNode.class, createArray(IntNode.valueOf(12)) },
			{ IntNode.valueOf(1), ObjectNode.class, CONVERSION_ERROR },

			{ createArray(), BooleanNode.class, BooleanNode.FALSE },
			{ createArray(IntNode.valueOf(12)), BooleanNode.class, BooleanNode.TRUE },
			{ createArray(IntNode.valueOf(12)), IntNode.class, CONVERSION_ERROR },
			{ createArray(IntNode.valueOf(12)), TextNode.class, TextNode.valueOf("[12]") },
			{ createArray(IntNode.valueOf(12)), ArrayNode.class, createArray(IntNode.valueOf(12)) },
			{ createArray(IntNode.valueOf(12)), ObjectNode.class, CONVERSION_ERROR },
		});
	}

	protected static IArrayNode createArray(final IJsonNode... elems) {
		final IArrayNode arrayNode = new ArrayNode();
		arrayNode.addAll(Arrays.asList(elems));
		return arrayNode;
	}
}
