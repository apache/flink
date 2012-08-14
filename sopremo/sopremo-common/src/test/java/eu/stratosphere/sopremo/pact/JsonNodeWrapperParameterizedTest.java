package eu.stratosphere.sopremo.pact;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.sopremo.type.BigIntegerNode;
import eu.stratosphere.sopremo.type.BooleanNode;
import eu.stratosphere.sopremo.type.DecimalNode;
import eu.stratosphere.sopremo.type.DoubleNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IntNode;
import eu.stratosphere.sopremo.type.JsonUtil;
import eu.stratosphere.sopremo.type.LongNode;
import eu.stratosphere.sopremo.type.MissingNode;
import eu.stratosphere.sopremo.type.NullNode;
import eu.stratosphere.sopremo.type.TextNode;

@RunWith(Parameterized.class)
public class JsonNodeWrapperParameterizedTest {

	@Parameters
	public static List<Object[]> data() {
		return Arrays.asList(new Object[][] {
			{ BooleanNode.TRUE, BooleanNode.TRUE, true },
			{ BooleanNode.FALSE, TextNode.valueOf("42"), false },
			{ IntNode.valueOf(42), IntNode.valueOf(43), false },
			{ TextNode.valueOf("42"), DecimalNode.valueOf(BigDecimal.TEN), false },
			{ DecimalNode.valueOf(BigDecimal.TEN), DecimalNode.valueOf(BigDecimal.TEN), true },
			{ BigIntegerNode.valueOf(BigInteger.TEN), DoubleNode.valueOf(42.42), false },
			{ DoubleNode.valueOf(42.42), NullNode.getInstance(), false },
			{ LongNode.valueOf(42), LongNode.valueOf(42), true },
			{ NullNode.getInstance(), MissingNode.getInstance(), false },
			{ MissingNode.getInstance(), MissingNode.getInstance(), true },
			{ JsonUtil.createArrayNode(IntNode.valueOf(42), IntNode.valueOf(43)),
				JsonUtil.createArrayNode(IntNode.valueOf(42), IntNode.valueOf(44)), false },
			{ JsonUtil.createObjectNode("field1", IntNode.valueOf(42), "field2", IntNode.valueOf(43)),
				JsonUtil.createObjectNode("field1", IntNode.valueOf(42), "field2", IntNode.valueOf(43)), true },
		});
	}

	private static int ARRAY_LENGHT = 100;

	private final JsonNodeWrapper wrapper1;

	private final JsonNodeWrapper wrapper2;

	private final boolean shouldBeEqual;

	private final byte[] target1;

	private final byte[] target2;

	public JsonNodeWrapperParameterizedTest(final IJsonNode node1, final IJsonNode node2, final boolean shouldBeEqual) {
		this.wrapper1 = (JsonNodeWrapper) SopremoUtil.wrap(node1);
		this.wrapper2 = (JsonNodeWrapper) SopremoUtil.wrap(node2);
		this.shouldBeEqual = shouldBeEqual;

		this.target1 = new byte[ARRAY_LENGHT];
		this.target2 = new byte[ARRAY_LENGHT];
	}

	@Test
	public void shouldAddRightTypeByte() {

		this.wrapper1.copyNormalizedKey(this.target1, 0, ARRAY_LENGHT);
		this.wrapper2.copyNormalizedKey(this.target2, 0, ARRAY_LENGHT);

		Assert.assertEquals(this.target1[0], (byte) this.wrapper1.getType().ordinal());
		Assert.assertEquals(this.target2[0], (byte) this.wrapper2.getType().ordinal());
	}

	@Test
	public void shouldCreateDifferentNormalizedKeys() {
		this.wrapper1.copyNormalizedKey(this.target1, 0, ARRAY_LENGHT);
		this.wrapper2.copyNormalizedKey(this.target2, 0, ARRAY_LENGHT);

		Assert.assertEquals(this.shouldBeEqual, Arrays.equals(this.target1, this.target2));
	}

	@Test
	public void shouldCorrectlyEqualTwoWrapper() {
		Assert.assertEquals(this.shouldBeEqual, this.wrapper1.equals(this.wrapper2));
	}
}
