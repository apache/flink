package eu.stratosphere.sopremo.jsondatamodel;

import java.math.BigDecimal;

import junit.framework.Assert;

public class DecimalNodeTest extends JsonNodeTest<DecimalNode> {

	@Override
	public void testValue() {
		DecimalNode decimalnode = new DecimalNode(BigDecimal.valueOf(42));
		Assert.assertEquals(BigDecimal.valueOf(42), decimalnode.getDecimalValue());
	}

}
