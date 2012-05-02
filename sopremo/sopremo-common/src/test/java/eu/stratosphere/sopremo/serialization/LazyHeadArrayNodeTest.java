package eu.stratosphere.sopremo.serialization;

import junit.framework.Assert;

import org.junit.Test;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.sopremo.pact.JsonNodeWrapper;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.ArrayNodeBaseTest;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IntNode;

public class LazyHeadArrayNodeTest extends ArrayNodeBaseTest<LazyHeadArrayNode> {

	@Override
	public void initArrayNode() {
		HeadArraySchema schema = new HeadArraySchema();
		schema.setHeadSize(5);
		PactRecord record = schema.jsonToRecord(
			new ArrayNode(IntNode.valueOf(0), IntNode.valueOf(1), IntNode.valueOf(2)), null, null);

		this.node = new LazyHeadArrayNode(record, schema);
	}

	@Test
	public void shouldIncrementOthersField() {
		PactRecord record = this.node.getJavaValue();
		IArrayNode others = (IArrayNode) SopremoUtil.unwrap(record.getField(5, JsonNodeWrapper.class));

		Assert.assertEquals(0, others.size());

		this.node.addAll(new ArrayNode(IntNode.valueOf(3), IntNode.valueOf(4), IntNode.valueOf(5)));

		Assert.assertEquals(1, others.size());
	}

	@Override
	public void testValue() {
	}

	@Override
	protected IJsonNode lowerNode() {
		HeadArraySchema schema = new HeadArraySchema();
		schema.setHeadSize(5);
		PactRecord record = schema.jsonToRecord(
			new ArrayNode(IntNode.valueOf(0), IntNode.valueOf(1), IntNode.valueOf(2)), null, null);

		return new LazyHeadArrayNode(record, schema);
	}

	@Override
	protected IJsonNode higherNode() {
		HeadArraySchema schema = new HeadArraySchema();
		schema.setHeadSize(5);
		PactRecord record = schema.jsonToRecord(
			new ArrayNode(IntNode.valueOf(0), IntNode.valueOf(1), IntNode.valueOf(3)), null, null);

		return new LazyHeadArrayNode(record, schema);
	}

	@Override
	@Test(expected = UnsupportedOperationException.class)
	public void shouldNormalizeKeys() {
		super.shouldNormalizeKeys();
	}

}
