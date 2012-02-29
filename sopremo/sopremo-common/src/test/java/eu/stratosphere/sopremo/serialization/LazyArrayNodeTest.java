package eu.stratosphere.sopremo.serialization;

import junit.framework.Assert;

import org.junit.Test;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.sopremo.pact.JsonNodeWrapper;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.ArrayNodeBaseTest;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IntNode;

public class LazyArrayNodeTest extends ArrayNodeBaseTest<LazyArrayNode> {

	@Override
	public void initArrayNode() {
		ArraySchema schema = new ArraySchema();
		schema.setHeadSize(5);
		PactRecord record = schema.jsonToRecord(
			new ArrayNode(IntNode.valueOf(0), IntNode.valueOf(1), IntNode.valueOf(2)), null);

		this.node = new LazyArrayNode(record, schema);
	}

	@Test
	public void shouldIncrementOthersField() {
		PactRecord record = this.node.getJavaValue();
		IArrayNode others = (IArrayNode) SopremoUtil.unwrap(record.getField(5, JsonNodeWrapper.class));

		Assert.assertEquals(0, others.size());

		this.node.addAll(new ArrayNode(IntNode.valueOf(3), IntNode.valueOf(4), IntNode.valueOf(5)));

		Assert.assertEquals(1, others.size());
	}

}
