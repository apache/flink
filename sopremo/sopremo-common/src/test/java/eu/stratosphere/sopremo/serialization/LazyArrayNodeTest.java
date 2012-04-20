package eu.stratosphere.sopremo.serialization;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.ArrayNodeBaseTest;
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

}
