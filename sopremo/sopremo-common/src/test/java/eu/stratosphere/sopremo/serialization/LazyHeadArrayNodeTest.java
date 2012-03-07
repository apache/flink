package eu.stratosphere.sopremo.serialization;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.ArrayNodeBaseTest;
import eu.stratosphere.sopremo.type.IntNode;

public class LazyHeadArrayNodeTest extends ArrayNodeBaseTest<LazyTailArrayNode> {

	@Override
	public void initArrayNode() {
		TailArraySchema schema = new TailArraySchema();
		schema.setHeadSize(5);
		PactRecord record = schema.jsonToRecord(
			new ArrayNode(IntNode.valueOf(0), IntNode.valueOf(1), IntNode.valueOf(2)), null);

		this.node = new LazyTailArrayNode(record, schema);
	}

}
