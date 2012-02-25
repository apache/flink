package eu.stratosphere.sopremo.type;

public class ArrayNodeTest extends ArrayNodeBaseTest<ArrayNode> {

	@Override
	public void initArrayNode() {
		this.node = new ArrayNode().add(IntNode.valueOf(1)).add(IntNode.valueOf(2)).add(IntNode.valueOf(3));
	}
}
