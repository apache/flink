package eu.stratosphere.sopremo;


public class StreamArrayNodeTest extends SopremoTest<StreamArrayNode> {
	@Override
	protected StreamArrayNode createDefaultInstance(int index) {
		return createStreamArray(index);
	}
}
