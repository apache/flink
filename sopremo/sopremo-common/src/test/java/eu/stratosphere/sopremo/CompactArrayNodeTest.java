package eu.stratosphere.sopremo;

import org.junit.Test;

public class CompactArrayNodeTest extends SopremoTest<CompactArrayNode> {
	@Override
	protected CompactArrayNode createDefaultInstance(int index) {
		return createCompactArray(index);
	}
}
