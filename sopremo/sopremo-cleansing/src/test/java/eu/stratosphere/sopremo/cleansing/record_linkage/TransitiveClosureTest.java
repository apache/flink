package eu.stratosphere.sopremo.cleansing.record_linkage;

import junit.framework.Assert;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.IntNode;
import org.junit.Test;

public class TransitiveClosureTest {
	@Test
	public void testWarshall() {
		final BinarySparseMatrix matrix = new BinarySparseMatrix();
		final JsonNode[] nodes = new JsonNode[6];
		for (int index = 0; index < nodes.length; index++)
			nodes[index] = IntNode.valueOf(index);

		matrix.set(nodes[0], nodes[1]);
		matrix.set(nodes[1], nodes[2]);
		matrix.set(nodes[2], nodes[3]);

		matrix.set(nodes[4], nodes[5]);
		matrix.makeSymmetric();

		TransitiveClosure.warshall(matrix);

		final BinarySparseMatrix expected = new BinarySparseMatrix();
		expected.set(nodes[0], nodes[1]);
		expected.set(nodes[1], nodes[2]);
		expected.set(nodes[2], nodes[3]);
		// newly found pairs
		expected.set(nodes[0], nodes[2]);
		expected.set(nodes[0], nodes[3]);
		expected.set(nodes[1], nodes[3]);

		expected.set(nodes[4], nodes[5]);
		expected.makeSymmetric();

		Assert.assertEquals(expected, matrix);
	}
}
