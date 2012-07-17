package eu.stratosphere.sopremo.type;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import eu.stratosphere.pact.testing.AssertUtil;
import eu.stratosphere.pact.testing.Equaler;

@Ignore
public abstract class ArrayNodeBaseTest<T extends IArrayNode> extends JsonNodeTest<T> {

	// protected T node;

	@Override
	@Before
	public void setUp() {
	}

	@Before
	public abstract void initArrayNode();

	@Test
	public void shouldAddNodes() {
		this.node.add(TextNode.valueOf("firstname"));
		this.node.add(TextNode.valueOf("lastname"));

		Assert.assertEquals(TextNode.valueOf("firstname"), this.node.get(this.node.size() - 2));
		Assert.assertEquals(TextNode.valueOf("lastname"), this.node.get(this.node.size() - 1));
	}

	@Test
	public void shouldCalculateTheCorrectSize() {
		int initialSize = this.node.size();
		int numberOfNewNodes = 6;

		for (int i = 0; i < numberOfNewNodes; i++) {
			this.node.add(TextNode.valueOf("newNode: " + i));
		}

		Assert.assertEquals(initialSize + numberOfNewNodes, this.node.size());
	}

	@Test
	public void shouldRemoveNodes() {

		this.node.add(0, TextNode.valueOf("firstname"));
		this.node.add(1, TextNode.valueOf("lastname"));

		int initialSize = this.node.size();

		this.node.remove(0);
		// index of following nodes should be decremented by 1 after removal of a node
		this.node.remove(0);

		Assert.assertEquals(initialSize - 2, this.node.size());
	}

	@Test
	public void shouldReturnCorrectNodesAfterRemoval() {
		IJsonNode value1 = TextNode.valueOf("firstname");
		IJsonNode value2 = TextNode.valueOf("lastname");

		this.node.add(0, value1);
		this.node.add(1, value2);

		Assert.assertEquals(value1, this.node.remove(0));
		Assert.assertEquals(value2, this.node.remove(0));
	}

	@Test
	public void shouldClearTheNode() {
		this.node.add(TextNode.valueOf("firstname"));
		this.node.add(TextNode.valueOf("lastname"));

		Assert.assertTrue(this.node.size() != 0);

		this.node.clear();
		Assert.assertEquals(0, this.node.size());
	}

	@Test
	public void shouldReturnTheCorrectNode() {
		this.node.add(0, TextNode.valueOf("firstname"));

		Assert.assertEquals(TextNode.valueOf("firstname"), this.node.get(0));
	}

	@Test
	public void shouldReturnMissingIfGetIndexOutOfRange() {
		// index range of node: 0 to size -1
		Assert.assertSame(MissingNode.getInstance(), this.node.get(this.node.size()));
	}

	@Test
	public void shouldReturnMissingIfRemoveIndexOutOfRange() {
		// index range of node: 0 to size - 1
		Assert.assertSame(MissingNode.getInstance(), this.node.remove(this.node.size()));
	}

	@Test(expected = IndexOutOfBoundsException.class)
	public void shouldThrowExceptionIfAddingWithWrongIndex() {
		this.node.add(this.node.size() + 1, TextNode.valueOf("firstname"));
	}

	@Test
	public void shouldCreateIterator() {
		this.node.clear();
		List<IJsonNode> expected = new ArrayList<IJsonNode>();

		for (int i = 0; i < 10; i++) {
			IJsonNode value = IntNode.valueOf(i);

			expected.add(value);
			this.node.add(value);
		}

		Iterator<IJsonNode> it = this.node.iterator();
		AssertUtil.assertIteratorEquals(expected.iterator(), it, Equaler.JavaEquals);
	}

	@Test
	public void shouldBeEqualWithAnotherArrayNode() {
		Assert.assertEquals(this.higherNode(), this.higherNode());
	}
}