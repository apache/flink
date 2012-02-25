package eu.stratosphere.sopremo.type;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public abstract class ArrayNodeBaseTest<T extends IArrayNode> {

	protected T node;

	@Before
	public abstract void initArrayNode();

	@Test
	public void shouldAddNodes() {
		this.node.add(0, TextNode.valueOf("firstname"));
		this.node.add(1, TextNode.valueOf("lastname"));

		Assert.assertEquals(TextNode.valueOf("firstname"), this.node.get(0));
		Assert.assertEquals(TextNode.valueOf("lastname"), this.node.get(1));
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
		// index of following nodes should be decremented by 1 after removale of a node
		this.node.remove(0);

		Assert.assertEquals(initialSize - 2, this.node.size());
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
	public void shouldReturnMissingIfIndexOutOfRange() {
		// index range of node: 0 to size -1
		Assert.assertSame(MissingNode.getInstance(), this.node.get(this.node.size()));
	}

	@Test(expected = IndexOutOfBoundsException.class)
	public void shouldThrowExceptionIfSettingWrongIndex() {
		// setting index = size should be possible to let the arraynode grow if needed
		this.node.add(this.node.size() + 1, TextNode.valueOf("firstname"));
	}
}