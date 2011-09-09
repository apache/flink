package eu.stratosphere.pact.testing;

import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;


import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.testing.AssertUtil;
import eu.stratosphere.pact.testing.TestPairs;

/**
 * Tests {@link TestPairs}.
 * 
 * @author Arvid Heise
 */
@SuppressWarnings("unchecked")
public class TestPairsTest {
	private TestPairs<Key, Value> pairs;

	/**
	 * 
	 */
	@Before
	public void setup() {
		this.pairs = new TestPairs<Key, Value>();
	}

	/**
	 * 
	 */
	@Test
	public void singleAddShouldAddOneItem() {
		AssertUtil.assertIteratorEquals("should be empty in the beginning", Arrays.asList().iterator(), this.pairs
			.iterator());
		this.pairs.add(new PactInteger(1), new PactString("test1"));
		AssertUtil.assertIteratorEquals("should contain one element after invoking add once", Arrays.asList(
			new KeyValuePair<Key, Value>(new PactInteger(1), new PactString("test1"))).iterator(), this.pairs.iterator());
		this.pairs.add(new PactInteger(2), new PactString("test2"));
		AssertUtil.assertIteratorEquals("should contain two element after invoking add twice", Arrays.asList(
			new KeyValuePair<Key, Value>(new PactInteger(1), new PactString("test1")),
			new KeyValuePair<Key, Value>(new PactInteger(2), new PactString("test2"))).iterator(), this.pairs.iterator());
	}

	/**
	 * 
	 */
	@Test
	public void iterableAddShouldAddAllItems() {
		AssertUtil.assertIteratorEquals("should be empty in the beginning", Arrays.asList().iterator(), this.pairs
			.iterator());
		this.pairs.add(Arrays.asList(new KeyValuePair<Key, Value>(new PactInteger(1), new PactString("test1")),
			new KeyValuePair<Key, Value>(new PactInteger(2), new PactString("test2"))));
		AssertUtil.assertIteratorEquals("should contain two element after batch adding two items", Arrays.asList(
			new KeyValuePair<Key, Value>(new PactInteger(1), new PactString("test1")),
			new KeyValuePair<Key, Value>(new PactInteger(2), new PactString("test2"))).iterator(), this.pairs.iterator());
	}

	/**
	 * 
	 */
	@Test
	public void arrayAddShouldAddAllItems() {
		AssertUtil.assertIteratorEquals("should be empty in the beginning", Arrays.asList().iterator(), this.pairs
			.iterator());
		this.pairs.add(new KeyValuePair<Key, Value>(new PactInteger(1), new PactString("test1")),
			new KeyValuePair<Key, Value>(new PactInteger(2), new PactString("test2")));
		AssertUtil.assertIteratorEquals("should contain two element after batch adding two items", Arrays.asList(
			new KeyValuePair<Key, Value>(new PactInteger(1), new PactString("test1")),
			new KeyValuePair<Key, Value>(new PactInteger(2), new PactString("test2"))).iterator(), this.pairs.iterator());
	}
}
