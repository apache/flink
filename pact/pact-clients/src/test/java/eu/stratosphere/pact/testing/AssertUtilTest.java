package eu.stratosphere.pact.testing;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import eu.stratosphere.pact.testing.AssertUtil;

/**
 * Tests {@link AssertUtil}.
 * 
 * @author Arvid Heise
 */
public class AssertUtilTest {
	/**
	 * 
	 */
	@Test
	public void twoEmptyIteratorsShouldBeEquals() {
		AssertUtil.assertIteratorEquals(Arrays.asList().iterator(), Arrays.asList().iterator());
	}
	
	/**
	 * 
	 */
	@Test(expected = NullPointerException.class)
	public void nullAsExpectedIteratorShouldFail() {
		AssertUtil.assertIteratorEquals(null, Arrays.asList().iterator());
	}

	/**
	 * 
	 */
	@Test(expected = NullPointerException.class)
	public void nullAsActualIteratorShouldFail() {
		AssertUtil.assertIteratorEquals(Arrays.asList().iterator(), null);
	}

	/**
	 * 
	 */
	@Test
	public void twoIteratorsFromSameCollectionShouldBeEquals() {
		List<String> collection = Arrays.asList("a", "b", "c");
		AssertUtil.assertIteratorEquals(collection.iterator(), collection.iterator());
	}

	/**
	 * 
	 */
	@Test
	public void twoIteratorsFromEqualCollectionShouldBeEquals() {
		List<String> collection = Arrays.asList("a", "b", "c");
		AssertUtil.assertIteratorEquals(collection.iterator(), new ArrayList<String>(collection).iterator());
	}

	/**
	 * 
	 */
	@Test(expected = AssertionError.class)
	public void twoUnequalIteratorsShouldFail() {
		List<String> collection1 = Arrays.asList("a", "b", "c");
		List<String> collection2 = Arrays.asList("d", "e", "f");
		AssertUtil.assertIteratorEquals(collection1.iterator(), collection2.iterator());
	}

	/**
	 * 
	 */
	@Test(expected = AssertionError.class)
	public void emptyAndFilledIteratorShouldFail() {
		List<String> collection1 = Arrays.asList("a", "b", "c");
		AssertUtil.assertIteratorEquals(Arrays.asList().iterator(), collection1.iterator());
	}

	/**
	 * 
	 */
	@Test(expected = AssertionError.class)
	public void filledAndEmptyIteratorShouldFail() {
		List<String> collection1 = Arrays.asList("a", "b", "c");
		AssertUtil.assertIteratorEquals(collection1.iterator(), Arrays.asList().iterator());
	}

	/**
	 * 
	 */
	@Test(expected = AssertionError.class)
	public void differentOrderShouldFail() {
		List<String> collection1 = Arrays.asList("a", "b", "c");
		List<String> collection2 = Arrays.asList("b", "c", "a");
		AssertUtil.assertIteratorEquals(collection1.iterator(), collection2.iterator());
	}

}
