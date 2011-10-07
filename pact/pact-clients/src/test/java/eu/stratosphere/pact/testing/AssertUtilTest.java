/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.testing;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

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
