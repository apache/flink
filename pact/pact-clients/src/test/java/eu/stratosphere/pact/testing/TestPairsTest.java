/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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

import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;

/**
 * Tests {@link TestPairs}.
 * 
 * @author Arvid Heise
 */
public class TestPairsTest {
	private TestRecords pairs;

	private static PactRecordEqualer IntStringEqualer = new PactRecordEqualer(PactInteger.class, PactString.class);

	/**
	 * 
	 */
	@Before
	public void setup() {
		this.pairs = new TestRecords();
	}

	/**
	 * 
	 */
	@Test
	public void singleAddShouldAddOneItem() {
		AssertUtil.assertIteratorEquals("should be empty in the beginning",
			new ArrayList<PactRecord>().iterator(),
			this.pairs.iterator(),
			IntStringEqualer);

		this.pairs.add(new PactInteger(1), new PactString("test1"));
		AssertUtil.assertIteratorEquals("should contain one element after invoking add once",
			Arrays.asList(new PactRecord(new PactInteger(1), new PactString("test1"))).iterator(),
			this.pairs.iterator(),
			IntStringEqualer);

		this.pairs.add(new PactInteger(2), new PactString("test2"));
		AssertUtil.assertIteratorEquals("should contain two element after invoking add twice",
			Arrays.asList(
				new PactRecord(new PactInteger(1), new PactString("test1")),
				new PactRecord(new PactInteger(2), new PactString("test2"))).iterator(),
			this.pairs.iterator(),
			IntStringEqualer);
	}

	/**
	 * 
	 */
	@Test
	public void iterableAddShouldAddAllItems() {
		AssertUtil.assertIteratorEquals("should be empty in the beginning",
			new ArrayList<PactRecord>().iterator(),
			this.pairs.iterator(),
			IntStringEqualer);

		this.pairs.add(Arrays.asList(new PactRecord(new PactInteger(1), new PactString("test1")),
			new PactRecord(new PactInteger(2), new PactString("test2"))));
		AssertUtil.assertIteratorEquals("should contain two element after batch adding two items",
			Arrays.asList(
				new PactRecord(new PactInteger(1), new PactString("test1")),
				new PactRecord(new PactInteger(2), new PactString("test2"))).iterator(),
			this.pairs.iterator(),
			IntStringEqualer);
	}

	/**
	 * 
	 */
	@Test
	public void arrayAddShouldAddAllItems() {
		AssertUtil.assertIteratorEquals("should be empty in the beginning",
			new ArrayList<PactRecord>().iterator(),
			this.pairs.iterator(),
			IntStringEqualer);

		this.pairs.add(new PactRecord(new PactInteger(1), new PactString("test1")),
			new PactRecord(new PactInteger(2), new PactString("test2")));
		AssertUtil.assertIteratorEquals("should contain two element after batch adding two items",
			Arrays.asList(
				new PactRecord(new PactInteger(1), new PactString("test1")),
				new PactRecord(new PactInteger(2), new PactString("test2"))).iterator(),
			this.pairs.iterator(),
			IntStringEqualer);
	}
}
