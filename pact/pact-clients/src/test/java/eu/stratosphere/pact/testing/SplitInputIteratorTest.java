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

import java.io.IOException;
import java.util.Arrays;
import java.util.NoSuchElementException;

import org.junit.Assert;
import org.junit.Test;

import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.testing.ioformats.FormatUtil;
import eu.stratosphere.pact.testing.ioformats.SequentialInputFormat;
import eu.stratosphere.pact.testing.ioformats.SequentialOutputFormat;

/**
 * Tests {@link SplitInputIterator}.
 * 
 * @author Arvid Heise
 */
public class SplitInputIteratorTest {
	/**
	 * Tests if a input split iterator of an empty file returns any pairs at all.
	 * 
	 * @throws IOException
	 *         if an I/O exception occurred
	 */
	@Test
	public void emptyIteratorShouldReturnNoElements() throws IOException {
		SplitInputIterator<Key, Value> inputFileIterator = createFileIterator();

		AssertUtil.assertIteratorEquals("input file iterator is not empty", Arrays.asList().iterator(),
			inputFileIterator);
	}

	/**
	 * Tests if a input split iterator of an empty file returns any pairs at all.
	 * 
	 * @throws IOException
	 *         if an I/O exception occurred
	 */
	@Test
	public void filledIteratorShouldReturnExactlyTheGivenArguments() throws IOException {
		KeyValuePair<?, ?>[] pairs = { new KeyValuePair<Key, Value>(new PactInteger(1), new PactString("test1")),
			new KeyValuePair<Key, Value>(new PactInteger(2), new PactString("test2")) };
		SplitInputIterator<Key, Value> inputFileIterator = createFileIterator(pairs);

		AssertUtil.assertIteratorEquals("input file iterator is does not return the right sequence of pairs", Arrays
			.asList(pairs).iterator(), inputFileIterator);
	}

	/**
	 * Tests if a input split iterator of a non-existent file fails.
	 * 
	 * @throws IOException
	 *         if an I/O exception occurred
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void emptyIteratorIfInputFileDoesNotExists() throws IOException {
		String testPlanFile = TestPlan.getTestPlanFile("fileIteratorTest");
		SequentialInputFormat<Key, Value> inputFormat = FormatUtil.createInputFormat(SequentialInputFormat.class,
			testPlanFile, null);
		SplitInputIterator<Key, Value> inputFileIterator = new SplitInputIterator<Key, Value>(inputFormat);

		AssertUtil.assertIteratorEquals("input file iterator is not empty", Arrays.asList().iterator(),
			inputFileIterator);
	}

	/**
	 * Tests if a input split iterator of a non-existent file fails.
	 * 
	 * @throws IOException
	 *         if an I/O exception occurred
	 */
	@Test
	public void failIfReadTwoManyItems() throws IOException {
		KeyValuePair<?, ?>[] pairs = { new KeyValuePair<Key, Value>(new PactInteger(1), new PactString("test1")),
			new KeyValuePair<Key, Value>(new PactInteger(2), new PactString("test2")) };
		SplitInputIterator<Key, Value> inputFileIterator = createFileIterator(pairs);

		while (inputFileIterator.hasNext())
			Assert.assertNotNull(inputFileIterator.next());

		try {
			inputFileIterator.next();
			Assert.fail("should have thrown Exception");
		} catch (NoSuchElementException e) {
		}
	}

	/**
	 * Tests if a input split iterator of an empty file returns any pairs at all.
	 * 
	 * @throws IOException
	 *         if an I/O exception occurred
	 */
	@Test
	public void iteratorShouldCreateNewPairs() throws IOException {
		KeyValuePair<?, ?>[] pairs = { new KeyValuePair<Key, Value>(new PactInteger(1), new PactString("test1")),
			new KeyValuePair<Key, Value>(new PactInteger(2), new PactString("test2")),
			new KeyValuePair<Key, Value>(new PactInteger(3), new PactString("test3")) };
		SplitInputIterator<Key, Value> inputFileIterator = createFileIterator(pairs);

		AssertUtil.assertIteratorEquals("input file iterator is does not return the right sequence of pairs", Arrays
			.asList(pairs).iterator(), inputFileIterator);

		inputFileIterator = createFileIterator(pairs);
		KeyValuePair<Key, Value> lastPair = null;
		for (int index = 0; inputFileIterator.hasNext(); index++) {
			KeyValuePair<Key, Value> currentPair = inputFileIterator.next();
			Assert.assertNotSame("should have created different instances", currentPair, lastPair);
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private SplitInputIterator<Key, Value> createFileIterator(KeyValuePair<?, ?>... pairs)
			throws IOException {
		String testPlanFile = TestPlan.getTestPlanFile("fileIteratorTest");
		SequentialOutputFormat output = FormatUtil.createOutputFormat(SequentialOutputFormat.class,
			testPlanFile, null);
		for (KeyValuePair keyValuePair : pairs)
			output.writePair(keyValuePair);
		output.close();
		SequentialInputFormat<Key, Value> inputFormat = FormatUtil.createInputFormat(SequentialInputFormat.class,
			testPlanFile, null);
		SplitInputIterator<Key, Value> inputFileIterator = new SplitInputIterator<Key, Value>(inputFormat);
		return inputFileIterator;
	}
}
