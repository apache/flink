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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import junit.framework.AssertionFailedError;

import org.junit.Assert;
import org.junit.Test;
import org.junit.internal.ArrayComparisonFailure;

import eu.stratosphere.pact.common.contract.CoGroupContract;
import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.contract.CrossContract;
import eu.stratosphere.pact.common.contract.FileDataSinkContract;
import eu.stratosphere.pact.common.contract.FileDataSourceContract;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.contract.OutputContract.SameKey;
import eu.stratosphere.pact.common.contract.ReduceContract.Combinable;
import eu.stratosphere.pact.common.io.FileInputFormat;
import eu.stratosphere.pact.common.io.FileOutputFormat;
import eu.stratosphere.pact.common.stub.CoGroupStub;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.CrossStub;
import eu.stratosphere.pact.common.stub.MapStub;
import eu.stratosphere.pact.common.stub.MatchStub;
import eu.stratosphere.pact.common.stub.ReduceStub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactJsonObject;
import eu.stratosphere.pact.common.type.base.PactList;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.common.type.base.PactNull;
import eu.stratosphere.pact.common.type.base.PactPair;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.testing.ioformats.JsonInputFormat;
import eu.stratosphere.pact.testing.ioformats.JsonOutputFormat;
import eu.stratosphere.pact.testing.ioformats.SequentialOutputFormat;

/**
 * Tests {@link TestPlan}.
 * 
 * @author Arvid Heise
 */
public class TestPlanTest extends TestPlanTestCase {
	/**
	 * Pair of {@link PactInteger}s.
	 * 
	 * @author Arvid Heise
	 */
	public static final class IntPair extends PactPair<PactInteger, PactInteger> {
		/**
		 * Initializes IntPair.
		 */
		public IntPair() {
		}

		private IntPair(PactInteger first, PactInteger second) {
			super(first, second);
		}

		private IntPair(int first, int second) {
			super(new PactInteger(first), new PactInteger(second));
		}
	}

	/**
	 * Pair of {@link PactString}s.
	 * 
	 * @author Arvid Heise
	 */
	public static final class StringPair extends PactPair<PactString, PactString> {
		/**
		 * Initializes StringPair.
		 */
		public StringPair() {
		}

		private StringPair(PactString first, PactString second) {
			super(first, second);
		}

		private StringPair(String first, String second) {
			super(new PactString(first), new PactString(second));
		}
	}

	/**
	 * (int1, string1) x (int2, string2) -&gt; ((int1, int2), (string1, string2))
	 * 
	 * @author Arvid Heise
	 */
	public static final class CartesianProduct extends
			CrossStub<PactInteger, PactString, PactInteger, PactString, IntPair, StringPair> {
		@Override
		public void cross(PactInteger key1, PactString value1, PactInteger key2, PactString value2,
				Collector<IntPair, StringPair> out) {
			out.collect(new IntPair(key1, key2), new StringPair(value1, value2));
		}
	}

	/**
	 * (int1, [string1, string2]) x (int1, [string3, string4]) -&gt; (int1, [string1, string2, string3, string4])
	 * 
	 * @author Arvid Heise
	 */
	public static final class AppendingCoGroup extends
			CoGroupStub<PactInteger, PactString, PactString, PactInteger, StringList> {
		@Override
		public void coGroup(PactInteger key, Iterator<PactString> values1, Iterator<PactString> values2,
				Collector<PactInteger, StringList> out) {
			StringList values = new StringList();
			while (values1.hasNext())
				values.add(new PactString(values1.next().getValue()));
			while (values2.hasNext())
				values.add(new PactString(values2.next().getValue()));
			out.collect(key, values);
		}
	}

	/**
	 * (int1, string1) x (int1, string2) -&gt; (int1, (string1, string2))
	 * 
	 * @author Arvid Heise
	 */
	public static final class Join extends
			MatchStub<PactInteger, PactString, PactString, StringPair, PactInteger> {
		@Override
		public void match(PactInteger key, PactString value1, PactString value2, Collector<StringPair, PactInteger> out) {
			out.collect(new StringPair(new PactString(value1.getValue()), new PactString(value2.getValue())), key);
		}
	}

	/**
	 * (int1, string1), (int1, string2) -&gt; (int1, [string1, string2])
	 * 
	 * @author Arvid Heise
	 */
	public static final class AppendingReduce extends
			ReduceStub<PactInteger, PactString, PactInteger, StringList> {
		@Override
		public void reduce(PactInteger key, Iterator<PactString> values, Collector<PactInteger, StringList> out) {
			StringList list = new StringList();
			while (values.hasNext())
				list.add(new PactString(values.next().getValue()));
			out.collect(key, list);
		}
	}

	/**
	 * List of {@link PactString}s.
	 * 
	 * @author Arvid Heise
	 */
	public static final class StringList extends PactList<PactString> {
		/**
		 * Initializes StringList.
		 */
		public StringList() {
		}

		private StringList(String... strings) {
			for (String string : strings)
				add(new PactString(string));
		}
	}

	/**
	 * Tests if a {@link TestPlan} can be executed.
	 */
	@Test
	public void completeTestPasses() {
		final FileDataSourceContract<PactLong, PactJsonObject> read = createInput(JsonInputFormat.class,
			"TestPlan/test.json");

		final MapContract<Key, Value, Key, Value> map =
			new MapContract<Key, Value, Key, Value>(IdentityMap.class, "Map");
		map.setInput(read);

		FileDataSinkContract<Key, Value> output = createOutput(map, SequentialOutputFormat.class);

		TestPlan testPlan = new TestPlan(output);
		testPlan.run();
		assertEquals("input and output should be equal in identity map", testPlan.getInput(), testPlan
			.getActualOutput());
	}

	/**
	 * Tests if a {@link TestPlan} without explicit data sources and sinks can be executed.
	 */
	@Test
	public void adhocInputAndOutputShouldTransparentlyWork() {
		final MapContract<Key, Value, Key, Value> map = new MapContract<Key, Value, Key, Value>(IdentityMap.class,
			"Map");
		TestPlan testPlan = new TestPlan(map);
		testPlan.getInput().
			add(new PactInteger(1), new PactString("test1")).
			add(new PactInteger(2), new PactString("test2"));
		testPlan.run();

		assertEquals("input and output should be equal in identity map", testPlan.getInput(), testPlan
			.getActualOutput());

		// explicitly check output
		Iterator<KeyValuePair<Key, Value>> outputIterator = testPlan.getActualOutput().iterator();
		Iterator<KeyValuePair<Key, Value>> inputIterator = testPlan.getInput().iterator();
		for (int index = 0; index < 2; index++) {
			assertTrue("too few actual output values", outputIterator.hasNext());
			assertTrue("too few input values", outputIterator.hasNext());
			try {
				assertEquals(inputIterator.next(), outputIterator.next());
			} catch (AssertionFailedError e) {
				throw new ArrayComparisonFailure("Could not verify output values", e, index);
			}
		}
		assertFalse("too few actual output values", outputIterator.hasNext());
		assertFalse("too few input values", outputIterator.hasNext());
	}

	/**
	 * Tests if a {@link TestPlan} can be executed.
	 */
	@Test
	public void completeTestPassesWithExpectedValues() {
		final FileDataSourceContract<PactLong, PactJsonObject> read = createInput(JsonInputFormat.class,
			"TestPlan/test.json");

		final MapContract<Key, Value, Key, Value> map = new MapContract<Key, Value, Key, Value>(IdentityMap.class,
			"Map");
		map.setInput(read);

		FileDataSinkContract<PactNull, PactJsonObject> output = createOutput(map, JsonOutputFormat.class);

		TestPlan testPlan = new TestPlan(output);
		testPlan.getExpectedOutput(output).fromFile(JsonInputFormat.class, getResourcePath("TestPlan/test.json"));
		testPlan.run();
	}

	private String getResourcePath(String resource) {
		try {
			Enumeration<URL> resources = TestPlanTest.class.getClassLoader().getResources(resource);
			if (resources.hasMoreElements())
				return resources.nextElement().toString();
		} catch (IOException e) {
			throw new IllegalStateException(e);
		}
		throw new IllegalArgumentException("no resources found");
	}

	/**
	 * Tests if a {@link TestPlan} without explicit data sources and sinks can be executed.
	 */
	@Test
	public void expectedValuesShouldAlsoWorkWithAdhocInputAndOutput() {
		final MapContract<Key, Value, Key, Value> map = new MapContract<Key, Value, Key, Value>(IdentityMap.class,
			"Map");
		TestPlan testPlan = new TestPlan(map);
		testPlan.getInput().
			add(new PactInteger(1), new PactString("test1")).
			add(new PactInteger(2), new PactString("test2"));
		testPlan.getExpectedOutput().
			add(new PactInteger(1), new PactString("test1")).
			add(new PactInteger(2), new PactString("test2"));
		testPlan.run();
	}

	/**
	 * Tests a {@link TestPlan} with a {@link CrossContract}.
	 */
	@Test
	public void crossShouldBeSupported() {
		CrossContract<PactInteger, PactString, PactInteger, PactString, IntPair, StringPair> crossContract = new CrossContract<PactInteger, PactString, PactInteger, PactString, IntPair, StringPair>(
			CartesianProduct.class);

		TestPlan testPlan = new TestPlan(crossContract);
		testPlan.getInput(0).
			add(new PactInteger(1), new PactString("test1")).
			add(new PactInteger(2), new PactString("test2"));
		testPlan.getInput(1).
			add(new PactInteger(3), new PactString("test3")).
			add(new PactInteger(4), new PactString("test4"));

		testPlan.getExpectedOutput().
			add(new IntPair(1, 3), new StringPair("test1", "test3")).
			add(new IntPair(1, 4), new StringPair("test1", "test4")).
			add(new IntPair(2, 3), new StringPair("test2", "test3")).
			add(new IntPair(2, 4), new StringPair("test2", "test4"));
		testPlan.run();
	}

	/**
	 * Tests a {@link TestPlan} with a {@link CoGroupContract}.
	 */
	@Test
	public void coGroupShouldBeSupported() {
		CoGroupContract<PactInteger, PactString, PactString, PactInteger, StringList> crossContract =
			new CoGroupContract<PactInteger, PactString, PactString, PactInteger, StringList>(AppendingCoGroup.class);

		TestPlan testPlan = new TestPlan(crossContract);
		testPlan.getInput(0).
			add(new PactInteger(1), new PactString("test1")).
			add(new PactInteger(1), new PactString("test2")).
			add(new PactInteger(2), new PactString("test3"));
		testPlan.getInput(1).
			add(new PactInteger(1), new PactString("test4")).
			add(new PactInteger(3), new PactString("test5"));

		testPlan.getExpectedOutput().
			add(new PactInteger(1), new StringList("test1", "test2", "test4")).
			add(new PactInteger(2), new StringList("test3")).
			add(new PactInteger(3), new StringList("test5"));
		testPlan.run();
	}

	/**
	 * Tests a {@link TestPlan} with a {@link CoGroupContract}.
	 */
	@Test
	public void matchShouldBeSupported() {
		MatchContract<PactInteger, PactString, PactString, StringPair, PactInteger> crossContract =
			new MatchContract<PactInteger, PactString, PactString, StringPair, PactInteger>(Join.class);

		TestPlan testPlan = new TestPlan(crossContract);
		testPlan.getInput(0).
			add(new PactInteger(1), new PactString("test1")).
			add(new PactInteger(1), new PactString("test2")).
			add(new PactInteger(2), new PactString("test3"));
		testPlan.getInput(1).
			add(new PactInteger(1), new PactString("test4")).
			add(new PactInteger(3), new PactString("test5"));

		testPlan.getExpectedOutput().
			add(new StringPair("test1", "test4"), new PactInteger(1)).
			add(new StringPair("test2", "test4"), new PactInteger(1));
		testPlan.run();
	}

	/**
	 * Tests a {@link TestPlan} with a {@link CoGroupContract}.
	 */
	@Test
	public void reduceShouldBeSupported() {
		ReduceContract<PactInteger, PactString, PactInteger, StringList> crossContract =
			new ReduceContract<PactInteger, PactString, PactInteger, StringList>(AppendingReduce.class);

		TestPlan testPlan = new TestPlan(crossContract);
		testPlan.getInput().
			add(new PactInteger(1), new PactString("test1")).
			add(new PactInteger(1), new PactString("test2")).
			add(new PactInteger(2), new PactString("test3")).
			add(new PactInteger(1), new PactString("test4")).
			add(new PactInteger(3), new PactString("test5"));

		testPlan.getExpectedOutput().
			add(new PactInteger(1), new StringList("test1", "test2", "test4")).
			add(new PactInteger(2), new StringList("test3")).
			add(new PactInteger(3), new StringList("test5"));
		testPlan.run();
	}

	/**
	 * Tests a {@link TestPlan} with a {@link CrossContract}.
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void settingValuesShouldWorkWithSourceContracts() {
		CrossContract<PactInteger, PactString, PactInteger, PactString, IntPair, StringPair> crossContract = new CrossContract<PactInteger, PactString, PactInteger, PactString, IntPair, StringPair>(
			CartesianProduct.class);

		TestPlan testPlan = new TestPlan(crossContract);
		// first and second input are added in TestPlan
		testPlan.getInput((FileDataSourceContract<PactInteger, PactString>) crossContract.getFirstInput()).
			add(new PactInteger(1), new PactString("test1")).
			add(new PactInteger(2), new PactString("test2"));
		testPlan.getInput((FileDataSourceContract<PactInteger, PactString>) crossContract.getSecondInput()).
			add(new PactInteger(3), new PactString("test3")).
			add(new PactInteger(4), new PactString("test4"));

		testPlan.getExpectedOutput().
			add(new IntPair(1, 3), new StringPair("test1", "test3")).
			add(new IntPair(1, 4), new StringPair("test1", "test4")).
			add(new IntPair(2, 3), new StringPair("test2", "test3")).
			add(new IntPair(2, 4), new StringPair("test2", "test4"));
		testPlan.run();
	}

	/**
	 * Tests {@link TestPlan#setDegreeOfParallelism(int)}.
	 */
	@Test
	public void degreeOfParallelismShouldBeConfigurable() {
		final MapContract<Key, Value, Key, Value> map = new MapContract<Key, Value, Key, Value>(IdentityMap.class,
			"Map");
		TestPlan testPlan = new TestPlan(map);
		testPlan.getInput().
			add(new PactInteger(1), new PactString("test1")).
			add(new PactInteger(2), new PactString("test2"));
		testPlan.getExpectedOutput().
			add(new PactInteger(1), new PactString("test1")).
			add(new PactInteger(2), new PactString("test2"));
		testPlan.setDegreeOfParallelism(2);
		Assert.assertEquals(2, testPlan.getDegreeOfParallelism());
		testPlan.run();
		Assert.assertEquals(2, map.getDegreeOfParallelism());
	}

	/**
	 * Creates an output file in the temporary folder for arbitrary key/value pairs coming from the given input
	 * contract.
	 * 
	 * @param input
	 *        the input from which the values are read
	 * @param outputFormatClass
	 *        the output format
	 * @return the {@link FileDataSinkContract} for the temporary file
	 */
	private <K extends Key, V extends Value> FileDataSinkContract<K, V> createOutput(final Contract input,
			final Class<? extends FileOutputFormat<K, V>> outputFormatClass) {
		try {
			final FileDataSinkContract<K, V> out = new FileDataSinkContract<K, V>(outputFormatClass, File.createTempFile(
				"output", null).toURI().toString(), "Output");
			out.setInput(input);
			return out;
		} catch (IOException e) {
			fail("cannot create temporary output file" + e);
			return null;
		}
	}

	/**
	 * Creates an {@link FileDataSourceContract} contract for the specified resource file in the temporary folder for
	 * arbitrary key/value pairs coming from the given input
	 * contract.
	 * 
	 * @param input
	 *        the input from which the values are read
	 * @return the {@link FileDataSinkContract} for the temporary file
	 */
	private <K extends Key, V extends Value> FileDataSourceContract<K, V> createInput(
			Class<? extends FileInputFormat<K, V>> inputFormat, String resource) {
		final FileDataSourceContract<K, V> read = new FileDataSourceContract<K, V>(inputFormat, getResourcePath(resource),
			"Input");
		return read;
	}

	/**
	 * Converts a (String,Integer)-KeyValuePair into multiple KeyValuePairs. The
	 * key string is tokenized by spaces. For each token a new
	 * (String,Integer)-KeyValuePair is emitted where the Token is the key and
	 * an Integer(1) is the value.
	 */
	public static class TokenizeLine extends MapStub<PactString, PactInteger, PactString, PactInteger> {
		private static Pattern WORD_PATTERN = Pattern.compile("\\w+");

		@Override
		public void map(PactString key, PactInteger value, Collector<PactString, PactInteger> out) {
			Matcher matcher = WORD_PATTERN.matcher(key.getValue());
			while (matcher.find())
				out.collect(new PactString(matcher.group().toLowerCase()), new PactInteger(1));
		}
	}

	/**
	 * Counts the number of values for a given key. Hence, the number of
	 * occurences of a given token (word) is computed and emitted. The key is
	 * not modified, hence a SameKey OutputContract is attached to this class.
	 */
	@SameKey
	@Combinable
	public static class CountWords extends ReduceStub<PactString, PactInteger, PactString, PactInteger> {
		@Override
		public void reduce(PactString key, Iterator<PactInteger> values, Collector<PactString, PactInteger> out) {
			int sum = 0;
			while (values.hasNext())
				sum += values.next().getValue();
			out.collect(key, new PactInteger(sum));
		}

		@Override
		public void combine(PactString key, Iterator<PactInteger> values, Collector<PactString, PactInteger> out) {
			this.reduce(key, values, out);
		}
	}

	/**
	 * Tests if a {@link TestPlan} with two stubs can be executed.
	 */
	@Test
	public void complexTestPassesWithExpectedValues() {
		final MapContract<PactString, PactInteger, PactString, PactInteger> tokenize = new MapContract<PactString, PactInteger, PactString, PactInteger>(
			TokenizeLine.class,
			"Map");
		final ReduceContract<PactString, PactInteger, PactString, PactInteger> summing = new ReduceContract<PactString, PactInteger, PactString, PactInteger>(
			CountWords.class,
			"Map");
		summing.setInput(tokenize);

		TestPlan testPlan = new TestPlan(summing);
		String[] lines = {
			"Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.",
			"Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.",
			"Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur.",
			"Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum." };
		for (String line : lines)
			testPlan.getInput().add(new PactString(line), new PactInteger(1));

		String[] singleWords = { "voluptate", "veniam", "velit", "ullamco", "tempor", "sunt", "sit", "sint", "sed",
			"reprehenderit", "quis", "qui", "proident", "pariatur", "officia", "occaecat", "nulla", "nostrud", "non",
			"nisi", "mollit", "minim", "magna", "lorem", "laborum", "laboris", "labore", "irure", "ipsum",
			"incididunt", "id", "fugiat", "exercitation", "excepteur", "ex", "eu", "et", "est", "esse", "enim", "elit",
			"eiusmod", "ea", "duis", "do", "deserunt", "cupidatat", "culpa", "consequat", "consectetur", "commodo",
			"cillum", "aute", "anim", "amet", "aliquip", "aliqua", "adipisicing", "ad" };
		for (String singleWord : singleWords)
			testPlan.getExpectedOutput().add(new PactString(singleWord), new PactInteger(1));
		testPlan.getExpectedOutput().add(new PactString("ut"), new PactInteger(3)).
			add(new PactString("in"), new PactInteger(3)).
			add(new PactString("dolore"), new PactInteger(2)).
			add(new PactString("dolor"), new PactInteger(2));
		testPlan.run();
	}

	/**
	 * Tests if a {@link TestPlan} fails if the actual values do not match the expected values.
	 */
	@Test
	public void shouldFailIfExpectedAndActualValuesDiffer() {
		final MapContract<Key, Value, Key, Value> map = new MapContract<Key, Value, Key, Value>(IdentityMap.class,
			"Map");
		TestPlan testPlan = new TestPlan(map);
		testPlan.getInput().
			add(new PactInteger(1), new PactString("test1")).
			add(new PactInteger(2), new PactString("test2"));
		testPlan.getExpectedOutput().
			add(new PactInteger(1), new PactString("test1")).
			add(new PactInteger(2), new PactString("test3"));
		assertTestRunFails(testPlan);
	}

	/**
	 * Tests if a {@link TestPlan} fails there are too many values.
	 */
	@Test
	public void shouldFailIfTooManyValues() {
		final MapContract<Key, Value, Key, Value> map = new MapContract<Key, Value, Key, Value>(IdentityMap.class,
			"Map");
		TestPlan testPlan = new TestPlan(map);
		testPlan.getInput().
			add(new PactInteger(1), new PactString("test1")).
			add(new PactInteger(2), new PactString("test2"));
		testPlan.getExpectedOutput().
			add(new PactInteger(1), new PactString("test1"));
		assertTestRunFails(testPlan);
	}

	/**
	 * Tests if a {@link TestPlan} fails there are too few values.
	 */
	@Test
	public void shouldFailIfTooFewValues() {
		final MapContract<Key, Value, Key, Value> map = new MapContract<Key, Value, Key, Value>(IdentityMap.class,
			"Map");
		TestPlan testPlan = new TestPlan(map);
		testPlan.getInput().
			add(new PactInteger(1), new PactString("test1")).
			add(new PactInteger(2), new PactString("test2"));
		testPlan.getExpectedOutput().
			add(new PactInteger(1), new PactString("test1")).
			add(new PactInteger(2), new PactString("test2")).
			add(new PactInteger(3), new PactString("test3"));
		assertTestRunFails(testPlan);
	}

	private void assertTestRunFails(TestPlan testPlan) {
		boolean failed = false;
		try {
			testPlan.run();
		} catch (AssertionError error) {
			failed = true;
		}
		assertTrue("Test plan should have failed", failed);
	}

	/**
	 * Tests if a {@link TestPlan} succeeds with uninitialized expected values.
	 */
	@Test
	public void shouldSuceedIfNoExpectedValues() {
		final MapContract<Key, Value, Key, Value> map = new MapContract<Key, Value, Key, Value>(IdentityMap.class,
			"Map");
		TestPlan testPlan = new TestPlan(map);
		testPlan.getInput().
			add(new PactInteger(1), new PactString("test1")).
			add(new PactInteger(2), new PactString("test2"));
		testPlan.run();
	}

	/**
	 * Tests if a {@link TestPlan} fails if actual values appear but empty values are expected.
	 */
	@Test
	public void shouldFailIfNonEmptyExpectedValues() {
		final MapContract<Key, Value, Key, Value> map = new MapContract<Key, Value, Key, Value>(IdentityMap.class,
			"Map");
		TestPlan testPlan = new TestPlan(map);
		testPlan.getInput().
			add(new PactInteger(1), new PactString("test1")).
			add(new PactInteger(2), new PactString("test2"));
		testPlan.getExpectedOutput().setEmpty();
		assertTestRunFails(testPlan);
	}
}
