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

package eu.stratosphere.sopremo.testing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import junit.framework.AssertionFailedError;
import nl.jqno.equalsverifier.EqualsVerifier;

import org.junit.Test;
import org.junit.internal.ArrayComparisonFailure;

import eu.stratosphere.pact.common.contract.ReduceContract.Combinable;
import eu.stratosphere.pact.testing.TestRecords;
import eu.stratosphere.sopremo.SopremoTest;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.io.Sink;
import eu.stratosphere.sopremo.io.Source;
import eu.stratosphere.sopremo.operator.ElementaryOperator;
import eu.stratosphere.sopremo.operator.InputCardinality;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoCross;
import eu.stratosphere.sopremo.pact.SopremoMap;
import eu.stratosphere.sopremo.pact.SopremoReduce;
import eu.stratosphere.sopremo.serialization.ObjectSchema;
import eu.stratosphere.sopremo.serialization.Schema;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IObjectNode;
import eu.stratosphere.sopremo.type.IntNode;
import eu.stratosphere.sopremo.type.JsonUtil;
import eu.stratosphere.sopremo.type.TextNode;

/**
 * Tests {@link SopremoTestPlan}.
 * 
 * @author Arvid Heise
 */

public class SopremoTestPlanTest extends SopremoTest<SopremoTestPlan> {
	/**
	 * Tests if a {@link SopremoTestPlan} without explicit data sources and sinks can be executed.
	 */
	@Test
	public void adhocInputAndOutputShouldTransparentlyWork() {
		final SopremoTestPlan testPlan = new SopremoTestPlan(new Identity());
		testPlan.getInput(0).
			addValue("test1").
			addValue("test2");
		testPlan.run();

		assertEquals("input and output should be equal in identity map", testPlan.getInput(0), testPlan
			.getActualOutput(0));

		// explicitly check output
		final Iterator<IJsonNode> outputIterator = testPlan.getActualOutput(0).iterator();
		final Iterator<IJsonNode> inputIterator = testPlan.getInput(0).iterator();
		for (int index = 0; index < 2; index++) {
			assertTrue("too few actual output values", outputIterator.hasNext());
			assertTrue("too few input values", outputIterator.hasNext());
			try {
				assertEquals(inputIterator.next(), outputIterator.next());
			} catch (final AssertionFailedError e) {
				throw new ArrayComparisonFailure("Could not verify output values", e, index);
			}
		}
		assertFalse("too few actual output values", outputIterator.hasNext());
		assertFalse("too few input values", outputIterator.hasNext());
	}

	/**
	 * Tests if a {@link SopremoTestPlan} can be executed.
	 * 
	 * @throws IOException
	 */
	@Test
	public void completeTestPasses() throws IOException {
		final Source source = new Source(getResourcePath("SopremoTestPlan/test.json"));

		final Identity projection = new Identity();
		projection.setInputs(source);

		final Sink sink = new Sink(File.createTempFile("output", null).toURI().toString());
		sink.setInputs(projection);

		final SopremoTestPlan testPlan = new SopremoTestPlan(sink);
		testPlan.run();
		assertEquals("input and output should be equal in identity projection", testPlan.getInput(0), testPlan
			.getActualOutput(0));
	}

	/**
	 * Tests if a {@link SopremoTestPlan} can be executed.
	 */
	@Test
	public void completeTestPassesWithExpectedValues() {
		final SopremoTestPlan testPlan = new SopremoTestPlan(new Identity().
			withInputs(new Source(getResourcePath("SopremoTestPlan/test.json"))));

		testPlan.getExpectedOutput(0).setOperator(new Source(getResourcePath("SopremoTestPlan/test.json")));
		testPlan.run();
	}

	@Override
	protected SopremoTestPlan createDefaultInstance(final int index) {
		return new SopremoTestPlan(index, 1);
	}

	/**
	 * Tests if a {@link SopremoTestPlan} without explicit data sources and sinks can be executed.
	 */
	@Test
	public void expectedValuesShouldAlsoWorkWithAdhocInputAndOutput() {
		final SopremoTestPlan testPlan = new SopremoTestPlan(new Identity());
		testPlan.getInput(0).
			addValue("test1").
			addValue("test2");
		testPlan.getExpectedOutput(0).
			addValue("test1").
			addValue("test2");
		testPlan.run();
	}

	@SuppressWarnings("resource")
	@Override
	protected void initVerifier(final EqualsVerifier<SopremoTestPlan> equalVerifier) {
		super.initVerifier(equalVerifier);
		final ObjectSchema redSchema = new ObjectSchema("redField");
		equalVerifier.
			withPrefabValues(
				TestRecords.class,
				new TestRecords(redSchema.getPactSchema()).add(
					redSchema.jsonToRecord(JsonUtil.createObjectNode("color", "red"), null, null)),
				new TestRecords(redSchema.getPactSchema()).add(
					redSchema.jsonToRecord(JsonUtil.createObjectNode("color", "black"), null, null))).
			withPrefabValues(Schema.class,
				redSchema,
				new ObjectSchema("blackField")).
			// withPrefabValues(SopremoTestPlan.ActualOutput.class,
			// new SopremoTestPlan.ActualOutput(0).addValue(0),
			// new SopremoTestPlan.ActualOutput(1).addValue(1)).
			withPrefabValues(SopremoTestPlan.ExpectedOutput.class,
				new SopremoTestPlan.ExpectedOutput(null, 0).addValue(0),
				new SopremoTestPlan.ExpectedOutput(null, 1).addValue(1)).
			withPrefabValues(SopremoTestPlan.Input.class,
				new SopremoTestPlan.Input(null, 0).addValue(0),
				new SopremoTestPlan.Input(null, 1).addValue(1));
	}

	/**
	 * Tests a {@link SopremoTestPlan} with a {@link CrossContract}.
	 */

	@Test
	public void settingValuesShouldWorkWithSourceContracts() {
		final CartesianProduct cartesianProduct = new CartesianProduct();
		final SopremoTestPlan testPlan = new SopremoTestPlan(cartesianProduct);
		testPlan.getInputForStream(cartesianProduct.getInput(0)).
			addValue("test1").
			addValue("test2");
		testPlan.getInputForStream(cartesianProduct.getInput(1)).
			addValue("test3").
			addValue("test4");
		testPlan.getExpectedOutputForStream(cartesianProduct.getOutput(0)).
			addArray("test1", "test3").
			addArray("test1", "test4").
			addArray("test2", "test3").
			addArray("test2", "test4");
		testPlan.run();
	}

	@InputCardinality(2)
	public static class CartesianProduct extends ElementaryOperator<CartesianProduct> {
		/**
		 * 
		 */
		private static final long serialVersionUID = -4747731771822553359L;

		public static class Implementation extends SopremoCross {
			@Override
			protected void cross(final IJsonNode value1, final IJsonNode value2, final JsonCollector out) {
				out.collect(JsonUtil.asArray(value1, value2));
			}
		}
	}

	@InputCardinality(1)
	public static class Identity extends ElementaryOperator<Identity> {
		/**
		 * 
		 */
		private static final long serialVersionUID = 6764940997115103382L;

		public static class Implementation extends SopremoMap {
			@Override
			protected void map(final IJsonNode value, final JsonCollector out) {
				out.collect(value);
			}
		}
	}

	//
	// /**
	// * Creates an output file in the temporary folder for arbitrary key/value pairs coming from the given input
	// * contract.
	// *
	// * @param input
	// * the input from which the values are read
	// * @param outputFormatClass
	// * the output format
	// * @return the {@link DataSinkContract} for the temporary file
	// */
	// private <K extends Key, V extends Value> DataSinkContract<K, V> createOutput(final Contract input,
	// final Class<? extends OutputFormat<K, V>> outputFormatClass) {
	// try {
	// final DataSinkContract<K, V> out = new DataSinkContract<K, V>(outputFormatClass, File.createTempFile(
	// "output", null).toURI().toString(), "Output");
	// out.setInput(input);
	// return out;
	// } catch (IOException e) {
	// fail("cannot create temporary output file" + e);
	// return null;
	// }
	// }
	//
	// /**
	// * Creates an {@link DataSourceContract} contract for the specified resource file in the temporary folder for
	// * arbitrary key/value pairs coming from the given input
	// * contract.
	// *
	// * @param input
	// * the input from which the values are read
	// * @return the {@link DataSinkContract} for the temporary file
	// */
	// private <K extends Key, V extends Value> DataSourceContract<K, V> createInput(
	// Class<? extends InputFormat<K, V>> inputFormat, String resource) {
	// final DataSourceContract<K, V> read = new DataSourceContract<K, V>(inputFormat, getResourcePath(resource),
	// "Input");
	// return read;
	// }
	//

	/**
	 * Converts a (String,Integer)-KeyValuePair into multiple KeyValuePairs. The
	 * key string is tokenized by spaces. For each token a new
	 * (String,Integer)-KeyValuePair is emitted where the Token is the key and
	 * an Integer(1) is the value.<br>
	 * Expected input: { line: "word1 word2 word1" }<br>
	 * Output: [{ word: "word1"}, { word: "word2"}, { word: "word1"}]
	 */
	@InputCardinality(1)
	public static class TokenizeLine extends ElementaryOperator<TokenizeLine> {
		/**
		 * 
		 */
		private static final long serialVersionUID = 8227589262294543806L;

		public static class Implementation extends SopremoMap {
			private static Pattern WORD_PATTERN = Pattern.compile("\\w+");

			/*
			 * (non-Javadoc)
			 * @see eu.stratosphere.sopremo.pact.SopremoMap#map(eu.stratosphere.sopremo.type.IJsonNode,
			 * eu.stratosphere.sopremo.pact.JsonCollector)
			 */
			@Override
			protected void map(final IJsonNode value, final JsonCollector out) {
				final Matcher matcher = WORD_PATTERN.matcher(((TextNode) ((IObjectNode) value).get("line"))
					.getJavaValue());
				while (matcher.find())
					out.collect(JsonUtil.createObjectNode("word", TextNode.valueOf(matcher.group())));
			}
		}
	}

	/**
	 * Counts the number of values for a given key. Hence, the number of
	 * occurences of a given token (word) is computed and emitted. The key is
	 * not modified, hence a SameKey OutputContract is attached to this class.<br>
	 * Expected input: [{ word: "word1"}, { word: "word1"}] <br>
	 * Output: [{ word: "word1", count: 2}]
	 */
	@InputCardinality(1)
	public static class CountWords extends ElementaryOperator<CountWords> {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1472064163594245033L;

		/**
		 * Initializes SopremoTestPlanTest.CountWords.
		 */
		public CountWords() {
			this.setKeyExpressions(0, new ObjectAccess("word"));
		}

		@Combinable
		public static class Implementation extends SopremoReduce {
			/*
			 * (non-Javadoc)
			 * @see eu.stratosphere.sopremo.pact.SopremoReduce#reduce(eu.stratosphere.sopremo.type.IArrayNode,
			 * eu.stratosphere.sopremo.pact.JsonCollector)
			 */
			@Override
			protected void reduce(final IArrayNode values, final JsonCollector out) {
				final Iterator<IJsonNode> valueIterator = values.iterator();
				final IObjectNode firstEntry = (IObjectNode) valueIterator.next();
				int sum = this.getCount(firstEntry);
				while (valueIterator.hasNext())
					sum += this.getCount((IObjectNode) valueIterator.next());
				out.collect(JsonUtil.createObjectNode("word", firstEntry.get("word"), "count", sum));
			}

			protected int getCount(final IObjectNode entry) {
				final IJsonNode countNode = entry.get("count");
				if (countNode.isMissing())
					return 1;
				return ((IntNode) countNode).getIntValue();
			}
		}
	}

	/**
	 * Tests if a {@link SopremoTestPlan} with two stubs can be executed.
	 */
	@Test
	public void wordCountPasses() {
		final TokenizeLine tokenize = new TokenizeLine();
		final CountWords countWords = new CountWords().withInputs(tokenize);

		final SopremoTestPlan testPlan = new SopremoTestPlan(countWords);
		final String[] lines =
		{
			"Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.",
			"Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.",
			"Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur.",
			"Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum."
		};
		for (final String line : lines)
			testPlan.getInput(0).add(JsonUtil.createObjectNode("line", TextNode.valueOf(line.toLowerCase())));

		final String[] singleWords = { "voluptate", "veniam", "velit", "ullamco", "tempor", "sunt", "sit", "sint",
			"sed",
			"reprehenderit", "quis", "qui", "proident", "pariatur", "officia", "occaecat", "nulla", "nostrud", "non",
			"nisi", "mollit", "minim", "magna", "lorem", "laborum", "laboris", "labore", "irure", "ipsum",
			"incididunt", "id", "fugiat", "exercitation", "excepteur", "ex", "eu", "et", "est", "esse", "enim", "elit",
			"eiusmod", "ea", "duis", "do", "deserunt", "cupidatat", "culpa", "consequat", "consectetur", "commodo",
			"cillum", "aute", "anim", "amet", "aliquip", "aliqua", "adipisicing", "ad" };
		for (final String singleWord : singleWords)
			testPlan.getExpectedOutput(0).add(JsonUtil.createObjectNode("word", singleWord, "count", 1));
		testPlan.getExpectedOutput(0).
			add(JsonUtil.createObjectNode("word", "ut", "count", 3)).
			add(JsonUtil.createObjectNode("word", "in", "count", 3)).
			add(JsonUtil.createObjectNode("word", "dolore", "count", 2)).
			add(JsonUtil.createObjectNode("word", "dolor", "count", 2));
		testPlan.run();
	}

	// /**
	// * Tests if a {@link SopremoTestPlan} fails if the actual values do not match the expected values.
	// */
	// @Test
	// public void shouldFailIfExpectedAndActualValuesDiffer() {
	// final MapContract<Key, Value, Key, Value> map = new MapContract<Key, Value, Key, Value>(IdentityMap.class,
	// "Map");
	// TestPlan testPlan = new TestPlan(map);
	// testPlan.getInput().
	// add(new PactInteger(1), new PactString("test1")).
	// add(new PactInteger(2), new PactString("test2"));
	// testPlan.getExpectedOutput().
	// add(new PactInteger(1), new PactString("test1")).
	// add(new PactInteger(2), new PactString("test3"));
	// assertTestRunFails(testPlan);
	// }
	//
	// /**
	// * Tests if a {@link SopremoTestPlan} fails there are too many values.
	// */
	// @Test
	// public void shouldFailIfTooManyValues() {
	// final MapContract<Key, Value, Key, Value> map = new MapContract<Key, Value, Key, Value>(IdentityMap.class,
	// "Map");
	// TestPlan testPlan = new TestPlan(map);
	// testPlan.getInput().
	// add(new PactInteger(1), new PactString("test1")).
	// add(new PactInteger(2), new PactString("test2"));
	// testPlan.getExpectedOutput().
	// add(new PactInteger(1), new PactString("test1"));
	// assertTestRunFails(testPlan);
	// }
	//
	// /**
	// * Tests if a {@link SopremoTestPlan} fails there are too few values.
	// */
	// @Test
	// public void shouldFailIfTooFewValues() {
	// final MapContract<Key, Value, Key, Value> map = new MapContract<Key, Value, Key, Value>(IdentityMap.class,
	// "Map");
	// TestPlan testPlan = new TestPlan(map);
	// testPlan.getInput().
	// add(new PactInteger(1), new PactString("test1")).
	// add(new PactInteger(2), new PactString("test2"));
	// testPlan.getExpectedOutput().
	// add(new PactInteger(1), new PactString("test1")).
	// add(new PactInteger(2), new PactString("test2")).
	// add(new PactInteger(3), new PactString("test3"));
	// assertTestRunFails(testPlan);
	// }
	//
	// /**
	// * Fails if the test plan does not fail.
	// *
	// * @param testPlan
	// * the test plan expected to fail
	// */
	// public static void assertTestRunFails(TestPlan testPlan) {
	// boolean failed = false;
	// try {
	// testPlan.run();
	// } catch (AssertionError error) {
	// failed = true;
	// }
	// assertTrue("Test plan should have failed", failed);
	// }
	//
	// /**
	// * Tests if a {@link SopremoTestPlan} succeeds with uninitialized expected values.
	// */
	// @Test
	// public void shouldSuceedIfNoExpectedValues() {
	// final MapContract<Key, Value, Key, Value> map = new MapContract<Key, Value, Key, Value>(IdentityMap.class,
	// "Map");
	// TestPlan testPlan = new TestPlan(map);
	// testPlan.getInput().
	// add(new PactInteger(1), new PactString("test1")).
	// add(new PactInteger(2), new PactString("test2"));
	// testPlan.run();
	// }
	//
	// /**
	// * Tests if a {@link SopremoTestPlan} succeeds with values having the same key.
	// */
	// @Test
	// public void shouldMatchValuesWithSameKey() {
	// final MapContract<Key, Value, Key, Value> map = new MapContract<Key, Value, Key, Value>(IdentityMap.class,
	// "Map");
	// TestPlan testPlan = new TestPlan(map);
	// testPlan.getInput().
	// add(new PactInteger(1), new PactString("test1")).
	// add(new PactInteger(1), new PactString("test2")).
	// add(new PactInteger(1), new PactString("test3")).
	// add(new PactInteger(2), new PactString("test1")).
	// add(new PactInteger(2), new PactString("test2")).
	// add(new PactInteger(2), new PactString("test3"));
	// // randomize values
	// testPlan.getExpectedOutput().
	// add(new PactInteger(1), new PactString("test1")).
	// add(new PactInteger(2), new PactString("test1")).
	// add(new PactInteger(2), new PactString("test2")).
	// add(new PactInteger(1), new PactString("test3")).
	// add(new PactInteger(1), new PactString("test2")).
	// add(new PactInteger(2), new PactString("test3"));
	// testPlan.run();
	// }
	//
	// /**
	// * Tests if a {@link SopremoTestPlan} succeeds with values having the same key.
	// */
	// @Test
	// public void shouldFailWithEqualValuesWithSameKey() {
	// final MapContract<Key, Value, Key, Value> map = new MapContract<Key, Value, Key, Value>(IdentityMap.class,
	// "Map");
	// TestPlan testPlan = new TestPlan(map);
	// testPlan.getInput().
	// add(new PactInteger(1), new PactString("test1")).
	// add(new PactInteger(1), new PactString("test2")).
	// add(new PactInteger(1), new PactString("test3")).
	// add(new PactInteger(2), new PactString("test1")).
	// add(new PactInteger(2), new PactString("test2")).
	// add(new PactInteger(2), new PactString("test3"));
	// // randomize values
	// testPlan.getExpectedOutput().
	// add(new PactInteger(1), new PactString("test1")).
	// add(new PactInteger(2), new PactString("test1")).
	// add(new PactInteger(2), new PactString("test2")).
	// add(new PactInteger(1), new PactString("test3")).
	// add(new PactInteger(1), new PactString("test3")). // <-- duplicate
	// add(new PactInteger(2), new PactString("test3"));
	// assertTestRunFails(testPlan);
	// }
	//
	// /**
	// * Tests if a {@link SopremoTestPlan} fails if actual values appear but empty values are expected.
	// */
	// @Test
	// public void shouldFailIfNonEmptyExpectedValues() {
	// final MapContract<Key, Value, Key, Value> map = new MapContract<Key, Value, Key, Value>(IdentityMap.class,
	// "Map");
	// TestPlan testPlan = new TestPlan(map);
	// testPlan.getInput().
	// add(new PactInteger(1), new PactString("test1")).
	// add(new PactInteger(2), new PactString("test2"));
	// testPlan.getExpectedOutput().setEmpty();
	// assertTestRunFails(testPlan);
	// }
}
