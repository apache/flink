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
import java.net.URL;
import java.util.Enumeration;
import java.util.Iterator;

import junit.framework.AssertionFailedError;
import nl.jqno.equalsverifier.EqualsVerifier;

import org.codehaus.jackson.JsonNode;
import org.junit.Test;
import org.junit.internal.ArrayComparisonFailure;

import eu.stratosphere.pact.common.contract.CrossContract;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.type.base.PactNull;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.testing.TestPairs;
import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.InputCardinality;
import eu.stratosphere.sopremo.JsonStream;
import eu.stratosphere.sopremo.JsonUtil;
import eu.stratosphere.sopremo.PersistenceType;
import eu.stratosphere.sopremo.Sink;
import eu.stratosphere.sopremo.SopremoTest;
import eu.stratosphere.sopremo.Source;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.PactJsonObject;
import eu.stratosphere.sopremo.pact.SopremoCross;
import eu.stratosphere.sopremo.pact.SopremoMap;

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
			add(createPactJsonValue("test1")).
			add(createPactJsonValue("test2"));
		testPlan.run();

		assertEquals("input and output should be equal in identity map", testPlan.getInput(0), testPlan
			.getActualOutput(0));

		// explicitly check output
		final Iterator<KeyValuePair<PactJsonObject.Key, PactJsonObject>> outputIterator = testPlan.getActualOutput(0)
			.iterator();
		final Iterator<KeyValuePair<PactJsonObject.Key, PactJsonObject>> inputIterator = testPlan.getInput(0)
			.iterator();
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
		final Source source = new Source(PersistenceType.HDFS, this.getResourcePath("SopremoTestPlan/test.json"));

		final Identity projection = new Identity();
		projection.setInputs(source);

		final Sink sink = new Sink(PersistenceType.HDFS, File.createTempFile(
			"output", null).toURI().toString());
		projection.setInputs(projection);

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
			withInputs(new Source(PersistenceType.HDFS, this.getResourcePath("SopremoTestPlan/test.json"))));

		testPlan.getExpectedOutput(0).setOperator(new Source(PersistenceType.HDFS,
			this.getResourcePath("SopremoTestPlan/test.json")));
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
			add(createPactJsonValue("test1")).
			add(createPactJsonValue("test2"));
		testPlan.getExpectedOutput(0).
			add(createPactJsonValue("test1")).
			add(createPactJsonValue("test2"));
		testPlan.run();
	}

	@Override
	protected void initVerifier(final EqualsVerifier<SopremoTestPlan> equalVerifier) {
		super.initVerifier(equalVerifier);
		equalVerifier
			.withPrefabValues(TestPairs.class,
				new TestPairs<Key, Value>().add(PactNull.getInstance(), new PactString("red")),
				new TestPairs<Key, Value>().add(PactNull.getInstance(), new PactString("black")))
			.withPrefabValues(SopremoTestPlan.ActualOutput.class,
				new SopremoTestPlan.ActualOutput(0).add(createPactJsonValue(0)),
				new SopremoTestPlan.ActualOutput(1).add(createPactJsonValue(1)))
			.withPrefabValues(SopremoTestPlan.ExpectedOutput.class,
				new SopremoTestPlan.ExpectedOutput(0).add(createPactJsonValue(0)),
				new SopremoTestPlan.ExpectedOutput(1).add(createPactJsonValue(1)))
			.withPrefabValues(SopremoTestPlan.Input.class,
				new SopremoTestPlan.Input(0).add(createPactJsonValue(0)),
				new SopremoTestPlan.Input(1).add(createPactJsonValue(1)));
	}

	/**
	 * Tests a {@link SopremoTestPlan} with a {@link CrossContract}.
	 */
	@Test
	public void settingValuesShouldWorkWithSourceContracts() {
		final CartesianProduct cartesianProduct = new CartesianProduct();
		final SopremoTestPlan testPlan = new SopremoTestPlan(cartesianProduct);
		testPlan.getInputForStream(cartesianProduct.getInput(0)).
			add(createPactJsonValue("test1")).
			add(createPactJsonValue("test2"));
		testPlan.getInputForStream(cartesianProduct.getInput(1)).
			add(createPactJsonValue("test3")).
			add(createPactJsonValue("test4"));
		testPlan.getExpectedOutputForStream(cartesianProduct.getOutput(0)).
			add(createPactJsonArray(null, null), createPactJsonArray("test1", "test3")).
			add(createPactJsonArray(null, null), createPactJsonArray("test1", "test4")).
			add(createPactJsonArray(null, null), createPactJsonArray("test2", "test3")).
			add(createPactJsonArray(null, null), createPactJsonArray("test2", "test4"));
		testPlan.run();
	}

	@InputCardinality(min = 2, max = 2)
	public static class CartesianProduct extends ElementaryOperator {
		/**
		 * 
		 */
		private static final long serialVersionUID = -4747731771822553359L;

		public static class Implementation
				extends
				SopremoCross<PactJsonObject.Key, PactJsonObject, PactJsonObject.Key, PactJsonObject, PactJsonObject.Key, PactJsonObject> {
			@Override
			protected void cross(final JsonNode key1, final JsonNode value1, final JsonNode key2,
					final JsonNode value2, final JsonCollector out) {
				out.collect(JsonUtil.asArray(key1, key2), JsonUtil.asArray(value1, value2));
			}
		}
	}

	public static class Identity extends ElementaryOperator {
		/**
		 * 
		 */
		private static final long serialVersionUID = 6764940997115103382L;

		public static class Implementation
				extends
				SopremoMap<PactJsonObject.Key, PactJsonObject, PactJsonObject.Key, PactJsonObject> {
			@Override
			protected void map(final JsonNode key, final JsonNode value, final JsonCollector out) {
				out.collect(key, value);
			}
		}
	}

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
	// /**
	// * Converts a (String,Integer)-KeyValuePair into multiple KeyValuePairs. The
	// * key string is tokenized by spaces. For each token a new
	// * (String,Integer)-KeyValuePair is emitted where the Token is the key and
	// * an Integer(1) is the value.
	// */
	// public static class TokenizeLine extends MapStub<PactString, PactInteger, PactString, PactInteger> {
	// private static Pattern WORD_PATTERN = Pattern.compile("\\w+");
	//
	// @Override
	// public void map(PactString key, PactInteger value, Collector<PactString, PactInteger> out) {
	// Matcher matcher = WORD_PATTERN.matcher(key.getValue());
	// while (matcher.find())
	// out.collect(new PactString(matcher.group().toLowerCase()), new PactInteger(1));
	// }
	// }
	//
	// /**
	// * Counts the number of values for a given key. Hence, the number of
	// * occurences of a given token (word) is computed and emitted. The key is
	// * not modified, hence a SameKey OutputContract is attached to this class.
	// */
	// @SameKey
	// @Combinable
	// public static class CountWords extends ReduceStub<PactString, PactInteger, PactString, PactInteger> {
	// @Override
	// public void reduce(PactString key, Iterator<PactInteger> values, Collector<PactString, PactInteger> out) {
	// int sum = 0;
	// while (values.hasNext())
	// sum += values.next().getValue();
	// out.collect(key, new PactInteger(sum));
	// }
	//
	// @Override
	// public void combine(PactString key, Iterator<PactInteger> values, Collector<PactString, PactInteger> out) {
	// this.reduce(key, values, out);
	// }
	// }
	//
	// /**
	// * Tests if a {@link SopremoTestPlan} with two stubs can be executed.
	// */
	// @Test
	// public void complexTestPassesWithExpectedValues() {
	// final MapContract<PactString, PactInteger, PactString, PactInteger> tokenize = new MapContract<PactString,
	// PactInteger, PactString, PactInteger>(
	// TokenizeLine.class,
	// "Map");
	// final ReduceContract<PactString, PactInteger, PactString, PactInteger> summing = new ReduceContract<PactString,
	// PactInteger, PactString, PactInteger>(
	// CountWords.class,
	// "Map");
	// summing.setInput(tokenize);
	//
	// TestPlan testPlan = new TestPlan(summing);
	// String[] lines = {
	// "Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.",
	// "Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.",
	// "Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur.",
	// "Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum."
	// };
	// for (String line : lines)
	// testPlan.getInput().add(new PactString(line), new PactInteger(1));
	//
	// String[] singleWords = { "voluptate", "veniam", "velit", "ullamco", "tempor", "sunt", "sit", "sint", "sed",
	// "reprehenderit", "quis", "qui", "proident", "pariatur", "officia", "occaecat", "nulla", "nostrud", "non",
	// "nisi", "mollit", "minim", "magna", "lorem", "laborum", "laboris", "labore", "irure", "ipsum",
	// "incididunt", "id", "fugiat", "exercitation", "excepteur", "ex", "eu", "et", "est", "esse", "enim", "elit",
	// "eiusmod", "ea", "duis", "do", "deserunt", "cupidatat", "culpa", "consequat", "consectetur", "commodo",
	// "cillum", "aute", "anim", "amet", "aliquip", "aliqua", "adipisicing", "ad" };
	// for (String singleWord : singleWords)
	// testPlan.getExpectedOutput().add(new PactString(singleWord), new PactInteger(1));
	// testPlan.getExpectedOutput().add(new PactString("ut"), new PactInteger(3)).
	// add(new PactString("in"), new PactInteger(3)).
	// add(new PactString("dolore"), new PactInteger(2)).
	// add(new PactString("dolor"), new PactInteger(2));
	// testPlan.run();
	// }
	//
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
