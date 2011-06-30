package eu.stratosphere.sopremo.pact;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

import junit.framework.Assert;

import org.codehaus.jackson.JsonNode;
import org.junit.Test;

import eu.stratosphere.pact.common.IdentityMap;
import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.contract.DataSinkContract;
import eu.stratosphere.pact.common.contract.DataSourceContract;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.io.OutputFormat;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.testing.TestPlan;
import eu.stratosphere.pact.testing.ioformats.FormatUtil;
import eu.stratosphere.pact.testing.ioformats.SequentialOutputFormat;

/**
 * Tests {@link JsonInputFormat}.
 * 
 * @author Arvid Heise
 */
public class JsonInputFormatTest {
	/**
	 * @throws IOException
	 */
	@Test
	public void shouldProperlyReadArray() throws IOException {
		File file = File.createTempFile("jsonInputFormatTest", null);
		file.delete();
		OutputStreamWriter jsonWriter = new OutputStreamWriter(new FileOutputStream(file));
		jsonWriter.write("[{\"id\": 1}, {\"id\": 2}, {\"id\": 3}, {\"id\": 4}, {\"id\": 5}]");
		jsonWriter.close();

		JsonInputFormat inputFormat = FormatUtil.createInputFormat(JsonInputFormat.class, file.toURI()
			.toString(), null);
		KeyValuePair<PactJsonObject.Key, PactJsonObject> pair = inputFormat.createPair();
		for (int index = 1; index <= 5; index++) {
			Assert.assertFalse("more pairs expected @ " + index, inputFormat.reachedEnd());
			Assert.assertTrue("valid pair expected @ " + index, inputFormat.nextPair(pair));
			Assert
				.assertEquals("other order expected", index, pair.getValue().getValue().get("id").getIntValue());
		}

		if (!inputFormat.reachedEnd()) {
			Assert.assertTrue("no more pairs but reachedEnd did not return false", inputFormat.nextPair(pair));
			Assert.fail("pair unexpected: " + pair);
		}
	}

	/**
	 * @throws IOException
	 */
	@Test
	public void shouldProperlyReadSingleValue() throws IOException {
		File file = File.createTempFile("jsonInputFormatTest", null);
		file.delete();
		OutputStreamWriter jsonWriter = new OutputStreamWriter(new FileOutputStream(file));
		jsonWriter.write("{\"array\": [{\"id\": 1}, {\"id\": 2}, {\"id\": 3}, {\"id\": 4}, {\"id\": 5}]}");
		jsonWriter.close();

		JsonInputFormat inputFormat = FormatUtil.createInputFormat(JsonInputFormat.class, file.toURI()
			.toString(), null);
		KeyValuePair<PactJsonObject.Key, PactJsonObject> pair = inputFormat.createPair();

		if (!inputFormat.reachedEnd()) {
			if (!inputFormat.nextPair(pair))
				Assert.fail("one value expected expected: " + pair);
		}

		if (!inputFormat.reachedEnd()) {
			Assert.assertTrue("no more values but reachedEnd did not return false", inputFormat.nextPair(pair));
			Assert.fail("value unexpected: " + pair);
		}

		JsonNode arrayNode = pair.getValue().getValue().get("array");
		Assert.assertNotNull("could not find top level node", arrayNode);
		for (int index = 1; index <= 5; index++) {
			Assert.assertNotNull("could not find array element " + index, arrayNode.get(index - 1));
			Assert.assertEquals("other order expected", index, arrayNode.get(index - 1).get("id").getIntValue());
		}
	}

	/**
	 * Tests if a {@link TestPlan} can be executed.
	 * 
	 * @throws IOException
	 */
	@Test
	public void completeTestPasses() throws IOException {
		final DataSourceContract<PactJsonObject.Key, PactJsonObject> read = new DataSourceContract<PactJsonObject.Key, PactJsonObject>(
			JsonInputFormat.class, getResource("SopremoTestPlan/test.json"), "Input");

		final MapContract<Key, Value, Key, Value> map =
			new MapContract<Key, Value, Key, Value>(IdentityMap.class, "Map");
		map.setInput(read);

		DataSinkContract<Key, Value> output = createOutput(map, SequentialOutputFormat.class);

		TestPlan testPlan = new TestPlan(output);
		testPlan.run();
		Assert.assertEquals("input and output should be equal in identity map", testPlan.getInput(), testPlan
			.getActualOutput());
	}

	/**
	 * Creates an output file in the temporary folder for arbitrary key/value pairs coming from the given input
	 * contract.
	 * 
	 * @param input
	 *        the input from which the values are read
	 * @param outputFormatClass
	 *        the output format
	 * @return the {@link DataSinkContract} for the temporary file
	 */
	private <K extends Key, V extends Value> DataSinkContract<K, V> createOutput(final Contract input,
			final Class<? extends OutputFormat<K, V>> outputFormatClass) {
		try {
			final DataSinkContract<K, V> out = new DataSinkContract<K, V>(outputFormatClass, File.createTempFile(
				"output", null).toURI().toString(), "Output");
			out.setInput(input);
			return out;
		} catch (IOException e) {
			Assert.fail("cannot create temporary output file" + e);
			return null;
		}
	}

	/**
	 * Tests if a {@link TestPlan} can be executed.
	 * 
	 * @throws IOException
	 */
	@Test
	public void completeTestPassesWithExpectedValues() throws IOException {
		final DataSourceContract<PactJsonObject.Key, PactJsonObject> read = new DataSourceContract<PactJsonObject.Key, PactJsonObject>(
			JsonInputFormat.class, getResource("SopremoTestPlan/test.json"), "Input");

		final MapContract<Key, Value, Key, Value> map = new MapContract<Key, Value, Key, Value>(IdentityMap.class,
			"Map");
		map.setInput(read);

		DataSinkContract<PactJsonObject.Key, PactJsonObject> output = createOutput(map, JsonOutputFormat.class);

		TestPlan testPlan = new TestPlan(output);
		testPlan.getExpectedOutput(output).fromFile(JsonInputFormat.class, getResource("SopremoTestPlan/test.json"));
		testPlan.run();
	}

	private String getResource(String name) throws IOException {
		return JsonInputFormatTest.class.getClassLoader().getResources(name)
			.nextElement().toString();
	}
}
