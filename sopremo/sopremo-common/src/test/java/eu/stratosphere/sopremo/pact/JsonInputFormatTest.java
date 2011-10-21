package eu.stratosphere.sopremo.pact;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

import junit.framework.Assert;

import org.junit.Test;

import eu.stratosphere.pact.common.IdentityMap;
import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.contract.FileDataSinkContract;
import eu.stratosphere.pact.common.contract.FileDataSourceContract;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.io.FileOutputFormat;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.testing.TestPlan;
import eu.stratosphere.pact.testing.ioformats.FormatUtil;
import eu.stratosphere.pact.testing.ioformats.SequentialOutputFormat;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.IntNode;
import eu.stratosphere.sopremo.type.JsonNode;
import eu.stratosphere.sopremo.type.ObjectNode;

/**
 * Tests {@link JsonInputFormat}.
 * 
 * @author Arvid Heise
 */
public class JsonInputFormatTest {
	/**
	 * Tests if a {@link TestPlan} can be executed.
	 * 
	 * @throws IOException
	 */
	@Test
	public void completeTestPasses() throws IOException {
		final FileDataSourceContract<JsonNode, JsonNode> read = new FileDataSourceContract<JsonNode, JsonNode>(
			JsonInputFormat.class, this.getResource("SopremoTestPlan/test.json"), "Input");

		final MapContract<Key, Value, Key, Value> map =
			new MapContract<Key, Value, Key, Value>(IdentityMap.class, "Map");
		map.setInput(read);

		final FileDataSinkContract<Key, Value> output = this.createOutput(map, SequentialOutputFormat.class);

		final TestPlan testPlan = new TestPlan(output);
		testPlan.run();
		Assert.assertEquals("input and output should be equal in identity map", testPlan.getInput(), testPlan
			.getActualOutput());
	}

	/**
	 * Tests if a {@link TestPlan} can be executed.
	 * 
	 * @throws IOException
	 */
	@Test
	public void completeTestPassesWithExpectedValues() throws IOException {
		final FileDataSourceContract<JsonNode, JsonNode> read = new FileDataSourceContract<JsonNode, JsonNode>(
			JsonInputFormat.class, this.getResource("SopremoTestPlan/test.json"), "Input");

		final MapContract<Key, Value, Key, Value> map = new MapContract<Key, Value, Key, Value>(IdentityMap.class,
			"Map");
		map.setInput(read);

		final FileDataSinkContract<JsonNode, JsonNode> output = this.createOutput(map,
			JsonOutputFormat.class);

		final TestPlan testPlan = new TestPlan(output);
		testPlan.getExpectedOutput(output).fromFile(JsonInputFormat.class,
			this.getResource("SopremoTestPlan/test.json"));
		testPlan.run();
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
			final FileDataSinkContract<K, V> out = new FileDataSinkContract<K, V>(outputFormatClass, File
				.createTempFile(
					"output", null).toURI().toString(), "Output");
			out.setInput(input);
			return out;
		} catch (final IOException e) {
			Assert.fail("cannot create temporary output file" + e);
			return null;
		}
	}

	private String getResource(final String name) throws IOException {
		return JsonInputFormatTest.class.getClassLoader().getResources(name)
			.nextElement().toString();
	}

	/**
	 * @throws IOException
	 */
	@Test
	public void shouldProperlyReadArray() throws IOException {
		final File file = File.createTempFile("jsonInputFormatTest", null);
		file.delete();
		final OutputStreamWriter jsonWriter = new OutputStreamWriter(new FileOutputStream(file));
		jsonWriter.write("[{\"id\": 1}, {\"id\": 2}, {\"id\": 3}, {\"id\": 4}, {\"id\": 5}]");
		jsonWriter.close();

		final JsonInputFormat inputFormat = FormatUtil.createInputFormat(JsonInputFormat.class, file.toURI()
			.toString(), null);
		final KeyValuePair<JsonNode, JsonNode> pair = inputFormat.createPair();
		for (int index = 1; index <= 5; index++) {
			Assert.assertFalse("more pairs expected @ " + index, inputFormat.reachedEnd());
			Assert.assertTrue("valid pair expected @ " + index, inputFormat.nextRecord(pair));
			Assert.assertEquals("other order expected", index,
				((IntNode) ((ObjectNode) SopremoUtil.unwrap(pair.getValue())).get("id")).getIntValue());
		}

		if (!inputFormat.reachedEnd()) {
			Assert.assertTrue("no more pairs but reachedEnd did not return false", inputFormat.nextRecord(pair));
			Assert.fail("pair unexpected: " + pair);
		}
	}

	/**
	 * @throws IOException
	 */
	@Test
	public void shouldProperlyReadSingleValue() throws IOException {
		final File file = File.createTempFile("jsonInputFormatTest", null);
		file.delete();
		final OutputStreamWriter jsonWriter = new OutputStreamWriter(new FileOutputStream(file));
		jsonWriter.write("{\"array\": [{\"id\": 1}, {\"id\": 2}, {\"id\": 3}, {\"id\": 4}, {\"id\": 5}]}");
		jsonWriter.close();

		final JsonInputFormat inputFormat = FormatUtil.createInputFormat(JsonInputFormat.class, file.toURI()
			.toString(), null);
		final KeyValuePair<JsonNode, JsonNode> pair = inputFormat.createPair();

		if (!inputFormat.reachedEnd())
			if (!inputFormat.nextRecord(pair))
				Assert.fail("one value expected expected: " + pair);

		if (!inputFormat.reachedEnd()) {
			Assert.assertTrue("no more values but reachedEnd did not return false", inputFormat.nextRecord(pair));
			Assert.fail("value unexpected: " + pair);
		}

		final JsonNode arrayNode = ((ObjectNode) SopremoUtil.unwrap(pair.getValue())).get("array");
		Assert.assertNotNull("could not find top level node", arrayNode);
		for (int index = 1; index <= 5; index++) {
			Assert.assertNotNull("could not find array element " + index, ((ArrayNode) arrayNode).get(index - 1));
			Assert.assertEquals("other order expected", index,
				((IntNode) ((ObjectNode) ((ArrayNode) arrayNode).get(index - 1)).get("id")).getIntValue());
		}
	}
}
