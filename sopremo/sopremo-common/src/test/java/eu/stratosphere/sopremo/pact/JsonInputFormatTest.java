package eu.stratosphere.sopremo.pact;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

import junit.framework.Assert;

import org.junit.Test;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.IdentityMap;
import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.io.FileOutputFormat;
import eu.stratosphere.pact.common.io.FormatUtil;
import eu.stratosphere.pact.common.io.SequentialOutputFormat;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.testing.TestPlan;
import eu.stratosphere.pact.testing.TestRecords;
import eu.stratosphere.sopremo.serialization.DirectSchema;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IObjectNode;
import eu.stratosphere.sopremo.type.IntNode;

/**
 * Tests {@link JsonInputFormat}.
 * 
 * @author Arvid Heise
 */
public class JsonInputFormatTest {
	/**
	 * 
	 */
	private static final DirectSchema SCHEMA = new DirectSchema();

	/**
	 * Tests if a {@link TestPlan} can be executed.
	 * 
	 * @throws IOException
	 */
	@Test
	public void completeTestPasses() throws IOException {
		final FileDataSource read = new FileDataSource(
			JsonInputFormat.class, this.getResource("SopremoTestPlan/test.json"), "Input");
		SopremoUtil.serialize(read.getParameters(), IOConstants.SCHEMA, SCHEMA);

		final MapContract map =
			new MapContract(IdentityMap.class, "Map");
		map.setInput(read);

		final FileDataSink output = this.createOutput(map, SequentialOutputFormat.class);
		SopremoUtil.serialize(output.getParameters(), IOConstants.SCHEMA, SCHEMA);

		final TestPlan testPlan = new TestPlan(output);
		testPlan.run();
		final TestRecords input = testPlan.getInput();
		input.setSchema(SCHEMA.getPactSchema());
		Assert.assertEquals("input and output should be equal in identity map", input, testPlan
			.getActualOutput());
	}

	/**
	 * Tests if a {@link TestPlan} can be executed.
	 * 
	 * @throws IOException
	 */
	@Test
	public void completeTestPassesWithExpectedValues() throws IOException {
		final FileDataSource read = new FileDataSource(
			JsonInputFormat.class, this.getResource("SopremoTestPlan/test.json"), "Input");
		SopremoUtil.serialize(read.getParameters(), IOConstants.SCHEMA, SCHEMA);

		final MapContract map = new MapContract(IdentityMap.class, "Map");
		map.setInput(read);

		final FileDataSink output = this.createOutput(map,
			JsonOutputFormat.class);
		SopremoUtil.serialize(output.getParameters(), IOConstants.SCHEMA, SCHEMA);

		final TestPlan testPlan = new TestPlan(output);
		testPlan.getExpectedOutput(output, SCHEMA.getPactSchema()).fromFile(JsonInputFormat.class,
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
	 * @return the {@link FileDataSink} for the temporary file
	 */
	private <K extends Key, V extends Value> FileDataSink createOutput(final Contract input,
			final Class<? extends FileOutputFormat> outputFormatClass) {
		try {
			final FileDataSink out = new FileDataSink(outputFormatClass,
				File.createTempFile("output", null).toURI().toString(), "Output");
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

		Configuration config = new Configuration();
		SopremoUtil.serialize(config, IOConstants.SCHEMA, SCHEMA);
		final JsonInputFormat inputFormat = FormatUtil.openInput(JsonInputFormat.class, file.toURI()
			.toString(), config);
		final PactRecord record = new PactRecord();
		for (int index = 1; index <= 5; index++) {
			Assert.assertFalse("more pairs expected @ " + index, inputFormat.reachedEnd());
			Assert.assertTrue("valid pair expected @ " + index, inputFormat.nextRecord(record));
			Assert.assertEquals("other order expected", index,
				((IntNode) ((IObjectNode) SCHEMA.recordToJson(record, null)).get("id")).getIntValue());
		}

		if (!inputFormat.reachedEnd()) {
			Assert.assertTrue("no more pairs but reachedEnd did not return false", inputFormat.nextRecord(record));
			Assert.fail("pair unexpected: " + SCHEMA.recordToJson(record, null));
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

		Configuration config = new Configuration();
		SopremoUtil.serialize(config, IOConstants.SCHEMA, SCHEMA);
		final JsonInputFormat inputFormat = FormatUtil.openInput(JsonInputFormat.class, file.toURI()
			.toString(), config);
		final PactRecord record = new PactRecord();

		if (!inputFormat.reachedEnd())
			if (!inputFormat.nextRecord(record))
				Assert.fail("one value expected expected: " + SCHEMA.recordToJson(record, null));

		if (!inputFormat.reachedEnd()) {
			Assert.assertTrue("no more values but reachedEnd did not return false", inputFormat.nextRecord(record));
			Assert.fail("value unexpected: " + SCHEMA.recordToJson(record, null));
		}

		final IJsonNode arrayNode = ((IObjectNode) SCHEMA.recordToJson(record, null)).get("array");
		Assert.assertNotNull("could not find top level node", arrayNode);
		for (int index = 1; index <= 5; index++) {
			Assert.assertNotNull("could not find array element " + index, ((IArrayNode) arrayNode).get(index - 1));
			Assert.assertEquals("other order expected", index,
				((IntNode) ((IObjectNode) ((IArrayNode) arrayNode).get(index - 1)).get("id")).getIntValue());
		}
	}
}
