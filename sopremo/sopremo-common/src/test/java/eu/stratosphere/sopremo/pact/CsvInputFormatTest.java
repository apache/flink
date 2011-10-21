package eu.stratosphere.sopremo.pact;

import java.io.File;
import java.io.IOException;

import junit.framework.Assert;

import org.junit.Ignore;
import org.junit.Test;

import eu.stratosphere.pact.common.IdentityMap;
import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.contract.FileDataSinkContract;
import eu.stratosphere.pact.common.contract.FileDataSourceContract;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.io.FileOutputFormat;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.testing.TestPlan;
import eu.stratosphere.pact.testing.ioformats.SequentialOutputFormat;
import eu.stratosphere.sopremo.type.JsonNode;

@Ignore
public class CsvInputFormatTest {
	/**
	 * Tests if a {@link TestPlan} can be executed.
	 * 
	 * @throws IOException
	 */
	@Test
	public void completeTestPassesWithExpectedValues() throws IOException {
		final FileDataSourceContract<JsonNode, JsonNode> read = new FileDataSourceContract<JsonNode, JsonNode>(
			CsvInputFormat.class, this.getResource("SopremoTestPlan/restaurant_short.csv"), "Input");

		final MapContract<Key, Value, Key, Value> map = new MapContract<Key, Value, Key, Value>(IdentityMap.class,
			"Map");
		map.setInput(read);

		final FileDataSinkContract<Key, Value> output = this.createOutput(map,
			SequentialOutputFormat.class);

		final TestPlan testPlan = new TestPlan(output); // write
		testPlan.getExpectedOutput(output).fromFile(JsonInputFormat.class,// write
			this.getResource("SopremoTestPlan/restaurant_short.json"));
		testPlan.run();
	}

	private String getResource(final String name) throws IOException {
		return JsonInputFormatTest.class.getClassLoader().getResources(name)
			.nextElement().toString();
	}

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
}
