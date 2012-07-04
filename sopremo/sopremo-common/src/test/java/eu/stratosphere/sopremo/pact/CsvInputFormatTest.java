package eu.stratosphere.sopremo.pact;

import java.io.File;
import java.io.IOException;

import junit.framework.Assert;

import org.junit.Ignore;
import org.junit.Test;

import eu.stratosphere.pact.common.IdentityMap;
import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.io.FileOutputFormat;
import eu.stratosphere.pact.common.io.SequentialOutputFormat;
import eu.stratosphere.pact.testing.TestPlan;
import eu.stratosphere.sopremo.serialization.DirectSchema;

@Ignore
public class CsvInputFormatTest {
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
	public void completeTestPassesWithExpectedValues() throws IOException {
		final FileDataSource read = new FileDataSource(
			CsvInputFormat.class, this.getResource("SopremoTestPlan/restaurant_short.csv"), "Input");

		final MapContract map = new MapContract(IdentityMap.class, "Map");
		map.setInput(read);

		final FileDataSink output = this.createOutput(map, SequentialOutputFormat.class);

		final TestPlan testPlan = new TestPlan(output); // write
		testPlan.getExpectedOutput(output, SCHEMA.getPactSchema()).fromFile(JsonInputFormat.class,// write
			this.getResource("SopremoTestPlan/restaurant_short.json"));
		testPlan.run();
	}

	private String getResource(final String name) throws IOException {
		return JsonInputFormatTest.class.getClassLoader().getResources(name)
			.nextElement().toString();
	}

	private FileDataSink createOutput(final Contract input, final Class<? extends FileOutputFormat> outputFormatClass) {
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
}
