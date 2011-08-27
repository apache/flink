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
import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.testing.TestPlan;
import eu.stratosphere.pact.testing.ioformats.SequentialOutputFormat;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.PersistenceType;
import eu.stratosphere.sopremo.Sink;
import eu.stratosphere.sopremo.Source;
import eu.stratosphere.sopremo.base.Projection;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.expressions.ObjectCreation;
import eu.stratosphere.sopremo.pact.CsvInputFormat;
import eu.stratosphere.sopremo.pact.JsonInputFormat;
import eu.stratosphere.sopremo.pact.JsonInputFormatTest;
import eu.stratosphere.sopremo.pact.JsonOutputFormat;
import eu.stratosphere.sopremo.pact.PactJsonObject;
import eu.stratosphere.sopremo.testing.SopremoTestPlan;

public class CsvInputFormatTest {
	/**
	 * Tests if a {@link TestPlan} can be executed.
	 * 
	 * @throws IOException
	 */
	@Test
	public void completeTestPassesWithExpectedValues() throws IOException {
		final FileDataSourceContract<PactJsonObject.Key, PactJsonObject> read = new FileDataSourceContract<PactJsonObject.Key, PactJsonObject>(
			CsvInputFormat.class, new File("/home/arv/workflow/input/2.csv").getAbsoluteFile().toURI().toString(), "Input");

		 //testing a bigger csv to watch the output
		 final FileDataSinkContract<PactJsonObject.Key, PactJsonObject> write = new
		 FileDataSinkContract<PactJsonObject.Key, PactJsonObject>(
		 JsonOutputFormat.class, new File("/home/arv/workflow/output/2011_08_25_12_01C.json").getAbsoluteFile().toURI().toString(), read, "Output");

		final MapContract<Key, Value, Key, Value> map = new MapContract<Key, Value, Key, Value>(IdentityMap.class,
			"Map");
		map.setInput(read);

		final FileDataSinkContract<Key, Value> output = this.createOutput(map,
			SequentialOutputFormat.class);

		final TestPlan testPlan = new TestPlan(write); // write
		testPlan.getExpectedOutput(write).fromFile(JsonInputFormat.class,// write
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
	
//	/**
//	 * Tests if a {@link TestPlan} can be executed.
//	 * 
//	 * @throws IOException
//	 */
//	@Test
//	public void completeTestPassesWithExpectedValues() throws IOException {
//		final Source read = new Source(CsvInputFormat.class, "file://localhost:80/home/arv/workflow/input/2011_08_25_12_01C.csv");
//
//		Projection spending = new Projection(creation, null);
//
//		// testing a bigger csv to watch the output
//		final Sink write = new Sink(PersistenceType.HDFS, new File("/home/arv/workflow/output/UsSpending.json").toURI().toString(), spending);
//
//		final SopremoTestPlan testPlan = new SopremoTestPlan(write);
//		testPlan.getInput(0).
//		testPlan.run();
//	}

}
