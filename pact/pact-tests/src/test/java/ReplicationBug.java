import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.pact.common.contract.DataSinkContract;
import eu.stratosphere.pact.common.contract.DataSourceContract;
import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.io.TextOutputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.compiler.PactCompiler;
import eu.stratosphere.pact.compiler.jobgen.JobGraphGenerator;
import eu.stratosphere.pact.compiler.plan.OptimizedPlan;
import eu.stratosphere.pact.test.util.TestBase;

//public class ReplicationBug {
//	@SameKey
//	public static class IdentityMap extends MapStub<Key, Value, Key, Value> {
//		@Override
//		protected void map(Key key, Value value, Collector<Key, Value> out) {
//			out.collect(key, value);
//		}
//	}
//
//	public static class StringTextInputFormat extends TextInputFormat<PactString, PactString> {
//		@Override
//		public boolean readLine(KeyValuePair<PactString, PactString> pair, byte[] record) {
//			String[] input = new String(record).split(";");
//			if (input.length < 2)
//				return false;
//			pair.getKey().setValue(input[0]);
//			pair.getValue().setValue(input[1]);
//			return true;
//		}
//	}
//
//	public static class StringTextOutputFormat extends TextOutputFormat<PactString, PactString> {
//		@Override
//		public byte[] writeLine(KeyValuePair<PactString, PactString> pair) {
//			return (pair.getKey() + ";" + pair.getValue()).getBytes();
//		}
//	}
//
//	public void failsForReplication() {
//		File input = File.createTempFile("input", null);
//		FileWriter inputWriter = new FileWriter(input);
//		inputWriter.write("key;value\n");
//		inputWriter.close();
//
//		DataSourceContract<PactString, PactString> source = new DataSourceContract<PactString, PactString>(
//			StringTextInputFormat.class, input.toURI().toString());
//		source.setDegreeOfParallelism(2);
//
//		File output = File.createTempFile("output", null);
//		output.delete();
//		DataSinkContract<PactString, PactString> sink = new DataSinkContract<PactString, PactString>(
//			StringTextOutputFormat.class, output.toURI().toString());
//		sink.setDegreeOfParallelism(1);
//		sink.setInput(source);
//
//		final Plan plan = new Plan(sink);
//
//		final OptimizedPlan optimizedPlan = new PactCompiler().compile(plan);
//		final JobGraph jobGraph = new JobGraphGenerator().compileJobGraph(optimizedPlan);
//		LibraryCacheManager.register(jobGraph.getJobID(), new String[0]);
//		final ExecutionGraph eg = new ExecutionGraph(jobGraph,			this.instanceManager);
//
//		BufferedReader outputReader = new BufferedReader(new FileReader(output));
//		Assert.assertEquals("key;value", outputReader.readLine());
//		Assert.assertEquals("key;value", outputReader.readLine());
//		Assert.assertEquals("", outputReader.readLine());
//	}
//}

@RunWith(Parameterized.class)
public class ReplicationBug extends TestBase

{
	private static final Log LOG = LogFactory.getLog(ReplicationBug.class);

	public ReplicationBug(String clusterConfig, Configuration testConfig) {
		super(testConfig, clusterConfig);
	}

	private static final String MAP_IN_1 = "1 1\n2 2\n2 8\n4 4\n4 4\n6 6\n7 7\n8 8\n";

	@Override
	protected void preSubmit() throws Exception {
		String tempDir = getFilesystemProvider().getTempDirPath();

		getFilesystemProvider().createDir(tempDir + "/mapInput");

		getFilesystemProvider().createFile(tempDir + "/mapInput/mapTest_1.txt", MAP_IN_1);
	}

	public static class TestInFormat extends TextInputFormat<PactString, PactString> {

		@Override
		public boolean readLine(KeyValuePair<PactString, PactString> pair, byte[] line) {

			pair.setKey(new PactString(new String((char) line[0] + "")));
			pair.setValue(new PactString(new String((char) line[2] + "")));

			LOG.debug("Read in: [" + pair.getKey() + "," + pair.getValue() + "]");

			return true;
		}

	}

	public static class TestOutFormat extends TextOutputFormat<PactString, PactString> {

		@Override
		public byte[] writeLine(KeyValuePair<PactString, PactString> pair) {
			LOG.debug("Writing out: [" + pair.getKey() + "," + pair.getValue() + "]");

			return (pair.getKey().toString() + " " + pair.getValue().toString() + "\n").getBytes();
		}
	}

	@Override
	protected JobGraph getJobGraph() throws Exception {
		String pathPrefix = getFilesystemProvider().getURIPrefix() + getFilesystemProvider().getTempDirPath();

		DataSourceContract<PactString, PactString> input = new DataSourceContract<PactString, PactString>(
			TestInFormat.class, pathPrefix + "/mapInput");
		input.setFormatParameter("delimiter", "\n");
		input.setDegreeOfParallelism(2);
		
		DataSinkContract<PactString, PactString> output = new DataSinkContract<PactString, PactString>(
			TestOutFormat.class, pathPrefix + "/result.txt");
		output.setDegreeOfParallelism(1);

		output.setInput(input);

		Plan plan = new Plan(output);

		PactCompiler pc = new PactCompiler();
		OptimizedPlan op = pc.compile(plan);

		JobGraphGenerator jgg = new JobGraphGenerator();
		return jgg.compileJobGraph(op);
	}

	@Override
	protected void postSubmit() throws Exception {
		String tempDir = getFilesystemProvider().getTempDirPath();

		compareResultsByLinesInMemory(MAP_IN_1, tempDir + "/result.txt");

		getFilesystemProvider().delete(tempDir + "/result.txt", true);
		getFilesystemProvider().delete(tempDir + "/mapInput", true);

	}

	@Parameters
	public static Collection<Object[]> getConfigurations() throws FileNotFoundException, IOException {
		LinkedList<Configuration> testConfigs = new LinkedList<Configuration>();

		Configuration config = new Configuration();
//		config.setInteger("MapTest#NoSubtasks", 4);
		testConfigs.add(config);

		return toParameterList(ReplicationBug.class, testConfigs);
	}
}
