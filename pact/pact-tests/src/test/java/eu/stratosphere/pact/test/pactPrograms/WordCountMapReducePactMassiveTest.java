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

package eu.stratosphere.pact.test.pactPrograms;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;

import org.junit.Assert;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.pact.common.contract.DataSinkContract;
import eu.stratosphere.pact.common.contract.DataSourceContract;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.io.TextOutputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MapStub;
import eu.stratosphere.pact.common.stub.ReduceStub;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.compiler.PactCompiler;
import eu.stratosphere.pact.compiler.jobgen.JobGraphGenerator;
import eu.stratosphere.pact.compiler.plan.OptimizedPlan;
import eu.stratosphere.pact.test.pactPrograms.WordCount.Integer;
import eu.stratosphere.pact.test.pactPrograms.WordCount.Text;
import eu.stratosphere.pact.test.util.TestBase;

/**
 * @author Erik Nijkamp
 */
@RunWith(Parameterized.class)
public class WordCountMapReducePactMassiveTest extends TestBase {

	private static final String TEST_FILE_IN = "words.txt";

	private static final String TEST_FILE_OUT = "result";

	private static final String[] TEST_DATA_STR = { "1234", "444" };

	private static final int[] TEST_DATA_NUM = { 1234, 444 };

	public WordCountMapReducePactMassiveTest(Configuration config) {
		super(config);
		// TODO Auto-generated constructor stub
	}

	public static class TextFormatIn extends TextInputFormat<Text, Text> {
		@Override
		public boolean readLine(KeyValuePair<Text, Text> text, byte[] line) {
			StringBuffer buffer = new StringBuffer();
			for (int i = 0; i < line.length; i++) {
				buffer.append((char) line[i]);
			}
			text.getValue().setValue(buffer.toString());
			return true;
		}

		@Override
		public KeyValuePair<Text, Text> createPair() {
			return new KeyValuePair<Text, Text>(new Text(), new Text());
		}
	}

	public static class TextFormatOut extends TextOutputFormat<Text, Integer> {
		@Override
		public byte[] writeLine(KeyValuePair<Text, Integer> pair) {
			return (pair.getKey().toString() + ":" + pair.getValue().toString() + "\n").getBytes();
		}

		@Override
		public KeyValuePair<Text, Integer> createPair() {
			return new KeyValuePair<Text, Integer>(new Text(), new Integer());
		}
	}

	public static class Mapper extends MapStub<Text, Text, Text, Integer> {
		protected void map(Text key, Text value, Collector<Text, Integer> out) {
			out.collect(value, new Integer(1));
		}
	}

	public static class Reducer extends ReduceStub<Text, Integer, Text, Integer> {
		public void reduce(Text key, Iterator<Integer> values, Collector<Text, Integer> out) {
			int sum = 0;
			while (values.hasNext())
				sum += values.next().value;
			out.collect(key, new Integer(sum));
		}
	}

	@Override
	protected void preSubmit() throws Exception {
		OutputStream os = getFilesystemProvider().getOutputStream(TEST_FILE_IN);
		Writer wr = new OutputStreamWriter(os);
		for (int i = 0; i < TEST_DATA_NUM.length; i++) {
			for (int j = 0; j < TEST_DATA_NUM[i]; j++) {
				wr.write(TEST_DATA_STR[i] + "\n");
			}
		}
		wr.close();
	}

	@Override
	protected JobGraph getJobGraph() throws Exception {
		DataSourceContract<Text, Text> source = new DataSourceContract<Text, Text>(TextFormatIn.class,
			getFilesystemProvider().getTempDirPath() + "/" + TEST_FILE_IN);
		source.setFormatParameter("delimiter", " ");
		MapContract<Text, Text, Text, Integer> map = new MapContract<Text, Text, Text, Integer>(Mapper.class);
		ReduceContract<Text, Integer, Text, Integer> reduce = new ReduceContract<Text, Integer, Text, Integer>(
			Reducer.class);
		DataSinkContract<Text, Integer> sink = new DataSinkContract<Text, Integer>(TextFormatOut.class,
			getFilesystemProvider().getTempDirPath() + "/" + TEST_FILE_OUT);

		sink.setInput(reduce);
		reduce.setInput(map);
		map.setInput(source);

		// TODO not defined by user, afaik (en)
		map.setDegreeOfParallelism(config.getInteger("NoSubtasks", 1));
		reduce.setDegreeOfParallelism(config.getInteger("NoSubtasks", 1));

		Plan plan = new Plan(sink);

		PactCompiler pc = new PactCompiler();
		OptimizedPlan op = pc.compile(plan);

		JobGraphGenerator jgg = new JobGraphGenerator();
		return jgg.compileJobGraph(op);
	}

	@Override
	protected void postSubmit() throws Exception {
		// TODO ################################### FIXME nephele submitAndWait
		// not working yet (en)
		Thread.sleep(60 * 1000);

		// read result
		InputStream is = getFilesystemProvider().getInputStream(getFilesystemProvider().getTempDirPath() + "/" + TEST_FILE_OUT);
		BufferedReader reader = new BufferedReader(new InputStreamReader(is));
		String line = reader.readLine();
		Assert.assertNotNull("no output", line);

		// expected words
		Set<String> words = new HashSet<String>(Arrays.asList(TEST_DATA_STR));

		// check aggregation
		while (line != null) {
			// print
			System.out.println("### >>> out = " + line);

			// split
			String[] parts = line.split(":");
			String word = parts[0];
			int count = java.lang.Integer.parseInt(parts[1]);

			// find word and verify occurrences
			for (int i = 0; i < TEST_DATA_STR.length; i++) {
				if (word.equals(TEST_DATA_STR[i])) {
					Assert.assertTrue(count == TEST_DATA_NUM[i]);
					break;
				}
			}

			// remove word
			words.remove(word);

			// next
			line = reader.readLine();
		}

		// check totality
		Assert.assertTrue(words.isEmpty());

		// done
		reader.close();
	}

	@Parameters
	public static Collection<Object[]> getConfigurations() {

		LinkedList<Configuration> tConfigs = new LinkedList<Configuration>();

		Configuration config = new Configuration();
		config.setInteger("NoSubtasks", 4);
		tConfigs.add(config);

		return toParameterList(tConfigs);
	}

	@Override
	protected String getJarFilePath() {
		// TODO Auto-generated method stub
		return null;
	}

}
