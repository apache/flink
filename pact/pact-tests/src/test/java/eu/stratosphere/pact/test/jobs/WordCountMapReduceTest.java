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

package eu.stratosphere.pact.test.jobs;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

import org.junit.Assert;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.pact.test.util.TestBase;

/**
 * @author Erik Nijkamp
 */
@RunWith(Parameterized.class)
public class WordCountMapReduceTest extends TestBase {

	private static final String TEST_FILE_IN = "words.txt";

	private static final String TEST_FILE_OUT = "/result";

	private static final String[] TEST_DATA_STR = { "4444", "1111", "2222" };

	private static final int[] TEST_DATA_NUM = { 4, 1, 2 };

	public WordCountMapReduceTest(Configuration config) {
		super(config);
		// TODO Auto-generated constructor stub
	}

	@Override
	protected void preSubmit() throws Exception {
		OutputStream os = getHDFSProvider().getHdfsOutputStream(TEST_FILE_IN);
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
		/*
		 * JobGraph jobGraph = new JobGraph("Word Count Example Job");
		 * // input vertex
		 * JobFileInputVertex input = new JobFileInputVertex("DataSource", jobGraph);
		 * // vertex config
		 * {
		 * // vertex configuration
		 * input.setFileInputClass(DataSourceTask.class);
		 * input.setFilePath(new Path(getHDFSProvider().getHdfsHome() + "/" + TEST_FILE_IN));
		 * // format parameters
		 * Configuration formatParameters = new Configuration();
		 * formatParameters.setString("delimiter", " ");
		 * // task configuration
		 * DataSourceTask.Config taskConfig = new DataSourceTask.Config(input.getConfiguration());
		 * taskConfig.setStubClass(TextFormatIn.class);
		 * taskConfig.setStubParameters(formatParameters);
		 * }
		 * // map vertex
		 * JobTaskVertex map = new JobTaskVertex("Map", jobGraph);
		 * // vertex config
		 * {
		 * // vertex configuration
		 * map.setTaskClass(MapTask.class);
		 * // task configuration
		 * TaskConfig taskConfig = map.getConfiguration();
		 * taskConfig.setStubClass(Mapper.class);
		 * taskConfig.setOutputStrategy(OutputStrategy.PARTIAL_SORTED);
		 * taskConfig.setOutputShipStrategy(ShipStrategy.PARTITION);
		 * }
		 * // reduce vertex
		 * JobTaskVertex reduce = new JobTaskVertex("Reduce", jobGraph);
		 * // vertex config
		 * {
		 * // vertex configuration
		 * reduce.setTaskClass(ReduceTask.class);
		 * // task configuration
		 * ReduceTask.Config taskConfig = new ReduceTask.Config(reduce.getConfiguration());
		 * taskConfig.setStubClass(Reducer.class);
		 * }
		 * // output vertex
		 * JobFileOutputVertex output = new JobFileOutputVertex("DataSink", jobGraph);
		 * // vertex config
		 * {
		 * // vertex configuration
		 * output.setFileOutputClass(DataSinkTask.class);
		 * output.setFilePath(new Path(getHDFSProvider().getHdfsHome() + "/" + TEST_FILE_OUT));
		 * // task configuration
		 * DataSinkTask.Config taskConfig = new DataSinkTask.Config(output.getConfiguration());
		 * taskConfig.setStubClass(TextFormatOut.class);
		 * }
		 * // create edges
		 * {
		 * // input to map
		 * input.connectTo(map, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION);
		 * // map to reduce
		 * map.connectTo(reduce, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION);
		 * // reduce to output
		 * reduce.connectTo(output, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION);
		 * }
		 * return jobGraph;
		 */
		return null;
	}

	@Override
	protected void postSubmit() throws Exception {
		// TODO ################################### FIXME nephele submitAndWait
		// not working yet (en)
		Thread.sleep(20 * 1000);

		// read result
		InputStream is = getHDFSProvider().getHdfsInputStream(getHDFSProvider().getHdfsHome() + "/" + TEST_FILE_OUT);
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
			int count = Integer.parseInt(parts[1]);

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
	public String getJarFilePath() {
		return null;
	}
}
