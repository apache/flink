/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.test.operators;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.operators.FileDataSink;
import eu.stratosphere.api.common.operators.FileDataSource;
import eu.stratosphere.api.record.functions.CoGroupFunction;
import eu.stratosphere.api.record.io.DelimitedInputFormat;
import eu.stratosphere.api.record.io.FileOutputFormat;
import eu.stratosphere.api.record.operators.CoGroupOperator;
import eu.stratosphere.compiler.DataStatistics;
import eu.stratosphere.compiler.PactCompiler;
import eu.stratosphere.compiler.plan.OptimizedPlan;
import eu.stratosphere.compiler.plantranslate.NepheleJobGraphGenerator;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.test.util.TestBase;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.Collector;

/**
 * @author Fabian Hueske
 */
@RunWith(Parameterized.class)
public class CoGroupITCase extends TestBase {
	private static final Log LOG = LogFactory.getLog(CoGroupITCase.class);

	public CoGroupITCase(String clusterConfig, Configuration testConfig) {
		super(testConfig, clusterConfig);
	}

	private static final String COGROUP_LEFT_IN_1 = "1 1\n2 2\n3 3\n4 4\n";

	private static final String COGROUP_LEFT_IN_2 = "1 2\n2 3\n3 4\n4 5\n";

	private static final String COGROUP_LEFT_IN_3 = "1 3\n2 4\n3 5\n4 6\n";

	private static final String COGROUP_LEFT_IN_4 = "1 4\n2 5\n3 6\n4 7\n";

	private static final String COGROUP_RIGHT_IN_1 = "1 1\n2 2\n3 3\n5 1\n";

	private static final String COGROUP_RIGHT_IN_2 = "1 1\n2 2\n3 3\n6 1\n";

	private static final String COGROUP_RIGHT_IN_3 = "1 1\n2 2\n2 2\n7 1\n";

	private static final String COGROUP_RIGHT_IN_4 = "1 1\n2 2\n2 2\n8 1\n";

	private static final String COGROUP_RESULT = "1 6\n2 2\n3 12\n4 22\n5 -1\n6 -1\n7 -1\n8 -1\n";

	@Override
	protected void preSubmit() throws Exception {
		String tempPath = getFilesystemProvider().getTempDirPath();

		getFilesystemProvider().createDir(tempPath + "/cogroup_left");

		getFilesystemProvider().createFile(tempPath + "/cogroup_left/cogroupTest_1.txt", COGROUP_LEFT_IN_1);
		getFilesystemProvider().createFile(tempPath + "/cogroup_left/cogroupTest_2.txt", COGROUP_LEFT_IN_2);
		getFilesystemProvider().createFile(tempPath + "/cogroup_left/cogroupTest_3.txt", COGROUP_LEFT_IN_3);
		getFilesystemProvider().createFile(tempPath + "/cogroup_left/cogroupTest_4.txt", COGROUP_LEFT_IN_4);

		getFilesystemProvider().createDir(tempPath + "/cogroup_right");

		getFilesystemProvider().createFile(tempPath + "/cogroup_right/cogroupTest_1.txt", COGROUP_RIGHT_IN_1);
		getFilesystemProvider().createFile(tempPath + "/cogroup_right/cogroupTest_2.txt", COGROUP_RIGHT_IN_2);
		getFilesystemProvider().createFile(tempPath + "/cogroup_right/cogroupTest_3.txt", COGROUP_RIGHT_IN_3);
		getFilesystemProvider().createFile(tempPath + "/cogroup_right/cogroupTest_4.txt", COGROUP_RIGHT_IN_4);
	}

	public static class CoGroupTestInFormat extends DelimitedInputFormat {
		private static final long serialVersionUID = 1L;
		
		private final StringValue keyString = new StringValue();
		private final StringValue valueString = new StringValue();
		
		@Override
		public boolean readRecord(Record target, byte[] bytes, int offset, int numBytes) {
			this.keyString.setValueAscii(bytes, offset, 1);
			this.valueString.setValueAscii(bytes, offset + 2, 1);
			target.setField(0, keyString);
			target.setField(1, valueString);
			
			LOG.debug("Read in: [" + keyString.getValue() + "," + valueString.getValue() + "]");
			
			return true;
		}

	}

	public static class CoGroupOutFormat extends FileOutputFormat {
		private static final long serialVersionUID = 1L;
		
		private final StringBuilder buffer = new StringBuilder();
		private final StringValue keyString = new StringValue();
		private final IntValue valueInteger = new IntValue();
		
		@Override
		public void writeRecord(Record record) throws IOException {
			this.buffer.setLength(0);
			this.buffer.append(record.getField(0, keyString).toString());
			this.buffer.append(' ');
			this.buffer.append(record.getField(1, valueInteger).getValue());
			this.buffer.append('\n');
			
			byte[] bytes = this.buffer.toString().getBytes();
			
			LOG.debug("Writing out: [" + keyString.toString() + "," + valueInteger.getValue() + "]");
			
			this.stream.write(bytes);
		}
	}

	public static class TestCoGrouper extends CoGroupFunction implements Serializable {
		private static final long serialVersionUID = 1L;

		private StringValue keyString = new StringValue();
		private StringValue valueString = new StringValue();
		private Record record = new Record();
		
		@Override
		public void coGroup(Iterator<Record> records1, Iterator<Record> records2, Collector<Record> out) {
			// TODO Auto-generated method stub
			
			int sum = 0;
			LOG.debug("Start iterating over input1");
			while (records1.hasNext()) {
				record = records1.next();
				keyString = record.getField(0, keyString);
				valueString = record.getField(1, valueString);
				sum += Integer.parseInt(valueString.getValue());

				LOG.debug("Processed: [" + keyString.getValue() + "," + valueString.getValue() + "]");
			}
			LOG.debug("Start iterating over input2");
			while (records2.hasNext()) {
				record = records2.next();
				keyString = record.getField(0, keyString);
				valueString = record.getField(1, valueString);
				sum -= Integer.parseInt(valueString.getValue());

				LOG.debug("Processed: [" + keyString.getValue() + "," + valueString.getValue() + "]");
			}
			record.setField(1, new IntValue(sum));
			LOG.debug("Finished");
			
			out.collect(record);
		}

	}

	@Override
	protected JobGraph getJobGraph() throws Exception {

		String pathPrefix = getFilesystemProvider().getURIPrefix() + getFilesystemProvider().getTempDirPath();

		FileDataSource input_left =  new FileDataSource(new CoGroupTestInFormat(), pathPrefix + "/cogroup_left");
		DelimitedInputFormat.configureDelimitedFormat(input_left)
			.recordDelimiter('\n');
		input_left.setDegreeOfParallelism(config.getInteger("CoGroupTest#NoSubtasks", 1));

		FileDataSource input_right =  new FileDataSource(new CoGroupTestInFormat(), pathPrefix + "/cogroup_right");
		DelimitedInputFormat.configureDelimitedFormat(input_right)
			.recordDelimiter('\n');
		input_right.setDegreeOfParallelism(config.getInteger("CoGroupTest#NoSubtasks", 1));

		CoGroupOperator testCoGrouper = CoGroupOperator.builder(new TestCoGrouper(), StringValue.class, 0, 0)
			.build();
		testCoGrouper.setDegreeOfParallelism(config.getInteger("CoGroupTest#NoSubtasks", 1));
		testCoGrouper.getParameters().setString(PactCompiler.HINT_LOCAL_STRATEGY,
				config.getString("CoGroupTest#LocalStrategy", ""));
		testCoGrouper.getParameters().setString(PactCompiler.HINT_SHIP_STRATEGY,
				config.getString("CoGroupTest#ShipStrategy", ""));

		FileDataSink output = new FileDataSink(new CoGroupOutFormat(), pathPrefix + "/result.txt");
		output.setDegreeOfParallelism(1);

		output.addInput(testCoGrouper);
		testCoGrouper.addFirstInput(input_left);
		testCoGrouper.addSecondInput(input_right);

		Plan plan = new Plan(output);

		PactCompiler pc = new PactCompiler(new DataStatistics());
		OptimizedPlan op = pc.compile(plan);

		NepheleJobGraphGenerator jgg = new NepheleJobGraphGenerator();
		return jgg.compileJobGraph(op);
	}

	@Override
	protected void postSubmit() throws Exception {

		String tempPath = getFilesystemProvider().getTempDirPath();

		compareResultsByLinesInMemory(COGROUP_RESULT, tempPath + "/result.txt");
				
		getFilesystemProvider().delete(tempPath + "/result.txt", true);
		getFilesystemProvider().delete(tempPath + "/cogroup_left", true);
		getFilesystemProvider().delete(tempPath + "/cogroup_right", true);
	}

	@Parameters
	public static Collection<Object[]> getConfigurations() throws FileNotFoundException, IOException {

		LinkedList<Configuration> tConfigs = new LinkedList<Configuration>();

		String[] localStrategies = { PactCompiler.HINT_LOCAL_STRATEGY_SORT_BOTH_MERGE};

		String[] shipStrategies = { PactCompiler.HINT_SHIP_STRATEGY_REPARTITION_HASH, };

		for (String localStrategy : localStrategies) {
			for (String shipStrategy : shipStrategies) {

				Configuration config = new Configuration();
				config.setString("CoGroupTest#LocalStrategy", localStrategy);
				config.setString("CoGroupTest#ShipStrategy", shipStrategy);
				config.setInteger("CoGroupTest#NoSubtasks", 4);

				tConfigs.add(config);
			}
		}

		return toParameterList(CoGroupITCase.class, tConfigs);
	}
}
