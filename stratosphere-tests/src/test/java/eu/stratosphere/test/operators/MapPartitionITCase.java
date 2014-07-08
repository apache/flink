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

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.java.record.operators.FileDataSink;
import eu.stratosphere.api.java.record.operators.FileDataSource;
import eu.stratosphere.api.java.record.functions.MapPartitionFunction;
import eu.stratosphere.api.java.record.io.DelimitedInputFormat;
import eu.stratosphere.api.java.record.operators.MapPartitionOperator;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.test.operators.io.ContractITCaseIOFormats.ContractITCaseInputFormat;
import eu.stratosphere.test.operators.io.ContractITCaseIOFormats.ContractITCaseOutputFormat;
import eu.stratosphere.test.util.RecordAPITestBase;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.Collector;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;

@RunWith(Parameterized.class)
public class MapPartitionITCase extends RecordAPITestBase {
	
	private static final Log LOG = LogFactory.getLog(MapITCase.class);

	String inPath = null;
	String resultPath = null;
	
	public MapPartitionITCase(Configuration testConfig) {
		super(testConfig);
	}

	private static final String IN = "1 1\n2 2\n2 8\n4 4\n4 4\n6 6\n7 7\n8 8\n" +
			"1 1\n2 2\n2 2\n4 4\n4 4\n6 3\n5 9\n8 8\n1 1\n2 2\n2 2\n3 0\n4 4\n" +
			"5 9\n7 7\n8 8\n1 1\n9 1\n5 9\n4 4\n4 4\n6 6\n7 7\n8 8\n";

	private static final String RESULT = "1 11\n2 12\n4 14\n4 14\n1 11\n2 12\n2 12\n4 14\n4 14\n3 16\n1 11\n2 12\n2 12\n0 13\n4 14\n1 11\n4 14\n4 14\n";

	@Override
	protected void preSubmit() throws Exception {
		inPath = createTempFile("in.txt", IN);
		resultPath = getTempDirPath("result");
	}

	public static class TestMapPartition extends MapPartitionFunction implements Serializable {
		private static final long serialVersionUID = 1L;

		private StringValue keyString = new StringValue();
		private StringValue valueString = new StringValue();
		

        @Override
        public void mapPartition(Iterator<Record> records, Collector<Record> out) throws Exception {
            while(records.hasNext() ){
                Record record = records.next();
                keyString = record.getField(0, keyString);
                valueString = record.getField(1, valueString);

                LOG.debug("Processed: [" + keyString.toString() + "," + valueString.getValue() + "]");

                if (Integer.parseInt(keyString.toString()) + Integer.parseInt(valueString.toString()) < 10) {

                    record.setField(0, valueString);
                    record.setField(1, new IntValue(Integer.parseInt(keyString.toString()) + 10));

                    out.collect(record);
                }
            }
        }
    }

	@Override
	protected Plan getTestJob() {
		FileDataSource input = new FileDataSource(
				new ContractITCaseInputFormat(), inPath);
		DelimitedInputFormat.configureDelimitedFormat(input)
			.recordDelimiter('\n');
		input.setDegreeOfParallelism(config.getInteger("MapPartitionTest#NoSubtasks", 1));

        MapPartitionOperator testMapper = MapPartitionOperator.builder(new TestMapPartition()).build();
		testMapper.setDegreeOfParallelism(config.getInteger("TestMapPartition#NoSubtasks", 1));

		FileDataSink output = new FileDataSink(
				new ContractITCaseOutputFormat(), resultPath);
		output.setDegreeOfParallelism(1);

		output.setInput(testMapper);
		testMapper.setInput(input);

		return new Plan(output);
	}

	@Override
	protected void postSubmit() throws Exception {
		compareResultsByLinesInMemory(RESULT, resultPath);
	}

	@Parameters
	public static Collection<Object[]> getConfigurations() throws FileNotFoundException, IOException {
		LinkedList<Configuration> testConfigs = new LinkedList<Configuration>();

		Configuration config = new Configuration();
		config.setInteger("MapPartitionTest#NoSubtasks", 4);
		testConfigs.add(config);

		return toParameterList(testConfigs);
	}
}
