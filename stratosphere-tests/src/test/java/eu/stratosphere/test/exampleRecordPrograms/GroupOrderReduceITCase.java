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

package eu.stratosphere.test.exampleRecordPrograms;

import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.api.Plan;
import eu.stratosphere.api.operators.FileDataSink;
import eu.stratosphere.api.operators.FileDataSource;
import eu.stratosphere.api.operators.Order;
import eu.stratosphere.api.operators.Ordering;
import eu.stratosphere.api.record.functions.ReduceFunction;
import eu.stratosphere.api.record.io.CsvInputFormat;
import eu.stratosphere.api.record.io.CsvOutputFormat;
import eu.stratosphere.api.record.operators.ReduceOperator;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.test.util.TestBase2;
import eu.stratosphere.types.PactInteger;
import eu.stratosphere.types.PactRecord;
import eu.stratosphere.util.Collector;

@RunWith(Parameterized.class)
public class GroupOrderReduceITCase extends TestBase2 {

	private static final String INPUT = "1,3\n" + "2,1\n" + "5,1\n" + "3,1\n" + "1,8\n" + "1,9\n" + 
										"1,2\n" + "2,3\n" + "7,1\n" + "4,2\n" + "2,7\n" + "2,8\n" +
										"1,1\n" + "2,7\n" + "5,4\n" + "4,3\n" + "3,6\n" + "3,7\n" +
										"1,3\n" + "2,4\n" + "7,1\n" + "5,3\n" + "4,5\n" + "4,6\n" +
										"1,4\n" + "3,9\n" + "8,5\n" + "5,3\n" + "5,4\n" + "5,5\n" +
										"1,7\n" + "3,9\n" + "9,3\n" + "6,2\n" + "6,3\n" + "6,4\n" +
										"1,8\n" + "3,8\n" + "8,7\n" + "6,2\n" + "7,2\n" + "7,3\n" +
										"1,1\n" + "3,7\n" + "9,2\n" + "7,1\n" + "8,1\n" + "8,2\n" +
										"1,2\n" + "2,6\n" + "8,7\n" + "7,1\n" + "9,1\n" + "9,1\n" +
										"1,1\n" + "2,5\n" + "9,5\n" + "8,2\n" + "10,2\n" + "10,1\n" +
										"1,1\n" + "2,6\n" + "2,7\n" + "8,3\n" + "11,3\n" + "11,2\n" +
										"1,2\n" + "2,7\n" + "4,2\n" + "9,4\n" + "12,8\n" + "12,3\n" +
										"1,2\n" + "4,8\n" + "1,7\n" + "9,5\n" + "13,9\n" + "13,4\n" +
										"1,3\n" + "4,2\n" + "3,2\n" + "9,6\n" + "14,7\n" + "14,5\n";

	protected String textPath;
	protected String resultPath;

	
	public GroupOrderReduceITCase(Configuration config) {
		super(config);
	}

	
	@Override
	protected void preSubmit() throws Exception {
		textPath = createTempFile("pairs.csv", INPUT);
		resultPath = getTempDirPath("result");
	}

	@Override
	protected Plan getTestJob() {
		
		int dop = this.config.getInteger("GroupOrderTest#NumSubtasks", 1);
		
		@SuppressWarnings("unchecked")
		CsvInputFormat format = new CsvInputFormat(',', PactInteger.class, PactInteger.class);
		FileDataSource source = new FileDataSource(format, this.textPath, "Source");
		
		ReduceOperator reducer = ReduceOperator.builder(CheckingReducer.class)
			.keyField(PactInteger.class, 0)
			.input(source)
			.name("Ordered Reducer")
			.build();
		reducer.setGroupOrder(new Ordering(1, PactInteger.class, Order.ASCENDING));
		
		FileDataSink sink = new FileDataSink(CsvOutputFormat.class, this.resultPath, reducer, "Sink");
		CsvOutputFormat.configureRecordFormat(sink)
			.recordDelimiter('\n')
			.fieldDelimiter(',')
			.field(PactInteger.class, 0)
			.field(PactInteger.class, 1);
		
		Plan p = new Plan(sink);
		p.setDefaultParallelism(dop);
		return p;
	}

	@Override
	protected void postSubmit() throws Exception {
	}

	@Parameters
	public static Collection<Object[]> getConfigurations() {
		Configuration config = new Configuration();
		config.setInteger("GroupOrderTest#NumSubtasks", 4);
		return toParameterList(config);
	}
	
	public static final class CheckingReducer extends ReduceFunction implements Serializable {
		
		private static final long serialVersionUID = 1L;

		@Override
		public void reduce(Iterator<PactRecord> records, Collector<PactRecord> out) throws Exception {
			int lastValue = records.next().getField(1, PactInteger.class).getValue();
			
			while (records.hasNext()) {
				int nextValue = records.next().getField(1, PactInteger.class).getValue();
				
				if (nextValue < lastValue) {
					throw new Exception("Group Order is violated!");
				}
				
				lastValue = nextValue;
			}
		}
	}
}
