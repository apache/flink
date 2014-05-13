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

package eu.stratosphere.test.recordJobs.relational;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.Program;
import eu.stratosphere.api.common.ProgramDescription;
import eu.stratosphere.api.java.record.operators.FileDataSink;
import eu.stratosphere.api.java.record.operators.FileDataSource;
import eu.stratosphere.api.java.record.operators.MapOperator;
import eu.stratosphere.api.java.record.operators.ReduceOperator;
import eu.stratosphere.test.recordJobs.relational.query1Util.GroupByReturnFlag;
import eu.stratosphere.test.recordJobs.relational.query1Util.LineItemFilter;
import eu.stratosphere.test.recordJobs.util.IntTupleDataInFormat;
import eu.stratosphere.test.recordJobs.util.StringTupleDataOutFormat;
import eu.stratosphere.types.StringValue;


public class TPCHQuery1 implements Program, ProgramDescription {

	private static final long serialVersionUID = 1L;

	private int degreeOfParallelism = 1;
	private String lineItemInputPath;
	private String outputPath;
	
	@Override
	public Plan getPlan(String... args) throws IllegalArgumentException {
		
		
		if(args.length != 3)
		{
			this.degreeOfParallelism = 1;
			this.lineItemInputPath = "";
			this.outputPath = "";
		}else{
			this.degreeOfParallelism = Integer.parseInt(args[0]);
			this.lineItemInputPath = args[1];
			this.outputPath = args[2];
		}
		
		FileDataSource lineItems =
			new FileDataSource(new IntTupleDataInFormat(), this.lineItemInputPath, "LineItems");
		lineItems.setDegreeOfParallelism(this.degreeOfParallelism);
		
		FileDataSink result = 
			new FileDataSink(new StringTupleDataOutFormat(), this.outputPath, "Output");
		result.setDegreeOfParallelism(this.degreeOfParallelism);
		
		MapOperator lineItemFilter = 
			MapOperator.builder(new LineItemFilter())
			.name("LineItem Filter")
			.build();
		lineItemFilter.setDegreeOfParallelism(this.degreeOfParallelism);
		
		ReduceOperator groupByReturnFlag = 
			ReduceOperator.builder(new GroupByReturnFlag(), StringValue.class, 0)
			.name("groupyBy")
			.build();
		
		lineItemFilter.setInput(lineItems);
		groupByReturnFlag.setInput(lineItemFilter);
		result.setInput(groupByReturnFlag);
		
		return new Plan(result, "TPC-H 1");
	}

	@Override
	public String getDescription() {
		return "Parameters: [dop] [lineitem-input] [output]";
	}
}
