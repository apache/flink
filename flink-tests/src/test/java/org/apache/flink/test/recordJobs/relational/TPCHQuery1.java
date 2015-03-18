/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.recordJobs.relational;

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.Program;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.record.operators.FileDataSink;
import org.apache.flink.api.java.record.operators.FileDataSource;
import org.apache.flink.api.java.record.operators.MapOperator;
import org.apache.flink.api.java.record.operators.ReduceOperator;
import org.apache.flink.test.recordJobs.relational.query1Util.GroupByReturnFlag;
import org.apache.flink.test.recordJobs.relational.query1Util.LineItemFilter;
import org.apache.flink.test.recordJobs.util.IntTupleDataInFormat;
import org.apache.flink.test.recordJobs.util.StringTupleDataOutFormat;
import org.apache.flink.types.StringValue;

@SuppressWarnings("deprecation")
public class TPCHQuery1 implements Program, ProgramDescription {

	private static final long serialVersionUID = 1L;

	private int parallelism = 1;
	private String lineItemInputPath;
	private String outputPath;
	
	@Override
	public Plan getPlan(String... args) throws IllegalArgumentException {
		
		
		if (args.length != 3) {
			this.parallelism = 1;
			this.lineItemInputPath = "";
			this.outputPath = "";
		} else {
			this.parallelism = Integer.parseInt(args[0]);
			this.lineItemInputPath = args[1];
			this.outputPath = args[2];
		}
		
		FileDataSource lineItems =
			new FileDataSource(new IntTupleDataInFormat(), this.lineItemInputPath, "LineItems");
		lineItems.setParallelism(this.parallelism);
		
		FileDataSink result = 
			new FileDataSink(new StringTupleDataOutFormat(), this.outputPath, "Output");
		result.setParallelism(this.parallelism);
		
		MapOperator lineItemFilter = 
			MapOperator.builder(new LineItemFilter())
			.name("LineItem Filter")
			.build();
		lineItemFilter.setParallelism(this.parallelism);
		
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
		return "Parameters: [parallelism] [lineitem-input] [output]";
	}
}
