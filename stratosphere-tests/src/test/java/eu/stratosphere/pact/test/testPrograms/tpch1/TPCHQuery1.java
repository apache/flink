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

package eu.stratosphere.pact.test.testPrograms.tpch1;

import org.apache.log4j.Logger;

import eu.stratosphere.api.operators.FileDataSink;
import eu.stratosphere.api.operators.FileDataSource;
import eu.stratosphere.api.plan.Plan;
import eu.stratosphere.api.plan.PlanAssembler;
import eu.stratosphere.api.plan.PlanAssemblerDescription;
import eu.stratosphere.api.record.operators.MapOperator;
import eu.stratosphere.api.record.operators.ReduceOperator;
import eu.stratosphere.pact.test.testPrograms.util.IntTupleDataInFormat;
import eu.stratosphere.pact.test.testPrograms.util.StringTupleDataOutFormat;
import eu.stratosphere.types.PactString;

/**
 * @author Mathias Peters <mathias.peters@informatik.hu-berlin.de>
 *
 */
public class TPCHQuery1 implements PlanAssembler, PlanAssemblerDescription {

	@SuppressWarnings("unused")
	private static Logger LOGGER = Logger.getLogger(TPCHQuery1.class);
	
	private int degreeOfParallelism = 1;
	private String lineItemInputPath;
	private String outputPath;
	
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.plan.PlanAssembler#getPlan(java.lang.String[])
	 */
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
			ReduceOperator.builder(new GroupByReturnFlag(), PactString.class, 0)
			.name("groupyBy")
			.build();
		
		lineItemFilter.addInput(lineItems);
		groupByReturnFlag.addInput(lineItemFilter);
		result.addInput(groupByReturnFlag);
		
		return new Plan(result, "TPC-H 1");
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.plan.PlanAssemblerDescription#getDescription()
	 */
	@Override
	public String getDescription() {
		return "Parameters: [dop] [lineitem-input] [output]";
	}

}
