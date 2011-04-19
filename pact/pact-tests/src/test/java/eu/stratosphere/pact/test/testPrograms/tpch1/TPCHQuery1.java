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

import eu.stratosphere.pact.common.contract.DataSinkContract;
import eu.stratosphere.pact.common.contract.DataSourceContract;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.example.relational.util.IntTupleDataInFormat;
import eu.stratosphere.pact.example.relational.util.StringTupleDataOutFormat;
import eu.stratosphere.pact.example.relational.util.Tuple;

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
		
		DataSourceContract<PactInteger, Tuple> lineItems =
			new DataSourceContract<PactInteger, Tuple>(IntTupleDataInFormat.class, this.lineItemInputPath, "LineItems");
		lineItems.setDegreeOfParallelism(this.degreeOfParallelism);
		
		DataSinkContract<PactString, Tuple> result = new DataSinkContract<PactString, Tuple>(
				StringTupleDataOutFormat.class, this.outputPath, "Output");
		result.setDegreeOfParallelism(this.degreeOfParallelism);
		
		MapContract<PactInteger , Tuple, PactString, Tuple> lineItemFilter = 
			new MapContract<PactInteger, Tuple, PactString, Tuple>(LineItemFilter.class, "LineItem Filter");
		lineItemFilter.setDegreeOfParallelism(this.degreeOfParallelism);
		
		ReduceContract<PactString, Tuple, PactString, Tuple> groupByReturnFlag = 
			new ReduceContract<PactString, Tuple, PactString, Tuple>(GroupByReturnFlag.class, "groupyBy");
		
		lineItemFilter.setInput(lineItems);
		groupByReturnFlag.setInput(lineItemFilter);
		result.setInput(groupByReturnFlag);
		
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
