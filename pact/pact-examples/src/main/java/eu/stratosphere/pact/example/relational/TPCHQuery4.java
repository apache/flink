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
package eu.stratosphere.pact.example.relational;

import org.apache.log4j.Logger;

import eu.stratosphere.pact.common.contract.DataSinkContract;
import eu.stratosphere.pact.common.contract.DataSourceContract;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.contract.OutputContract.UniqueKey;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.example.relational.contracts.tpch4.Aggregation;
import eu.stratosphere.pact.example.relational.contracts.tpch4.Join;
import eu.stratosphere.pact.example.relational.contracts.tpch4.LineItemFilter;
import eu.stratosphere.pact.example.relational.contracts.tpch4.OrdersFilter;
import eu.stratosphere.pact.example.relational.util.IntTupleDataInFormat;
import eu.stratosphere.pact.example.relational.util.StringTupleDataOutFormat;
import eu.stratosphere.pact.example.relational.util.Tuple;

/**
 * Implementation of the TPC-H Query 4 as a PACT program.
 * 
 * @author Mathias Peters <mathias.peters@informatik.hu-berlin.de>
 *
 */
public class TPCHQuery4 implements PlanAssembler, PlanAssemblerDescription {

	private static Logger LOGGER = Logger.getLogger(TPCHQuery4.class);
	
	private int degreeOfParallelism = 1;
	private String ordersInputPath;
	private String lineItemInputPath;
	private String outputPath;
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public Plan getPlan(String... args) throws IllegalArgumentException {
		
		if(args == null || args.length != 4)
		{
			LOGGER.warn("number of arguments do not match!");
			this.ordersInputPath = "";
			this.lineItemInputPath = "";
			this.outputPath = "";
		}else
		{
			setArgs(args);
		}
		
		DataSourceContract<PactInteger, Tuple> orders = 
			new DataSourceContract<PactInteger, Tuple>(IntTupleDataInFormat.class, this.ordersInputPath, "Orders");
		orders.setDegreeOfParallelism(this.degreeOfParallelism);
		orders.setOutputContract(UniqueKey.class);
		
		DataSourceContract<PactInteger, Tuple> lineItems =
			new DataSourceContract<PactInteger, Tuple>(IntTupleDataInFormat.class, this.lineItemInputPath, "LineItems");
		lineItems.setDegreeOfParallelism(this.degreeOfParallelism);
		
		MatchContract<PactInteger, Tuple, Tuple, PactString, Tuple> join = 
			new MatchContract<PactInteger, Tuple, Tuple, PactString, Tuple>(
				Join.class, "OrdersLineitemsJoin");
		join.setDegreeOfParallelism(degreeOfParallelism);
		
		DataSinkContract<PactString, Tuple> result = new DataSinkContract<PactString, Tuple>(
				StringTupleDataOutFormat.class, this.outputPath, "Output");
		result.setDegreeOfParallelism(degreeOfParallelism);
		
		MapContract<PactInteger, Tuple, PactInteger, Tuple> lineFilter = new MapContract<PactInteger, Tuple, PactInteger, Tuple>(
				LineItemFilter.class, "LineItemFilter");
		lineFilter.setDegreeOfParallelism(degreeOfParallelism);
		
		MapContract<PactInteger, Tuple, PactInteger, Tuple> ordersFilter = new MapContract<PactInteger, Tuple, PactInteger, Tuple>(
				OrdersFilter.class, "OrdersFilter");
		ordersFilter.setDegreeOfParallelism(degreeOfParallelism);
		
		ReduceContract<PactString, Tuple, PactString, Tuple> aggregation = new ReduceContract<PactString, Tuple, PactString, Tuple>(
				Aggregation.class, "AggregateGroupBy");
		aggregation.setDegreeOfParallelism(this.degreeOfParallelism);
		
		lineFilter.setInput(lineItems);
		ordersFilter.setInput(orders);
		join.setFirstInput(ordersFilter);
		join.setSecondInput(lineFilter);
		aggregation.setInput(join);
		result.setInput(aggregation);
		
			
		return new Plan(result, "TPC-H 4");
	}

	/**
	 * Get the args into the members.
	 * @param args
	 */
	private void setArgs(String[] args) {
		this.degreeOfParallelism = Integer.parseInt(args[0]);
		this.ordersInputPath = args[1];
		this.lineItemInputPath = args[2];
		this.outputPath = args[3];
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getDescription() {
		return "Parameters: [dop] [orders-input] [lineitem-input] [output]";
	}

}
