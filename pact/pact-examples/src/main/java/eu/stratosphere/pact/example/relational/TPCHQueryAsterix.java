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

import java.util.Iterator;

import eu.stratosphere.pact.common.contract.FileDataSinkContract;
import eu.stratosphere.pact.common.contract.FileDataSourceContract;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.contract.OutputContract.SameKey;
import eu.stratosphere.pact.common.contract.OutputContract.UniqueKey;
import eu.stratosphere.pact.common.contract.ReduceContract.Combinable;
import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.io.TextOutputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MapStub;
import eu.stratosphere.pact.common.stub.MatchStub;
import eu.stratosphere.pact.common.stub.ReduceStub;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactNull;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.example.relational.util.IntTupleDataInFormat;
import eu.stratosphere.pact.example.relational.util.Tuple;

/**
 * The TPC-H is a decision support benchmark on relational data.
 * Its documentation and the data generator (DBGEN) can be found
 * on http://www.tpc.org/tpch/ .This implementation is tested with
 * the DB2 data format.  
 * This PACT program implements a query on the TPC-H schema 
 * including one join and an aggregation.
 * This query is used as example in the Asterix project
 * (http://asterix.ics.uci.edu/).
 * 
 * SELECT c_mktsegment, COUNT(o_orderkey)
 *   FROM orders, customer
 *   WHERE c_custkey = o_custkey
 * GROUP BY c_mktsegment;
 * 
 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
 */
public class TPCHQueryAsterix implements PlanAssembler, PlanAssemblerDescription {

	/**
	 * Serializes a PactString,PactInteger key-value-pair into a byte array.
	 * 
	 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
	 *
	 */
	public static class StringIntOutFormat extends TextOutputFormat<PactString, PactInteger> {

		@Override
		public byte[] writeLine(KeyValuePair<PactString, PactInteger> pair) {
			
			return (pair.getKey().getValue()+"|"+pair.getValue().getValue()+"|\n").getBytes();
		}
		
	}
	
	/**
	 * Map PACT implements the projection on the orders table.   
	 */
	public static class ProjectOrder extends MapStub<PactInteger, Tuple, PactInteger, PactNull> {

		/**
	 	 * Output Schema:
	 	 *  Key: CUSTKEY
	 	 *  Value: 0:ORDERKEY
		 */
		@Override
		public void map(final PactInteger oKey, final Tuple value, final Collector<PactInteger, PactNull> out) {

			PactInteger custKey = new PactInteger((int)value.getLongValueAt(1));
			
			out.collect(custKey, new PactNull());
		}
	}


	/**
	 * Map PACT implements the projection on the Customer table. The SameKey
	 * OutputContract is annotated because the key does not change during
	 * projection.
	 *
	 */
	@SameKey
	public static class ProjectCust extends MapStub<PactInteger, Tuple, PactInteger, PactString> {

		/**
		 * Output Schema:
		 *  Key: CUSTOMERKEY
		 *  Value: 0:MKTSEGMENT
		 */
		@Override
		public void map(PactInteger cKey, Tuple value, Collector<PactInteger, PactString> out) {

			PactString mktSegment = new PactString(value.getStringValueAt(6));
			
			out.collect(cKey, mktSegment);
		}
	}

	/**
	 * Realizes the join between Customers and Order table.
	 */
	public static class JoinCO extends MatchStub<PactInteger, PactNull, PactString, PactString, PactInteger> {

		/**
		 * Output Schema:
		 *  Key: C_MKTSEGMENT
		 *  Value: 0:PARTIAL_COUNT=1
		 */
		@Override
		public void match(PactInteger cKey, PactNull oVal, PactString mktSeg, Collector<PactString, PactInteger> out) {

			out.collect(mktSeg, new PactInteger(1));
		}
	}

	/**
	 * Reduce PACT implements the aggregation of the results. The 
	 * Combinable annotation is set as the partial counts can be calculated
	 * already in the combiner
	 *
	 */
	@Combinable
	public static class AggCO extends ReduceStub<PactString, PactInteger, PactString, PactInteger> {

		/**
		 * Output Schema:
		 *  Key: C_MKTSEGMENT
		 *  Value: 0:COUNT
		 *
		 */
		@Override
		public void reduce(PactString mktSeg, Iterator<PactInteger> values, Collector<PactString, PactInteger> out) {

			int count = 0;

			while (values.hasNext()) {
				count+=values.next().getValue();
			}

			out.collect(mktSeg, new PactInteger(count));

		}

		/**
		 * Computes partial counts
		 */
		@Override
		public void combine(PactString mktSeg, Iterator<PactInteger> values, Collector<PactString, PactInteger> out) {

			int partialCount = 0;

			while (values.hasNext()) {
				partialCount+=values.next().getValue();
			}

			out.collect(mktSeg, new PactInteger(partialCount));
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Plan getPlan(final String... args) {

		// parse program parameters
		int noSubtasks       = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		String ordersPath    = (args.length > 1 ? args[1] : "");
		String customerPath  = (args.length > 2 ? args[2] : "");
		String output        = (args.length > 3 ? args[3] : "");

		// create DataSourceContract for Orders input
		FileDataSourceContract<PactInteger, Tuple> orders = new FileDataSourceContract<PactInteger, Tuple>(
			IntTupleDataInFormat.class, ordersPath, "Orders");
		orders.setParameter(TextInputFormat.RECORD_DELIMITER, "\n");
		orders.setDegreeOfParallelism(noSubtasks);
		orders.setOutputContract(UniqueKey.class);
		orders.getCompilerHints().setAvgNumValuesPerKey(1);

		// create DataSourceContract for LineItems input
		FileDataSourceContract<PactInteger, Tuple> customers = new FileDataSourceContract<PactInteger, Tuple>(
			IntTupleDataInFormat.class, customerPath, "Customers");
		customers.setParameter(TextInputFormat.RECORD_DELIMITER, "\n");
		customers.setDegreeOfParallelism(noSubtasks);
		customers.setOutputContract(UniqueKey.class);

		// create MapContract for filtering Orders tuples
		MapContract<PactInteger, Tuple, PactInteger, PactNull> projectO = new MapContract<PactInteger, Tuple, PactInteger, PactNull>(
			ProjectOrder.class, "ProjectO");
		projectO.setDegreeOfParallelism(noSubtasks);
		projectO.getCompilerHints().setAvgBytesPerRecord(5);
		projectO.getCompilerHints().setAvgNumValuesPerKey(10);

		// create MapContract for projecting LineItems tuples
		MapContract<PactInteger, Tuple, PactInteger, PactString> projectC = new MapContract<PactInteger, Tuple, PactInteger, PactString>(
			ProjectCust.class, "ProjectC");
		projectC.setDegreeOfParallelism(noSubtasks);
		projectC.getCompilerHints().setAvgNumValuesPerKey(1);
		projectC.getCompilerHints().setAvgBytesPerRecord(20);

		// create MatchContract for joining Orders and LineItems
		MatchContract<PactInteger, PactNull, PactString, PactString, PactInteger> joinCO = new MatchContract<PactInteger, PactNull, PactString, PactString, PactInteger>(
			JoinCO.class, "JoinCO");
		joinCO.setDegreeOfParallelism(noSubtasks);
		joinCO.getCompilerHints().setAvgBytesPerRecord(17);

		// create ReduceContract for aggregating the result
		ReduceContract<PactString, PactInteger, PactString, PactInteger> aggCO = new ReduceContract<PactString, PactInteger, PactString, PactInteger>(
			AggCO.class, "AggCo");
		aggCO.setDegreeOfParallelism(noSubtasks);
		aggCO.getCompilerHints().setAvgBytesPerRecord(17);
		aggCO.getCompilerHints().setAvgNumValuesPerKey(1);

		// create DataSinkContract for writing the result
		FileDataSinkContract<PactString, PactInteger> result = new FileDataSinkContract<PactString, PactInteger>(
			StringIntOutFormat.class, output, "Output");
		result.setDegreeOfParallelism(noSubtasks);

		// assemble the PACT plan
		result.setInput(aggCO);
		aggCO.setInput(joinCO);
		joinCO.setFirstInput(projectO);
		projectO.setInput(orders);
		joinCO.setSecondInput(projectC);
		projectC.setInput(customers);

		// return the PACT plan
		return new Plan(result, "TPCH Asterix");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getDescription() {
		return "Parameters: [noSubStasks], [orders], [customer], [output]";
	}

}
