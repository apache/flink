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

import java.io.IOException;
import java.util.Iterator;

import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.OutputContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.contract.ReduceContract.Combinable;
import eu.stratosphere.pact.common.io.FileOutputFormat;
import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
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
	public static class StringIntOutFormat extends FileOutputFormat {

		private final StringBuilder buffer = new StringBuilder();
		private final PactString keyString = new PactString();
		private final PactInteger valueInteger = new PactInteger();
		
		@Override
		public void writeRecord(PactRecord record) throws IOException {
			this.buffer.setLength(0);
			this.buffer.append(record.getField(0, keyString).toString());
			this.buffer.append('|');
			this.buffer.append(record.getField(1, valueInteger).getValue());
			this.buffer.append("|\n");
			
			byte[] bytes = this.buffer.toString().getBytes();
			
			this.stream.write(bytes);
		}
		
	}
	
	/**
	 * Map PACT implements the projection on the orders table.   
	 */
	public static class ProjectOrder extends MapStub {

		private Tuple value = new Tuple();
		private final PactInteger custKey = new PactInteger();

		/**
	 	 * Output Schema:
	 	 *  Key: CUSTKEY
	 	 *  Value: 0:ORDERKEY
		 */
		@Override
		public void map(PactRecord record, Collector out) throws Exception {
			value = record.getField(1, value);
			custKey.setValue((int)value.getLongValueAt(1));
			record.clear();
			record.setField(0, custKey);
			out.collect(record);
		}
	}


	/**
	 * Map PACT implements the projection on the Customer table. The SameKey
	 * OutputContract is annotated because the key does not change during
	 * projection.
	 *
	 */
	@OutputContract.Constant(0)
	public static class ProjectCust extends MapStub {

		private Tuple value = new Tuple();
		
		/**
		 * Output Schema:
		 *  Key: CUSTOMERKEY
		 *  Value: 0:MKTSEGMENT
		 */
		@Override
		public void map(PactRecord record, Collector out) throws Exception {
			value = record.getField(1, value);
			PactString mktSegment = new PactString(value.getStringValueAt(6));
			record.setField(1, mktSegment);
			
			out.collect(record);
			
		}
	}

	/**
	 * Realizes the join between Customers and Order table.
	 */
	public static class JoinCO extends MatchStub {

		private final PactInteger oneInteger = new PactInteger(1);
		private PactString mktSeg = new PactString();
		
		/**
		 * Output Schema:
		 *  Key: C_MKTSEGMENT
		 *  Value: 0:PARTIAL_COUNT=1
		 */
		@Override
		public void match(PactRecord value1, PactRecord value2, Collector out)
				throws Exception {
			mktSeg = value2.getField(1, mktSeg);
			value2.setField(0, mktSeg);
			value2.setField(1, oneInteger);
			out.collect(value2);
			
		}
	}

	/**
	 * Reduce PACT implements the aggregation of the results. The 
	 * Combinable annotation is set as the partial counts can be calculated
	 * already in the combiner
	 *
	 */
	@Combinable
	public static class AggCO extends ReduceStub {

		private final PactInteger integer = new PactInteger();
		private PactRecord record = new PactRecord();
	
		/**
		 * Output Schema:
		 *  Key: C_MKTSEGMENT
		 *  Value: 0:COUNT
		 *
		 */
		@Override
		public void reduce(Iterator<PactRecord> records, Collector out)
				throws Exception {

			int count = 0;

			while (records.hasNext()) {
				record = records.next();
				count+=record.getField(1, integer).getValue();
			}

			integer.setValue(count);
			record.setField(1, integer);
			out.collect(record);
		}
		
		/**
		 * Computes partial counts
		 */
		public void combine(Iterator<PactRecord> records, Collector out)
				throws Exception {
			reduce(records, out);
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
		FileDataSource orders = new FileDataSource(
			IntTupleDataInFormat.class, ordersPath, "Orders");
		orders.setParameter(TextInputFormat.RECORD_DELIMITER, "\n");
		orders.setDegreeOfParallelism(noSubtasks);
		//orders.setOutputContract(UniqueKey.class);
		orders.getCompilerHints().setAvgNumValuesPerKey(1);

		// create DataSourceContract for LineItems input
		FileDataSource customers = new FileDataSource(
			IntTupleDataInFormat.class, customerPath, "Customers");
		customers.setParameter(TextInputFormat.RECORD_DELIMITER, "\n");
		customers.setDegreeOfParallelism(noSubtasks);
		//customers.setOutputContract(UniqueKey.class);

		// create MapContract for filtering Orders tuples
		MapContract projectO = new MapContract(ProjectOrder.class, "ProjectO");
		projectO.setDegreeOfParallelism(noSubtasks);
		projectO.getCompilerHints().setAvgBytesPerRecord(5);
		projectO.getCompilerHints().setAvgNumValuesPerKey(10);

		// create MapContract for projecting LineItems tuples
		MapContract projectC = new MapContract(ProjectCust.class, "ProjectC");
		projectC.setDegreeOfParallelism(noSubtasks);
		projectC.getCompilerHints().setAvgNumValuesPerKey(1);
		projectC.getCompilerHints().setAvgBytesPerRecord(20);

		// create MatchContract for joining Orders and LineItems
		MatchContract joinCO = new MatchContract(JoinCO.class, PactInteger.class, 0, 0, "JoinCO");
		joinCO.setDegreeOfParallelism(noSubtasks);
		joinCO.getCompilerHints().setAvgBytesPerRecord(17);

		// create ReduceContract for aggregating the result
		ReduceContract aggCO = new ReduceContract(AggCO.class, PactString.class, 0, "AggCo");
		aggCO.setDegreeOfParallelism(noSubtasks);
		aggCO.getCompilerHints().setAvgBytesPerRecord(17);
		aggCO.getCompilerHints().setAvgNumValuesPerKey(1);

		// create DataSinkContract for writing the result
		FileDataSink result = new FileDataSink(StringIntOutFormat.class, output, "Output");
		result.setDegreeOfParallelism(noSubtasks);

		// assemble the PACT plan
		result.addInput(aggCO);
		aggCO.addInput(joinCO);
		joinCO.addFirstInput(projectO);
		projectO.addInput(orders);
		joinCO.addSecondInput(projectC);
		projectC.addInput(customers);

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
