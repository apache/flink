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

import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.contract.ReduceContract.Combinable;
import eu.stratosphere.pact.common.io.RecordInputFormat;
import eu.stratosphere.pact.common.io.RecordOutputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation.AddSet;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantSet;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantSetFirst;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantSetSecond;
import eu.stratosphere.pact.common.stubs.StubAnnotation.OutCardBounds;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ReadSet;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ReadSetFirst;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ReadSetSecond;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantSet.ConstantSetMode;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.common.util.FieldSet;
import eu.stratosphere.pact.common.type.base.parser.DecimalTextIntParser;
import eu.stratosphere.pact.common.type.base.parser.VarLengthStringParser;

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
	 * Realizes the join between Customers and Order table.
	 */
	@ReadSetFirst(fields={})
	@ReadSetSecond(fields={})
	@ConstantSetFirst(fields={}, setMode=ConstantSetMode.Constant)
	@ConstantSetSecond(fields={0}, setMode=ConstantSetMode.Update)
	@AddSet(fields={})
	@OutCardBounds(lowerBound=1, upperBound=1)
	public static class JoinCO extends MatchStub {

		private final PactInteger oneInteger = new PactInteger(1);
		
		/**
		 * Output Schema:
		 *  Key: C_MKTSEGMENT
		 *  Value: 0:PARTIAL_COUNT=1
		 */
		@Override
		public void match(PactRecord order, PactRecord cust, Collector out)
				throws Exception {
			cust.setField(0, oneInteger);
			out.collect(cust);
		}
	}

	/**
	 * Reduce PACT implements the aggregation of the results. The 
	 * Combinable annotation is set as the partial counts can be calculated
	 * already in the combiner
	 *
	 */
	@Combinable
	@ReadSet(fields={0})
	@ConstantSet(fields={0}, setMode=ConstantSetMode.Update)
	@AddSet(fields={})
	@OutCardBounds(lowerBound=1, upperBound=1)
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
				count+=record.getField(0, integer).getValue();
			}

			integer.setValue(count);
			record.setField(0, integer);
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
		FileDataSource orders = new FileDataSource(RecordInputFormat.class, ordersPath, "Orders");
		orders.setDegreeOfParallelism(noSubtasks);
		orders.setParameter(RecordInputFormat.RECORD_DELIMITER, "\n");
		orders.setParameter(RecordInputFormat.FIELD_DELIMITER_PARAMETER, "|");
		orders.setParameter(RecordInputFormat.NUM_FIELDS_PARAMETER, 1);
		// cust id
		orders.getParameters().setClass(RecordInputFormat.FIELD_PARSER_PARAMETER_PREFIX+0, DecimalTextIntParser.class);
		orders.setParameter(RecordInputFormat.TEXT_POSITION_PARAMETER_PREFIX+0, 1);
		// compiler hints
		orders.getCompilerHints().setAvgBytesPerRecord(5);
//		orders.getCompilerHints().setAvgNumValuesPerKey(10);
		orders.getCompilerHints().setAvgNumValuesPerDistinctValue(new FieldSet(new int[]{0}), 10);
		
		// create DataSourceContract for Customer input
		FileDataSource customers = new FileDataSource(RecordInputFormat.class, customerPath, "Customers");
		customers.setDegreeOfParallelism(noSubtasks);
		customers.setParameter(RecordInputFormat.RECORD_DELIMITER, "\n");
		customers.setParameter(RecordInputFormat.FIELD_DELIMITER_PARAMETER, "|");
		customers.setParameter(RecordInputFormat.NUM_FIELDS_PARAMETER, 2);
		// cust id
		customers.getParameters().setClass(RecordInputFormat.FIELD_PARSER_PARAMETER_PREFIX+0, DecimalTextIntParser.class);
		customers.setParameter(RecordInputFormat.TEXT_POSITION_PARAMETER_PREFIX+0, 0);
		// market segment
		customers.getParameters().setClass(RecordInputFormat.FIELD_PARSER_PARAMETER_PREFIX+1, VarLengthStringParser.class);
		customers.setParameter(RecordInputFormat.TEXT_POSITION_PARAMETER_PREFIX+1, 6);
		// compiler hints
//		customers.getCompilerHints().setAvgNumValuesPerKey(1);
		customers.getCompilerHints().setAvgNumValuesPerDistinctValue(new FieldSet(new int[]{0}), 1);
		customers.getCompilerHints().setAvgBytesPerRecord(20);
		
		// create MatchContract for joining Orders and LineItems
		MatchContract joinCO = new MatchContract(JoinCO.class, PactInteger.class, 0, 0, "JoinCO");
		joinCO.setDegreeOfParallelism(noSubtasks);
		// compiler hints
		joinCO.getCompilerHints().setAvgBytesPerRecord(17);

		// create ReduceContract for aggregating the result
		ReduceContract aggCO = new ReduceContract(AggCO.class, PactString.class, 1, "AggCo");
		aggCO.setDegreeOfParallelism(noSubtasks);
		// compiler hints
		aggCO.getCompilerHints().setAvgBytesPerRecord(17);
		aggCO.getCompilerHints().setAvgNumValuesPerDistinctValue(new FieldSet(new int[]{0}), 1);

		// create DataSinkContract for writing the result
		FileDataSink result = new FileDataSink(RecordOutputFormat.class, output, "Output");
		result.setDegreeOfParallelism(noSubtasks);
		result.getParameters().setString(RecordOutputFormat.RECORD_DELIMITER_PARAMETER, "\n");
		result.getParameters().setString(RecordOutputFormat.FIELD_DELIMITER_PARAMETER, "|");
		result.getParameters().setInteger(RecordOutputFormat.NUM_FIELDS_PARAMETER, 2);
		result.getParameters().setClass(RecordOutputFormat.FIELD_TYPE_PARAMETER_PREFIX + 0, PactInteger.class);
		result.getParameters().setClass(RecordOutputFormat.FIELD_TYPE_PARAMETER_PREFIX + 1, PactString.class);

		// assemble the PACT plan
		result.addInput(aggCO);
		aggCO.addInput(joinCO);
		joinCO.addFirstInput(orders);
		joinCO.addSecondInput(customers);

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
