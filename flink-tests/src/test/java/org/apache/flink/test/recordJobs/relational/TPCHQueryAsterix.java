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

import java.io.Serializable;
import java.util.Iterator;

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.Program;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.record.functions.JoinFunction;
import org.apache.flink.api.java.record.functions.ReduceFunction;
import org.apache.flink.api.java.record.functions.FunctionAnnotation.ConstantFields;
import org.apache.flink.api.java.record.functions.FunctionAnnotation.ConstantFieldsSecondExcept;
import org.apache.flink.api.java.record.io.CsvInputFormat;
import org.apache.flink.api.java.record.io.CsvOutputFormat;
import org.apache.flink.api.java.record.operators.FileDataSink;
import org.apache.flink.api.java.record.operators.FileDataSource;
import org.apache.flink.api.java.record.operators.JoinOperator;
import org.apache.flink.api.java.record.operators.ReduceOperator;
import org.apache.flink.api.java.record.operators.ReduceOperator.Combinable;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.Record;
import org.apache.flink.types.StringValue;
import org.apache.flink.util.Collector;

/**
 * The TPC-H is a decision support benchmark on relational data.
 * Its documentation and the data generator (DBGEN) can be found
 * on http://www.tpc.org/tpch/ .This implementation is tested with
 * the DB2 data format.  
 * 
 * This program implements a query on the TPC-H schema 
 * including one join and an aggregation.
 * This query is used as example in the Asterix project (http://asterix.ics.uci.edu/).
 * 
 * SELECT c_mktsegment, COUNT(o_orderkey)
 *   FROM orders, customer
 *   WHERE c_custkey = o_custkey
 * GROUP BY c_mktsegment;
 * 
 */
@SuppressWarnings("deprecation")
public class TPCHQueryAsterix implements Program, ProgramDescription {

	private static final long serialVersionUID = 1L;


	/**
	 * Realizes the join between Customers and Order table.
	 */
	@ConstantFieldsSecondExcept(0)
	public static class JoinCO extends JoinFunction implements Serializable {
		private static final long serialVersionUID = 1L;

		private final IntValue one = new IntValue(1);
		
		/**
		 * Output Schema:
		 *  0: PARTIAL_COUNT=1
		 *  1: C_MKTSEGMENT
		 */
		@Override
		public void join(Record order, Record cust, Collector<Record> out)
				throws Exception {
			cust.setField(0, one);
			out.collect(cust);
		}
	}

	/**
	 * Reduce implements the aggregation of the results. The 
	 * Combinable annotation is set as the partial counts can be calculated
	 * already in the combiner
	 *
	 */
	@Combinable
	@ConstantFields(1)
	public static class AggCO extends ReduceFunction implements Serializable {
		private static final long serialVersionUID = 1L;

		private final IntValue integer = new IntValue();
		private Record record = new Record();
	
		/**
		 * Output Schema:
		 * 0: COUNT
		 * 1: C_MKTSEGMENT
		 *
		 */
		@Override
		public void reduce(Iterator<Record> records, Collector<Record> out)
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
		public void combine(Iterator<Record> records, Collector<Record> out)
				throws Exception {
			reduce(records, out);
		}

	}


	@Override
	public Plan getPlan(final String... args) {

		// parse program parameters
		int numSubtasks       = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		String ordersPath    = (args.length > 1 ? args[1] : "");
		String customerPath  = (args.length > 2 ? args[2] : "");
		String output        = (args.length > 3 ? args[3] : "");

		/*
		 * Output Schema:
		 * 0: CUSTOMER_ID
		 */
		// create DataSourceContract for Orders input
		FileDataSource orders = new FileDataSource(new CsvInputFormat(), ordersPath, "Orders");
		orders.setParallelism(numSubtasks);
		CsvInputFormat.configureRecordFormat(orders)
			.recordDelimiter('\n')
			.fieldDelimiter('|')
			.field(IntValue.class, 1);
		
		/*
		 * Output Schema:
		 * 0: CUSTOMER_ID
		 * 1: MKT_SEGMENT
		 */
		// create DataSourceContract for Customer input
		FileDataSource customers = new FileDataSource(new CsvInputFormat(), customerPath, "Customers");
		customers.setParallelism(numSubtasks);
		CsvInputFormat.configureRecordFormat(customers)
			.recordDelimiter('\n')
			.fieldDelimiter('|')
			.field(IntValue.class, 0)
			.field(StringValue.class, 6);
		
		// create JoinOperator for joining Orders and LineItems
		JoinOperator joinCO = JoinOperator.builder(new JoinCO(), IntValue.class, 0, 0)
			.name("JoinCO")
			.build();
		joinCO.setParallelism(numSubtasks);

		// create ReduceOperator for aggregating the result
		ReduceOperator aggCO = ReduceOperator.builder(new AggCO(), StringValue.class, 1)
			.name("AggCo")
			.build();
		aggCO.setParallelism(numSubtasks);

		// create DataSinkContract for writing the result
		FileDataSink result = new FileDataSink(new CsvOutputFormat(), output, "Output");
		result.setParallelism(numSubtasks);
		CsvOutputFormat.configureRecordFormat(result)
			.recordDelimiter('\n')
			.fieldDelimiter('|')
			.field(IntValue.class, 0)
			.field(StringValue.class, 1);

		// assemble the plan
		result.setInput(aggCO);
		aggCO.setInput(joinCO);
		joinCO.setFirstInput(orders);
		joinCO.setSecondInput(customers);

		return new Plan(result, "TPCH Asterix");
	}


	@Override
	public String getDescription() {
		return "Parameters: [numSubStasks], [orders], [customer], [output]";
	}
}
