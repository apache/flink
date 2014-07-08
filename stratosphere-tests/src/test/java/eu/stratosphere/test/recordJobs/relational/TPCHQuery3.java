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

import java.io.Serializable;
import java.util.Iterator;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.Program;
import eu.stratosphere.api.common.ProgramDescription;
import eu.stratosphere.api.java.record.operators.FileDataSink;
import eu.stratosphere.api.java.record.operators.FileDataSource;
import eu.stratosphere.api.java.record.functions.FunctionAnnotation.ConstantFields;
import eu.stratosphere.api.java.record.functions.FunctionAnnotation.ConstantFieldsFirst;
import eu.stratosphere.api.java.record.functions.JoinFunction;
import eu.stratosphere.api.java.record.functions.MapFunction;
import eu.stratosphere.api.java.record.functions.ReduceFunction;
import eu.stratosphere.api.java.record.io.CsvInputFormat;
import eu.stratosphere.api.java.record.io.CsvOutputFormat;
import eu.stratosphere.api.java.record.operators.JoinOperator;
import eu.stratosphere.api.java.record.operators.MapOperator;
import eu.stratosphere.api.java.record.operators.ReduceOperator;
import eu.stratosphere.api.java.record.operators.ReduceOperator.Combinable;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.types.DoubleValue;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.LongValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.Collector;

/**
 * The TPC-H is a decision support benchmark on relational data.
 * Its documentation and the data generator (DBGEN) can be found
 * on http://www.tpc.org/tpch/ .This implementation is tested with
 * the DB2 data format.  
 * 
 * This program implements a modified version of the query 3 of 
 * the TPC-H benchmark including one join, some filtering and an
 * aggregation.
 * 
 * SELECT l_orderkey, o_shippriority, sum(l_extendedprice) as revenue
 *   FROM orders, lineitem
 *   WHERE l_orderkey = o_orderkey
 *     AND o_orderstatus = "X" 
 *     AND YEAR(o_orderdate) > Y
 *     AND o_orderpriority LIKE "Z%"
 * GROUP BY l_orderkey, o_shippriority;
 */
public class TPCHQuery3 implements Program, ProgramDescription {

	private static final long serialVersionUID = 1L;
	
	public static final String YEAR_FILTER = "parameter.YEAR_FILTER";
	public static final String PRIO_FILTER = "parameter.PRIO_FILTER";

	/**
	 * Map PACT implements the selection and projection on the orders table.
	 */
	@ConstantFields({0,1})
	public static class FilterO extends MapFunction implements Serializable {
		private static final long serialVersionUID = 1L;
		
		private String prioFilter;		// filter literal for the order priority
		private int yearFilter;			// filter literal for the year
		
		// reusable objects for the fields touched in the mapper
		private StringValue orderStatus;
		private StringValue orderDate;
		private StringValue orderPrio;
		
		/**
		 * Reads the filter literals from the configuration.
		 * 
		 * @see eu.stratosphere.api.common.functions.Function#open(eu.stratosphere.configuration.Configuration)
		 */
		@Override
		public void open(Configuration parameters) {
			this.yearFilter = parameters.getInteger(YEAR_FILTER, 1990);
			this.prioFilter = parameters.getString(PRIO_FILTER, "0");
		}
	
		/**
		 * Filters the orders table by year, order status and order priority.
		 *
		 *  o_orderstatus = "X" 
		 *  AND YEAR(o_orderdate) > Y
		 *  AND o_orderpriority LIKE "Z"
		 *  
		 * Output Schema: 
		 *   0:ORDERKEY, 
		 *   1:SHIPPRIORITY
		 */
		@Override
		public void map(final Record record, final Collector<Record> out) {
			orderStatus = record.getField(2, StringValue.class);
			if (!orderStatus.getValue().equals("F")) {
				return;
			}
			
			orderPrio = record.getField(4, StringValue.class);
			if(!orderPrio.getValue().startsWith(this.prioFilter)) {
				return;
			}
			
			orderDate = record.getField(3, StringValue.class);
			if (!(Integer.parseInt(orderDate.getValue().substring(0, 4)) > this.yearFilter)) {
				return;
			}
			
			record.setNumFields(2);
			out.collect(record);
		}
	}

	/**
	 * Match PACT realizes the join between LineItem and Order table.
	 *
	 */
	@ConstantFieldsFirst({0,1})
	public static class JoinLiO extends JoinFunction implements Serializable {
		private static final long serialVersionUID = 1L;
		
		/**
		 * Implements the join between LineItem and Order table on the order key.
		 * 
		 * Output Schema:
		 *   0:ORDERKEY
		 *   1:SHIPPRIORITY
		 *   2:EXTENDEDPRICE
		 */
		@Override
		public void join(Record order, Record lineitem, Collector<Record> out) {
			order.setField(2, lineitem.getField(1, DoubleValue.class));
			out.collect(order);
		}
	}

	/**
	 * Reduce PACT implements the sum aggregation. 
	 * The Combinable annotation is set as the partial sums can be calculated
	 * already in the combiner
	 *
	 */
	@Combinable
	@ConstantFields({0,1})
	public static class AggLiO extends ReduceFunction implements Serializable {
		private static final long serialVersionUID = 1L;
		
		private final DoubleValue extendedPrice = new DoubleValue();
		
		/**
		 * Implements the sum aggregation.
		 * 
		 * Output Schema:
		 *   0:ORDERKEY
		 *   1:SHIPPRIORITY
		 *   2:SUM(EXTENDEDPRICE)
		 */
		@Override
		public void reduce(Iterator<Record> values, Collector<Record> out) {
			Record rec = null;
			double partExtendedPriceSum = 0;

			while (values.hasNext()) {
				rec = values.next();
				partExtendedPriceSum += rec.getField(2, DoubleValue.class).getValue();
			}

			this.extendedPrice.setValue(partExtendedPriceSum);
			rec.setField(2, this.extendedPrice);
			out.collect(rec);
		}

		/**
		 * Creates partial sums on the price attribute for each data batch.
		 */
		@Override
		public void combine(Iterator<Record> values, Collector<Record> out) {
			reduce(values, out);
		}
	}


	@Override
	public Plan getPlan(final String... args) {
		// parse program parameters
		final int numSubtasks       = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		final String ordersPath    = (args.length > 1 ? args[1] : "");
		final String lineitemsPath = (args.length > 2 ? args[2] : "");
		final String output        = (args.length > 3 ? args[3] : "");

		// create DataSourceContract for Orders input
		FileDataSource orders = new FileDataSource(new CsvInputFormat(), ordersPath, "Orders");
		CsvInputFormat.configureRecordFormat(orders)
			.recordDelimiter('\n')
			.fieldDelimiter('|')
			.field(LongValue.class, 0)		// order id
			.field(IntValue.class, 7) 		// ship prio
			.field(StringValue.class, 2, 2)	// order status
			.field(StringValue.class, 4, 10)	// order date
			.field(StringValue.class, 5, 8);	// order prio

		// create DataSourceContract for LineItems input
		FileDataSource lineitems = new FileDataSource(new CsvInputFormat(), lineitemsPath, "LineItems");
		CsvInputFormat.configureRecordFormat(lineitems)
			.recordDelimiter('\n')
			.fieldDelimiter('|')
			.field(LongValue.class, 0)		// order id
			.field(DoubleValue.class, 5);	// extended price

		// create MapOperator for filtering Orders tuples
		MapOperator filterO = MapOperator.builder(new FilterO())
			.input(orders)
			.name("FilterO")
			.build();
		// filter configuration
		filterO.setParameter(YEAR_FILTER, 1993);
		filterO.setParameter(PRIO_FILTER, "5");
		// compiler hints
		filterO.getCompilerHints().setFilterFactor(0.05f);

		// create JoinOperator for joining Orders and LineItems
		JoinOperator joinLiO = JoinOperator.builder(new JoinLiO(), LongValue.class, 0, 0)
			.input1(filterO)
			.input2(lineitems)
			.name("JoinLiO")
			.build();

		// create ReduceOperator for aggregating the result
		// the reducer has a composite key, consisting of the fields 0 and 1
		ReduceOperator aggLiO = ReduceOperator.builder(new AggLiO())
			.keyField(LongValue.class, 0)
			.keyField(StringValue.class, 1)
			.input(joinLiO)
			.name("AggLio")
			.build();

		// create DataSinkContract for writing the result
		FileDataSink result = new FileDataSink(new CsvOutputFormat(), output, aggLiO, "Output");
		CsvOutputFormat.configureRecordFormat(result)
			.recordDelimiter('\n')
			.fieldDelimiter('|')
			.lenient(true)
			.field(LongValue.class, 0)
			.field(IntValue.class, 1)
			.field(DoubleValue.class, 2);
		
		// assemble the PACT plan
		Plan plan = new Plan(result, "TPCH Q3");
		plan.setDefaultParallelism(numSubtasks);
		return plan;
	}


	@Override
	public String getDescription() {
		return "Parameters: [numSubStasks], [orders], [lineitem], [output]";
	}
}
