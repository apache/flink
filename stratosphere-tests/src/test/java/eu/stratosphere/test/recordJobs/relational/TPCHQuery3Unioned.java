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
import eu.stratosphere.api.java.record.io.CsvInputFormat;
import eu.stratosphere.api.java.record.io.CsvOutputFormat;
import eu.stratosphere.api.java.record.operators.JoinOperator;
import eu.stratosphere.api.java.record.operators.MapOperator;
import eu.stratosphere.api.java.record.operators.ReduceOperator;
import eu.stratosphere.test.recordJobs.relational.TPCHQuery3.AggLiO;
import eu.stratosphere.test.recordJobs.relational.TPCHQuery3.FilterO;
import eu.stratosphere.test.recordJobs.relational.TPCHQuery3.JoinLiO;
import eu.stratosphere.types.DoubleValue;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.LongValue;
import eu.stratosphere.types.StringValue;

/**
 * The TPC-H is a decision support benchmark on relational data.
 * Its documentation and the data generator (DBGEN) can be found
 * on http://www.tpc.org/tpch/ .This implementation is tested with
 * the DB2 data format.  
 * THe PACT program implements a modified version of the query 3 of 
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
public class TPCHQuery3Unioned implements Program, ProgramDescription {

	private static final long serialVersionUID = 1L;

	@Override
	public Plan getPlan(final String... args) {
		// parse program parameters
		final int numSubtasks       = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		String orders1Path    = (args.length > 1 ? args[1] : "");
		String orders2Path    = (args.length > 2 ? args[2] : "");
		String partJoin1Path    = (args.length > 3 ? args[3] : "");
		String partJoin2Path    = (args.length > 4 ? args[4] : "");
		
		String lineitemsPath = (args.length > 5 ? args[5] : "");
		String output        = (args.length > 6 ? args[6] : "");

		// create DataSourceContract for Orders input
		FileDataSource orders1 = new FileDataSource(new CsvInputFormat(), orders1Path, "Orders 1");
		CsvInputFormat.configureRecordFormat(orders1)
			.recordDelimiter('\n')
			.fieldDelimiter('|')
			.field(LongValue.class, 0)		// order id
			.field(IntValue.class, 7) 		// ship prio
			.field(StringValue.class, 2, 2)	// order status
			.field(StringValue.class, 4, 10)	// order date
			.field(StringValue.class, 5, 8);	// order prio
		
		FileDataSource orders2 = new FileDataSource(new CsvInputFormat(), orders2Path, "Orders 2");
		CsvInputFormat.configureRecordFormat(orders2)
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
			.field(LongValue.class, 0)
			.field(DoubleValue.class, 5);

		// create MapOperator for filtering Orders tuples
		MapOperator filterO1 = MapOperator.builder(new FilterO())
			.name("FilterO")
			.input(orders1)
			.build();
		// filter configuration
		filterO1.setParameter(TPCHQuery3.YEAR_FILTER, 1993);
		filterO1.setParameter(TPCHQuery3.PRIO_FILTER, "5");
		filterO1.getCompilerHints().setFilterFactor(0.05f);
		
		// create MapOperator for filtering Orders tuples
		MapOperator filterO2 = MapOperator.builder(new FilterO())
			.name("FilterO")
			.input(orders2)
			.build();
		// filter configuration
		filterO2.setParameter(TPCHQuery3.YEAR_FILTER, 1993);
		filterO2.setParameter(TPCHQuery3.PRIO_FILTER, "5");

		// create JoinOperator for joining Orders and LineItems
		@SuppressWarnings("unchecked")
		JoinOperator joinLiO = JoinOperator.builder(new JoinLiO(), LongValue.class, 0, 0)
			.input1(filterO2, filterO1)
			.input2(lineitems)
			.name("JoinLiO")
			.build();
		
		FileDataSource partJoin1 = new FileDataSource(new CsvInputFormat(), partJoin1Path, "Part Join 1");
		CsvInputFormat.configureRecordFormat(partJoin1)
			.recordDelimiter('\n')
			.fieldDelimiter('|')
			.field(LongValue.class, 0)
			.field(IntValue.class, 1)
			.field(DoubleValue.class, 2);
		
		FileDataSource partJoin2 = new FileDataSource(new CsvInputFormat(), partJoin2Path, "Part Join 2");
		CsvInputFormat.configureRecordFormat(partJoin2)
			.recordDelimiter('\n')
			.fieldDelimiter('|')
			.field(LongValue.class, 0)
			.field(IntValue.class, 1)
			.field(DoubleValue.class, 2);
		
		// create ReduceOperator for aggregating the result
		// the reducer has a composite key, consisting of the fields 0 and 1
		@SuppressWarnings("unchecked")
		ReduceOperator aggLiO = ReduceOperator.builder(new AggLiO())
			.keyField(LongValue.class, 0)
			.keyField(StringValue.class, 1)
			.input(joinLiO, partJoin2, partJoin1)
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
		Plan plan = new Plan(result, "TPCH Q3 Unioned");
		plan.setDefaultParallelism(numSubtasks);
		return plan;
	}

	@Override
	public String getDescription() {
		return "Parameters: [numSubStasks], [orders1], [orders2], [partJoin1], [partJoin2], [lineitem], [output]";
	}
}
