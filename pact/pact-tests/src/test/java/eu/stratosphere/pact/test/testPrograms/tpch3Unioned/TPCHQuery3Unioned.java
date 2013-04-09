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

package eu.stratosphere.pact.test.testPrograms.tpch3Unioned;

import java.util.Iterator;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.io.RecordInputFormat;
import eu.stratosphere.pact.common.io.RecordOutputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFields;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFieldsFirstExcept;
import eu.stratosphere.pact.common.stubs.StubAnnotation.OutCardBounds;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.common.type.base.parser.DecimalTextDoubleParser;
import eu.stratosphere.pact.common.type.base.parser.DecimalTextIntParser;
import eu.stratosphere.pact.common.type.base.parser.DecimalTextLongParser;
import eu.stratosphere.pact.common.type.base.parser.VarLengthStringParser;
import eu.stratosphere.pact.common.util.FieldSet;

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
public class TPCHQuery3Unioned implements PlanAssembler, PlanAssemblerDescription {

	public static final String YEAR_FILTER = "parameter.YEAR_FILTER";
	public static final String PRIO_FILTER = "parameter.PRIO_FILTER";

	/**
	 * Map PACT implements the selection and projection on the orders table.
	 */
	@ConstantFields(fields={0,1})
	@OutCardBounds(upperBound=1, lowerBound=0)
	public static class FilterO extends MapStub
	{
		private String prioFilter;		// filter literal for the order priority
		private int yearFilter;			// filter literal for the year
		
		// reusable variables for the fields touched in the mapper
		private PactString orderStatus;
		private PactString orderDate;
		private PactString orderPrio;
		
		/**
		 * Reads the filter literals from the configuration.
		 * 
		 * @see eu.stratosphere.pact.common.stubs.Stub#open(eu.stratosphere.nephele.configuration.Configuration)
		 */
		@Override
		public void open(Configuration parameters) {
			this.yearFilter = parameters.getInteger(YEAR_FILTER, 1990);
			this.prioFilter = parameters.getString(PRIO_FILTER, "0");
		}
	
		/**
		 * Filters the orders table by year, orderstatus and orderpriority.
		 *
		 *  o_orderstatus = "X" 
		 *  AND YEAR(o_orderdate) > Y
		 *  AND o_orderpriority LIKE "Z"
	 	 *  
	 	 * Output Schema - 0:ORDERKEY, 1:SHIPPRIORITY
		 */
		@Override
		public void map(final PactRecord record, final Collector<PactRecord> out)
		{
			
			orderStatus = record.getField(2, PactString.class);
			if (!orderStatus.getValue().equals("F"))
				return;
			
			orderPrio = record.getField(4, PactString.class);
			if(!orderPrio.getValue().startsWith(this.prioFilter))
				return;
			
			orderDate = record.getField(3, PactString.class);
			if (!(Integer.parseInt(orderDate.getValue().substring(0, 4)) > this.yearFilter))
				return;
	
			out.collect(record);
		}
	}

	/**
	 * Match PACT realizes the join between LineItem and Order table. The 
	 * SuperKey OutputContract is annotated because the new key is
	 * built of the keys of the inputs.
	 *
	 */
	@ConstantFieldsFirstExcept(fields={2})
	@OutCardBounds(upperBound=1, lowerBound=1)
	public static class JoinLiO extends MatchStub
	{
		/**
		 * Implements the join between LineItem and Order table on the order key.
		 * 
		 * Output Schema - 0:ORDERKEY, 1:SHIPPRIORITY, 2:EXTENDEDPRICE
		 */
		@Override
		public void match(PactRecord order, PactRecord lineitem, Collector<PactRecord> out)
		{
			order.setField(2, lineitem.getField(1, PactDouble.class));
			out.collect(order);
			
			System.out.println("ID: " + order.getField(0, PactLong.class).getValue());
			System.out.println("Prio: " + order.getField(1, PactInteger.class).getValue());
			System.out.println("ExtPrice: " + order.getField(2, PactDouble.class).getValue());
		}
	}

	/**
	 * Reduce PACT implements the sum aggregation. 
	 * The Combinable annotation is set as the partial sums can be calculated
	 * already in the combiner
	 *
	 * Output Schema - 0:ORDERKEY, 1:SHIPPRIORITY, 2:SUM(EXTENDEDPRICE)
	 */
	@ReduceContract.Combinable
	@ConstantFields(fields={0,1})
	@OutCardBounds(upperBound=1, lowerBound=1)
	public static class AggLiO extends ReduceStub
	{
		private final PactDouble extendedPrice = new PactDouble();
		
		@Override
		public void reduce(Iterator<PactRecord> values, Collector<PactRecord> out)
		{
			PactRecord rec = null;
			double partExtendedPriceSum = 0;

			while (values.hasNext()) {
				rec = values.next();
				partExtendedPriceSum += rec.getField(2, PactDouble.class).getValue();
			}

			this.extendedPrice.setValue(partExtendedPriceSum);
			rec.setField(2, this.extendedPrice);
			out.collect(rec);
		}

		/**
		 * Creates partial sums on the price attribute for each data batch.
		 */
		@Override
		public void combine(Iterator<PactRecord> values, Collector<PactRecord> out)
		{
			reduce(values, out);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Plan getPlan(final String... args) 
	{
		// parse program parameters
		int noSubtasks       = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		String orders1Path    = (args.length > 1 ? args[1] : "");
		String orders2Path    = (args.length > 2 ? args[2] : "");
		String partJoin1Path    = (args.length > 3 ? args[3] : "");
		String partJoin2Path    = (args.length > 4 ? args[4] : "");
		
		String lineitemsPath = (args.length > 5 ? args[5] : "");
		String output        = (args.length > 6 ? args[6] : "");

		// create DataSourceContract for Orders input
		FileDataSource orders1 = new FileDataSource(RecordInputFormat.class, orders1Path, "Orders 1");
		orders1.setDegreeOfParallelism(noSubtasks);
		RecordInputFormat.configureRecordFormat(orders1)
			.recordDelimiter('\n')
			.fieldDelimiter('|')
			.field(DecimalTextLongParser.class, 0)
			.field(DecimalTextIntParser.class, 7)
			.field(VarLengthStringParser.class, 2)
			.field(VarLengthStringParser.class, 4)
			.field(VarLengthStringParser.class, 5);
			
		// compiler hints
		orders1.getCompilerHints().setAvgNumRecordsPerDistinctFields(new FieldSet(0), 1);
		orders1.getCompilerHints().setAvgBytesPerRecord(16);
		orders1.getCompilerHints().addUniqueField(0);

		
		FileDataSource orders2 = new FileDataSource(RecordInputFormat.class, orders2Path, "Orders 2");
		orders2.setDegreeOfParallelism(noSubtasks);
		RecordInputFormat.configureRecordFormat(orders2)
			.recordDelimiter('\n')
			.fieldDelimiter('|')
			.field(DecimalTextLongParser.class, 0)
			.field(DecimalTextIntParser.class, 7)
			.field(VarLengthStringParser.class, 2)
			.field(VarLengthStringParser.class, 4)
			.field(VarLengthStringParser.class, 5);
		
		// compiler hints
		orders2.getCompilerHints().setAvgNumRecordsPerDistinctFields(new FieldSet(0), 1);
		orders2.getCompilerHints().setAvgBytesPerRecord(16);
		orders2.getCompilerHints().addUniqueField(0);
		
		
		// create DataSourceContract for LineItems input
		FileDataSource lineitems = new FileDataSource(RecordInputFormat.class, lineitemsPath, "LineItems");
		lineitems.setDegreeOfParallelism(noSubtasks);
		RecordInputFormat.configureRecordFormat(lineitems)
			.recordDelimiter('\n')
			.fieldDelimiter('|')
			.field(DecimalTextLongParser.class, 0)
			.field(DecimalTextDoubleParser.class, 5);
		
		// compiler hints	
		lineitems.getCompilerHints().setAvgNumRecordsPerDistinctFields(new FieldSet(0), 4);
		lineitems.getCompilerHints().setAvgBytesPerRecord(20);

		// create MapContract for filtering Orders tuples
		MapContract filterO1 = MapContract.builder(FilterO.class)
			.name("FilterO")
			.build();
		filterO1.addInput(orders1);
		filterO1.setDegreeOfParallelism(noSubtasks);
		// filter configuration
		filterO1.setParameter(YEAR_FILTER, 1993);
		filterO1.setParameter(PRIO_FILTER, "5");
		// compiler hints
		filterO1.getCompilerHints().setAvgBytesPerRecord(16);
		filterO1.getCompilerHints().setAvgRecordsEmittedPerStubCall(0.05f);
		filterO1.getCompilerHints().setAvgNumRecordsPerDistinctFields(new FieldSet(0), 1);
		
		// create MapContract for filtering Orders tuples
		MapContract filterO2 = MapContract.builder(FilterO.class)
			.name("FilterO")
			.build();
		filterO2.addInput(orders2);
		filterO2.setDegreeOfParallelism(noSubtasks);
		// filter configuration
		filterO2.setParameter(YEAR_FILTER, 1993);
		filterO2.setParameter(PRIO_FILTER, "5");
		// compiler hints
		filterO2.getCompilerHints().setAvgBytesPerRecord(16);
		filterO2.getCompilerHints().setAvgRecordsEmittedPerStubCall(1.0f);
		filterO2.getCompilerHints().setAvgNumRecordsPerDistinctFields(new FieldSet(0), 1);

		// create MatchContract for joining Orders and LineItems
		MatchContract joinLiO = MatchContract.builder(JoinLiO.class, PactLong.class, 0, 0)
			.input1(filterO2)
			.input2(lineitems)
			.name("JoinLiO")
			.build();
		joinLiO.addFirstInput(filterO1);
		joinLiO.setDegreeOfParallelism(noSubtasks);
		// compiler hints
		joinLiO.getCompilerHints().setAvgBytesPerRecord(24);
		joinLiO.getCompilerHints().setAvgNumRecordsPerDistinctFields(new FieldSet(new int[]{0, 1}), 4);

		
		FileDataSource partJoin1 = new FileDataSource(RecordInputFormat.class, partJoin1Path, "Part Join 1");
		partJoin1.setDegreeOfParallelism(noSubtasks);
		RecordInputFormat.configureRecordFormat(partJoin1)
			.recordDelimiter('\n')
			.fieldDelimiter('|')
			.field(DecimalTextLongParser.class, 0)
			.field(DecimalTextIntParser.class, 1)
			.field(DecimalTextDoubleParser.class, 2);
		
		FileDataSource partJoin2 = new FileDataSource(RecordInputFormat.class, partJoin2Path, "Part Join 2");
		partJoin2.setDegreeOfParallelism(noSubtasks);
		RecordInputFormat.configureRecordFormat(partJoin2)
			.recordDelimiter('\n')
			.fieldDelimiter('|')
			.field(DecimalTextLongParser.class, 0)
			.field(DecimalTextIntParser.class, 1)
			.field(DecimalTextDoubleParser.class, 2);
		
		// create ReduceContract for aggregating the result
		// the reducer has a composite key, consisting of the fields 0 and 1
		ReduceContract aggLiO = ReduceContract.builder(AggLiO.class)
			.keyField(PactLong.class, 0)
			.keyField(PactString.class, 1)
			.input(joinLiO)
			.name("AggLio")
			.build();
		aggLiO.addInput(partJoin2);
		aggLiO.addInput(partJoin1);
		aggLiO.setDegreeOfParallelism(noSubtasks);
		// compiler hints
		aggLiO.getCompilerHints().setAvgBytesPerRecord(30);
		aggLiO.getCompilerHints().setAvgRecordsEmittedPerStubCall(1.0f);
		aggLiO.getCompilerHints().setAvgNumRecordsPerDistinctFields(new FieldSet(new int[]{0, 1}), 1);

		// create DataSinkContract for writing the result
		FileDataSink result = new FileDataSink(RecordOutputFormat.class, output, aggLiO, "Output");
		result.setDegreeOfParallelism(noSubtasks);
		RecordOutputFormat.configureRecordFormat(result)
			.recordDelimiter('\n')
			.fieldDelimiter('|')
			.lenient(true)
			.field(PactLong.class, 0)
			.field(PactInteger.class, 1)
			.field(PactDouble.class, 2);
		
		// assemble the PACT plan
		Plan plan = new Plan(result, "TPCH Q3 Unioned");
		plan.setDefaultParallelism(noSubtasks);
		return plan;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getDescription() {
		return "Parameters: [noSubStasks], [orders1], [orders2], [partJoin1], [partJoin2], [lineitem], [output]";
	}

}
