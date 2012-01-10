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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.contract.ReduceContract.Combinable;
import eu.stratosphere.pact.common.io.RecordInputFormat;
import eu.stratosphere.pact.common.io.RecordOutputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation.AddSet;
import eu.stratosphere.pact.common.stubs.StubAnnotation.OutCardBounds;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ReadSet;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ReadSetFirst;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ReadSetSecond;
import eu.stratosphere.pact.common.stubs.StubAnnotation.UpdateSet;
import eu.stratosphere.pact.common.stubs.StubAnnotation.UpdateSetFirst;
import eu.stratosphere.pact.common.stubs.StubAnnotation.UpdateSetSecond;
import eu.stratosphere.pact.common.stubs.StubAnnotation.UpdateSet.UpdateSetMode;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.common.type.base.parser.DecimalTextDoubleParser;
import eu.stratosphere.pact.common.type.base.parser.DecimalTextIntParser;
import eu.stratosphere.pact.common.type.base.parser.DecimalTextLongParser;
import eu.stratosphere.pact.common.type.base.parser.VarLengthStringParser;
import eu.stratosphere.pact.example.relational.util.Tuple;

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
public class TPCHQuery3 implements PlanAssembler, PlanAssemblerDescription {

	private static Log LOGGER = LogFactory.getLog(TPCHQuery3.class);
	
	public static final String YEAR_FILTER = "parameter.YEAR_FILTER";
	public static final String PRIO_FILTER = "parameter.PRIO_FILTER";

	/**
	 * Map PACT implements the selection and projection on the orders table.
	 * <p>
	 * The records enter the map as fields of type {@link Tuple}. Tuples are created by the
	 * input format in a very fast manner, without actual field parsing. They only index the
	 * delimiters. The parsed fields in the Pact Records are set after projection only
	 */
	@ReadSet(fields={2,3,4})
	@UpdateSet(fields={0,1}, setMode=UpdateSetMode.Constant)
	@AddSet(fields={})
	@OutCardBounds(lowerBound=0, upperBound=1)
	public static class FilterO extends MapStub
	{
		private String prioFilter;		// filter literal for the order priority
		private int yearFilter;			// filter literal for the year
		
		// reusable variables for the fields touched in the mapper
		private final PactString orderStatus = new PactString();
		private final PactString orderDate = new PactString();
		private final PactString orderPrio = new PactString();
		
		private final PactRecord outRecord = new PactRecord();
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
		public void map(final PactRecord record, final Collector out)
		{
			record.getFieldInto(2, orderStatus);
			record.getFieldInto(3, orderDate);
			record.getFieldInto(4, orderPrio);
			
			System.out.println("ODate<"+orderDate.getValue()+">");
			
			if (Integer.parseInt(orderDate.getValue().substring(0, 4)) > this.yearFilter
				&& orderStatus.getValue().equals("F") && orderPrio.getValue().startsWith(this.prioFilter))
			{
				outRecord.setField(0,record.getField(0, PactLong.class));
				outRecord.setField(1,record.getField(1, PactInteger.class));
	
				out.collect(outRecord);
				// Output Schema - 0:ORDERKEY, 1:SHIPPRIORITYfunction

			}
		}
	}


	/**
	 * Map PACT implements the projection on the LineItem table.
	 */
	public static class ProjectLi extends MapStub
	{
		// reusable objects for the touched fields
		
		private final PactLong orderKey = new PactLong();
		private final PactDouble extendedPrice = new PactDouble();
		private final PactRecord result = new PactRecord();
		
		/**
		 * Does the projection on the LineItem table 
		 *
		 * Output Schema - 0:ORDERKEY, 1:null, 2:EXTENDEDPRICE
		 */
		@Override
		public void map(PactRecord record, Collector out)
		{
			final Tuple t = record.getField(0, Tuple.class);
			
			try {
				this.orderKey.setValue(t.getLongValueAt(0));
				this.extendedPrice.setValue(Double.parseDouble(t.getStringValueAt(5)));
				
				result.setField(0, this.orderKey);
				result.setField(1, this.extendedPrice);
				
				out.collect(result);
			}
			catch (NumberFormatException nfe) {
				LOGGER.error(nfe);
			}
		}
	}

	/**
	 * Match PACT realizes the join between LineItem and Order table. The 
	 * SuperKey OutputContract is annotated because the new key is
	 * built of the keys of the inputs.
	 *
	 */
	@ReadSetFirst(fields={})
	@ReadSetSecond(fields={})
	@UpdateSetFirst(fields={}, setMode=UpdateSetMode.Update)
	@UpdateSetSecond(fields={}, setMode=UpdateSetMode.Constant)
	@AddSet(fields={})
	@OutCardBounds(lowerBound=1, upperBound=1)
	public static class JoinLiO extends MatchStub
	{
		/**
		 * Implements the join between LineItem and Order table on the 
		 * order key.
		 * 
		 * WHERE l_orderkey = o_orderkey
		 * 
		 * Output Schema - 0:ORDERKEY, 1:SHIPPRIORITY, 2:EXTENDEDPRICE
		 */
		@Override
		public void match(PactRecord first, PactRecord second, Collector out)
		{
			// we can simply union the fields since the first input has its fields on (0, 1) and the
			// second inputs has its fields in (0, 2). The conflicting field (0) is the key which is guaranteed
			// to be identical anyways 
			// <-- to do when union fields is implemented
			first.setField(2, second.getField(1, PactDouble.class));
			out.collect(first);
		}
	}

	/**
	 * Reduce PACT implements the aggregation of the results. The 
	 * Combinable annotation is set as the partial sums can be calculated
	 * already in the combiner
	 *
	 */
	@Combinable
	@ReadSet(fields={2})
	@UpdateSet(fields={2}, setMode=UpdateSetMode.Update)
	@AddSet(fields={})
	@OutCardBounds(lowerBound=1, upperBound=1)
	public static class AggLiO extends ReduceStub
	{
		private final PactDouble extendedPrice = new PactDouble();
		
		/**
		 * Does the aggregation of the query. 
		 * 
		 * sum(l_extendedprice) as revenue
		 * GROUP BY l_orderkey, o_shippriority;
		 * 
		 * Output Schema:
		 *  Key: ORDERKEY
		 *  Value: 0:ORDERKEY, 1:SHIPPRIORITY, 2:EXTENDEDPRICESUM
		 *
		 */
		@Override
		public void reduce(Iterator<PactRecord> values, Collector out)
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
		public void combine(Iterator<PactRecord> values, Collector out)
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
		String ordersPath    = (args.length > 1 ? args[1] : "");
		String lineitemsPath = (args.length > 2 ? args[2] : "");
		String output        = (args.length > 3 ? args[3] : "");

		// create DataSourceContract for Orders input
		FileDataSource orders = new FileDataSource(RecordInputFormat.class, ordersPath, "Orders");
		orders.setDegreeOfParallelism(noSubtasks);
		orders.setParameter(RecordInputFormat.RECORD_DELIMITER, "\n");
		orders.setParameter(RecordInputFormat.FIELD_DELIMITER_PARAMETER, "|");
		orders.setParameter(RecordInputFormat.NUM_FIELDS_PARAMETER, 5);
		// order id
		orders.getParameters().setClass(RecordInputFormat.FIELD_PARSER_PARAMETER_PREFIX+0, DecimalTextLongParser.class);
		orders.setParameter(RecordInputFormat.TEXT_POSITION_PARAMETER_PREFIX+0, 0);
		// ship prio
		orders.getParameters().setClass(RecordInputFormat.FIELD_PARSER_PARAMETER_PREFIX+1, DecimalTextIntParser.class);
		orders.setParameter(RecordInputFormat.TEXT_POSITION_PARAMETER_PREFIX+1, 7);
		// order status
		orders.getParameters().setClass(RecordInputFormat.FIELD_PARSER_PARAMETER_PREFIX+2, VarLengthStringParser.class);
		orders.setParameter(RecordInputFormat.TEXT_POSITION_PARAMETER_PREFIX+2, 2);
		// order date
		orders.getParameters().setClass(RecordInputFormat.FIELD_PARSER_PARAMETER_PREFIX+3, VarLengthStringParser.class);
		orders.setParameter(RecordInputFormat.TEXT_POSITION_PARAMETER_PREFIX+3, 4);
		// order prio
		orders.getParameters().setClass(RecordInputFormat.FIELD_PARSER_PARAMETER_PREFIX+4, VarLengthStringParser.class);
		orders.setParameter(RecordInputFormat.TEXT_POSITION_PARAMETER_PREFIX+4, 5);
		// compiler hints
		orders.getCompilerHints().setAvgNumValuesPerKey(1);
		orders.getCompilerHints().setAvgBytesPerRecord(16);

		// create DataSourceContract for LineItems input
		FileDataSource lineitems = new FileDataSource(RecordInputFormat.class, lineitemsPath, "LineItems");
		lineitems.setDegreeOfParallelism(noSubtasks);
		lineitems.setParameter(RecordInputFormat.RECORD_DELIMITER, "\n");
		lineitems.setParameter(RecordInputFormat.FIELD_DELIMITER_PARAMETER, "|");
		lineitems.setParameter(RecordInputFormat.NUM_FIELDS_PARAMETER, 2);
		// order id
		lineitems.getParameters().setClass(RecordInputFormat.FIELD_PARSER_PARAMETER_PREFIX+0, DecimalTextLongParser.class);
		lineitems.setParameter(RecordInputFormat.TEXT_POSITION_PARAMETER_PREFIX+0, 0);
		// extended price
		lineitems.getParameters().setClass(RecordInputFormat.FIELD_PARSER_PARAMETER_PREFIX+1, DecimalTextDoubleParser.class);
		lineitems.setParameter(RecordInputFormat.TEXT_POSITION_PARAMETER_PREFIX+1, 5);
		// compiler hints	
		lineitems.getCompilerHints().setAvgNumValuesPerKey(4);
		lineitems.getCompilerHints().setAvgBytesPerRecord(20);

		// create MapContract for filtering Orders tuples
		MapContract filterO = new MapContract(FilterO.class, orders, "FilterO");
		filterO.setDegreeOfParallelism(noSubtasks);
		// filter configuration
		filterO.setParameter(YEAR_FILTER, 1993);
		filterO.setParameter(PRIO_FILTER, "5");
		// compiler hints
		filterO.getCompilerHints().setAvgBytesPerRecord(16);
		filterO.getCompilerHints().setAvgRecordsEmittedPerStubCall(0.05f);
		filterO.getCompilerHints().setAvgNumValuesPerKey(1);

		// create MatchContract for joining Orders and LineItems
		MatchContract joinLiO = new MatchContract(JoinLiO.class, PactLong.class, 0, 0, filterO, lineitems, "JoinLiO");
		joinLiO.setDegreeOfParallelism(noSubtasks);
		// compiler hints
		joinLiO.getCompilerHints().setAvgBytesPerRecord(24);
		joinLiO.getCompilerHints().setAvgNumValuesPerKey(4);
		// fixing the strategy
		// TODO: remove
		joinLiO.setParameter("INPUT_LEFT_SHIP_STRATEGY", "SHIP_BROADCAST");
		joinLiO.setParameter("LOCAL_STRATEGY", "LOCAL_STRATEGY_HASH_BUILD_FIRST");

		// create ReduceContract for aggregating the result
		// the reducer has a composite key, consisting of the fields 0 and 1
		@SuppressWarnings("unchecked")
		ReduceContract aggLiO = new ReduceContract(AggLiO.class, new Class[] {PactLong.class, PactString.class}, new int[] {0, 1}, joinLiO, "AggLio");
		aggLiO.setDegreeOfParallelism(noSubtasks);
		// compiler hints
		aggLiO.getCompilerHints().setAvgBytesPerRecord(30);
		aggLiO.getCompilerHints().setAvgRecordsEmittedPerStubCall(1.0f);
		aggLiO.getCompilerHints().setAvgNumValuesPerKey(1);

		// create DataSinkContract for writing the result
		FileDataSink result = new FileDataSink(RecordOutputFormat.class, output, aggLiO, "Output");
		result.setDegreeOfParallelism(noSubtasks);
		result.getParameters().setString(RecordOutputFormat.RECORD_DELIMITER_PARAMETER, "\n");
		result.getParameters().setString(RecordOutputFormat.FIELD_DELIMITER_PARAMETER, "|");
		result.getParameters().setInteger(RecordOutputFormat.NUM_FIELDS_PARAMETER, 3);
		result.getParameters().setClass(RecordOutputFormat.FIELD_TYPE_PARAMETER_PREFIX + 0, PactLong.class);
		result.getParameters().setClass(RecordOutputFormat.FIELD_TYPE_PARAMETER_PREFIX + 1, PactInteger.class);
		result.getParameters().setClass(RecordOutputFormat.FIELD_TYPE_PARAMETER_PREFIX + 2, PactDouble.class);
		
		// assemble the PACT plan
		Plan plan = new Plan(result, "TPCH Q3");
		plan.setDefaultParallelism(noSubtasks);
		return plan;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getDescription() {
		return "Parameters: [noSubStasks], [orders], [lineitem], [output]";
	}

}
