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
import eu.stratosphere.pact.common.contract.OutputContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.contract.ReduceContract.Combinable;
import eu.stratosphere.pact.common.io.RecordOutputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.example.relational.util.NewTupleInFormat;
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

	/**
	 * Map PACT implements the selection and projection on the orders table.
	 * <p>
	 * The records enter the map as fields of type {@link Tuple}. Tuples are created by the
	 * input format in a very fast manner, without actual field parsing. They only index the
	 * delimiters. The parsed fields in the Pact Records are set after projection only
	 */
	public static class FilterO extends MapStub
	{
		public static final String YEAR_FILTER = "parameter.YEAR_FILTER";
		public static final String PRIO_FILTER = "parameter.PRIO_FILTER";
		
		private String prioFilter;		// filter literal for the order priority
		private int yearFilter;			// filter literal for the year
		
		// reusable variables for the fields touched in the mapper
		private final PactLong orderKey = new PactLong();
		private final PactString shipPriority = new PactString();
		
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
			final Tuple t = record.getField(0, Tuple.class);
			
			try {
				if (Integer.parseInt(t.getStringValueAt(4).substring(0, 4)) > this.yearFilter
					&& t.getStringValueAt(2).equals("F") && t.getStringValueAt(5).startsWith(this.prioFilter))
				{
					// extract relevant values from tuple (projection)
					this.orderKey.setValue(t.getLongValueAt(0));
					this.shipPriority.setValue(t.getStringValueAt(7));

					// compose output
					record.setField(0, this.orderKey);
					record.setField(1, this.shipPriority);
					out.collect(record);

					// Output Schema - 0:ORDERKEY, 1:SHIPPRIORITY
				}
			}
			catch (final StringIndexOutOfBoundsException e) {
				LOGGER.error(e);
			}
			catch (final Exception ex) {
				LOGGER.error(ex);
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
				
				record.setField(0, this.orderKey);
				record.setField(2, this.extendedPrice);
				
				out.collect(record);
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
	@OutputContract.AllConstant
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
			first.unionFields(second);
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
		
		// ====================================================================
		//    Alternative Signature Variants
		// ====================================================================
		
		public void reduce(Key[] keys, Iterator<PactRecord> values, Collector out)
		{
			PactRecord rec = null;
			double partExtendedPriceSum = 0;

			while (values.hasNext()) {
				rec = values.next();
				partExtendedPriceSum += rec.getField(2, PactDouble.class).getValue();
			}

			this.extendedPrice.setValue(partExtendedPriceSum);
			rec.setField(0, keys[0]);
			rec.setField(1, keys[1]);
			rec.setField(2, this.extendedPrice);
			out.collect(rec);
		}
		
		public void reduce(PactRecord keys, Iterator<PactRecord> values, Collector out)
		{
			double partExtendedPriceSum = 0;

			while (values.hasNext()) {
				final PactRecord rec = values.next();
				partExtendedPriceSum += rec.getField(2, PactDouble.class).getValue();
			}

			this.extendedPrice.setValue(partExtendedPriceSum);
			keys.setField(2, this.extendedPrice);
			out.collect(keys);
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
		String lineitemsPath = (args.length > 2 ? args[2] : "");
		String output        = (args.length > 3 ? args[3] : "");

		// create DataSourceContract for Orders input
		FileDataSource orders = new FileDataSource(NewTupleInFormat.class, ordersPath, "Orders");
		orders.setParameter(NewTupleInFormat.RECORD_DELIMITER, "\n");
		orders.getCompilerHints().setAvgNumValuesPerKey(1);

		// create DataSourceContract for LineItems input
		FileDataSource lineitems = new FileDataSource(NewTupleInFormat.class, lineitemsPath, "LineItems");
		lineitems.setParameter(NewTupleInFormat.RECORD_DELIMITER, "\n");
		lineitems.getCompilerHints().setAvgNumValuesPerKey(4);

		// create MapContract for filtering Orders tuples
		MapContract filterO = new MapContract(FilterO.class, orders, "FilterO");
		filterO.setParameter("YEAR_FILTER", 1993);
		filterO.setParameter("PRIO_FILTER", "5");
		filterO.getCompilerHints().setAvgBytesPerRecord(16);
		filterO.getCompilerHints().setAvgRecordsEmittedPerStubCall(0.05f);
		filterO.getCompilerHints().setAvgNumValuesPerKey(1);

		// create MapContract for projecting LineItems tuples
		MapContract projectLi = new MapContract(ProjectLi.class, lineitems, "ProjectLi");
		projectLi.getCompilerHints().setAvgBytesPerRecord(20);
		projectLi.getCompilerHints().setAvgRecordsEmittedPerStubCall(1.0f);
		projectLi.getCompilerHints().setAvgNumValuesPerKey(4);

		// create MatchContract for joining Orders and LineItems
		MatchContract joinLiO = new MatchContract(JoinLiO.class, PactLong.class, 0, 0, filterO, projectLi, "JoinLiO");
		joinLiO.getCompilerHints().setAvgBytesPerRecord(24);
		joinLiO.getCompilerHints().setAvgNumValuesPerKey(4);

		// create ReduceContract for aggregating the result
		// the reducer has a composite key, consisting of the fields 0 and 1
		@SuppressWarnings("unchecked")
		ReduceContract aggLiO = new ReduceContract(AggLiO.class, new Class[] {PactLong.class, PactString.class}, new int[] {0, 1}, joinLiO, "AggLio");
		aggLiO.getCompilerHints().setAvgBytesPerRecord(30);
		aggLiO.getCompilerHints().setAvgRecordsEmittedPerStubCall(1.0f);
		aggLiO.getCompilerHints().setAvgNumValuesPerKey(1);

		// create DataSinkContract for writing the result
		FileDataSink result = new FileDataSink(RecordOutputFormat.class, output, aggLiO, "Output");
		result.getParameters().setString(RecordOutputFormat.RECORD_DELIMITER_PARAMETER, "\n");
		result.getParameters().setString(RecordOutputFormat.FIELD_DELIMITER_PARAMETER, "|");
		result.getParameters().setInteger(RecordOutputFormat.NUM_FIELDS_PARAMETER, 3);
		result.getParameters().setClass(RecordOutputFormat.FIELD_TYPE_PARAMETER_PREFIX + 0, PactLong.class);
		result.getParameters().setClass(RecordOutputFormat.FIELD_TYPE_PARAMETER_PREFIX + 1, PactString.class);
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
