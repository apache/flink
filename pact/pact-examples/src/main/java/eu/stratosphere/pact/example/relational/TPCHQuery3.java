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

import org.apache.log4j.Logger;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.contract.FileDataSinkContract;
import eu.stratosphere.pact.common.contract.FileDataSourceContract;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.contract.OutputContract.SameKey;
import eu.stratosphere.pact.common.contract.OutputContract.SuperKey;
import eu.stratosphere.pact.common.contract.OutputContract.UniqueKey;
import eu.stratosphere.pact.common.contract.ReduceContract.Combinable;
import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MapStub;
import eu.stratosphere.pact.common.stub.MatchStub;
import eu.stratosphere.pact.common.stub.ReduceStub;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactPair;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.example.relational.util.IntTupleDataInFormat;
import eu.stratosphere.pact.example.relational.util.StringTupleDataOutFormat;
import eu.stratosphere.pact.example.relational.util.Tuple;

/**
 * The TPC-H is a decision support benchmark on relational data.
 * Its documentation and the data generator (DBGEN) can be found
 * on http://www.tpc.org/tpch/ .This implementation is tested with
 * the DB2 data format.
 * THe PACT program implements a modified version of the query 3 of
 * the TPC-H benchmark including one join, some filtering and an
 * aggregation.
 * SELECT l_orderkey, o_shippriority, sum(l_extendedprice) as revenue
 *   FROM orders, lineitem
 *   WHERE l_orderkey = o_orderkey
 *     AND o_orderstatus = "X"
 *     AND YEAR(o_orderdate) > Y
 *     AND o_orderpriority LIKE "Z%"
 *   GROUP BY l_orderkey, o_shippriority;
 */
public class TPCHQuery3 implements PlanAssembler, PlanAssemblerDescription {

	private static Logger LOGGER = Logger.getLogger(TPCHQuery3.class);

	/**
	 * Concatenation of Integer and String. Used for concatenation of keys
	 * after join (ORDERKEY, SHIPPRIORITY)
	 */
	public static class N_IntStringPair extends PactPair<PactInteger, PactString> {

		/**
		 * Initializes a blank pair. Required for deserialization
		 */
		public N_IntStringPair() {
			super();
		}

		/**
		 * Initializes the concatenation of integer and string.
		 * 
		 * @param first
		 *        Integer value for concatenating
		 * @param second
		 *        String value for concatenating
		 */
		public N_IntStringPair(PactInteger first, PactString second) {
			super(first, second);
		}
	}

	/**
	 * Map PACT implements the filter on the orders table. The SameKey
	 * OutputContract is annotated because the key does not change during
	 * filtering.
	 */
	@SameKey
	public static class FilterO extends MapStub<PactInteger, Tuple, PactInteger, Tuple> {

		private int yearFilter;

		private String prioFilter;

		@Override
		public void configure(Configuration parameters) {
			this.yearFilter = parameters.getInteger("YEAR_FILTER", 1990);
			this.prioFilter = parameters.getString("PRIO_FILTER", "0");
		}

		/**
		 * Filters the orders table by year, orderstatus and orderpriority
		 * 
		 * o_orderstatus = "X"
		 *   AND YEAR(o_orderdate) > Y
		 *   AND o_orderpriority LIKE "Z"
		 * 
		 * Output Schema:
		 *   Key: ORDERKEY
		 *   Value: 0:ORDERKEY, 1:SHIPPRIORITY
		 */
		@Override
		public void map(final PactInteger oKey, final Tuple value, final Collector<PactInteger, Tuple> out) {

			try {
				if (Integer.parseInt(value.getStringValueAt(4).substring(0, 4)) > this.yearFilter
					&& value.getStringValueAt(2).equals("F") && value.getStringValueAt(5).startsWith(this.prioFilter)) {

					// project
					value.project(129);

					out.collect(oKey, value);

					// Output Schema:
					// KEY: ORDERKEY
					// VALUE: 0:ORDERKEY, 1:SHIPPRIORITY

				}
			} catch (final StringIndexOutOfBoundsException e) {
				LOGGER.error(e);
			} catch (final Exception ex) {
				LOGGER.error(ex);
			}
		}
	}

	/**
	 * Map PACT implements the projection on the LineItem table. The SameKey
	 * OutputContract is annotated because the key does not change during
	 * projection.
	 */
	@SameKey
	public static class ProjectLi extends MapStub<PactInteger, Tuple, PactInteger, Tuple> {

		/**
		 * Does the projection on the LineItem table
		 * 
		 * Output Schema:
		 *   Key: ORDERKEY
		 *   Value: 0:ORDERKEY, 1:EXTENDEDPRICE
		 */
		@Override
		public void map(PactInteger oKey, Tuple value, Collector<PactInteger, Tuple> out) {
			value.project(33);
			out.collect(oKey, value);
		}
	}

	/**
	 * Match PACT realizes the join between LineItem and Order table. The
	 * SuperKey OutputContract is annotated because the new key is
	 * built of the keys of the inputs.
	 */
	@SuperKey
	public static class JoinLiO extends MatchStub<PactInteger, Tuple, Tuple, N_IntStringPair, Tuple> {

		/**
		 * Implements the join between LineItem and Order table on the
		 * order key.
		 * 
		 * WHERE l_orderkey = o_orderkey
		 * 
		 * Output Schema:
		 *   Key: ORDERKEY, SHIPPRIORITY
		 *   Value: 0:ORDERKEY, 1:SHIPPRIORITY, 2:EXTENDEDPRICE
		 */
		@Override
		public void match(PactInteger oKey, Tuple oVal, Tuple liVal, Collector<N_IntStringPair, Tuple> out) {

			oVal.concatenate(liVal);
			oVal.project(11);

			N_IntStringPair superKey = new N_IntStringPair(oKey, new PactString(oVal.getStringValueAt(1)));

			out.collect(superKey, oVal);
		}
	}

	/**
	 * Reduce PACT implements the aggregation of the results. The
	 * Combinable annotation is set as the partial sums can be calculated
	 * already in the combiner
	 */
	@Combinable
	public static class AggLiO extends ReduceStub<N_IntStringPair, Tuple, PactInteger, Tuple> {

		/**
		 * Does the aggregation of the query.
		 * 
		 * sum(l_extendedprice) as revenue
		 * GROUP BY l_orderkey, o_shippriority;
		 * 
		 * Output Schema:
		 *   Key: ORDERKEY
		 *   Value: 0:ORDERKEY, 1:SHIPPRIORITY, 2:EXTENDEDPRICESUM
		 */
		@Override
		public void reduce(N_IntStringPair oKeyShipPrio, Iterator<Tuple> values, Collector<PactInteger, Tuple> out) {

			long partExtendedPriceSum = 0;

			Tuple value = null;
			while (values.hasNext()) {
				value = values.next();
				partExtendedPriceSum += ((long) Double.parseDouble(value.getStringValueAt(2)));
			}

			if (value != null) {
				value.project(3);
				value.addAttribute(partExtendedPriceSum + "");

				out.collect(oKeyShipPrio.getFirst(), value);
			}

		}

		/**
		 * Creates partial sums on the price attribute for each data batch
		 */
		@Override
		public void combine(N_IntStringPair oKeyShipPrio, Iterator<Tuple> values, Collector<N_IntStringPair, Tuple> out) {

			long partExtendedPriceSum = 0;

			Tuple value = null;
			while (values.hasNext()) {
				value = values.next();
				partExtendedPriceSum += ((long) Double.parseDouble(value.getStringValueAt(2)));
			}

			if (value != null) {
				value.project(3);
				value.addAttribute(partExtendedPriceSum + "");

				out.collect(oKeyShipPrio, value);
			}

		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Plan getPlan(final String... args) {

		// parse program parameters
		int noSubtasks = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		String ordersPath = (args.length > 1 ? args[1] : "");
		String lineitemsPath = (args.length > 2 ? args[2] : "");
		String output = (args.length > 3 ? args[3] : "");

		// create DataSourceContract for Orders input
		FileDataSourceContract<PactInteger, Tuple> orders = new FileDataSourceContract<PactInteger, Tuple>(
			IntTupleDataInFormat.class, ordersPath, "Orders");
		orders.setParameter(TextInputFormat.RECORD_DELIMITER, "\n");
		orders.setDegreeOfParallelism(noSubtasks);
		orders.setOutputContract(UniqueKey.class);
		// set compiler hints
		orders.getCompilerHints().setAvgNumValuesPerKey(1);

		// create DataSourceContract for LineItems input
		FileDataSourceContract<PactInteger, Tuple> lineitems = new FileDataSourceContract<PactInteger, Tuple>(
			IntTupleDataInFormat.class, lineitemsPath, "LineItems");
		lineitems.setParameter(TextInputFormat.RECORD_DELIMITER, "\n");
		lineitems.setDegreeOfParallelism(noSubtasks);
		// set compiler hints
		lineitems.getCompilerHints().setAvgNumValuesPerKey(4);

		// create MapContract for filtering Orders tuples
		MapContract<PactInteger, Tuple, PactInteger, Tuple> filterO = new MapContract<PactInteger, Tuple, PactInteger, Tuple>(
			FilterO.class, "FilterO");
		filterO.setDegreeOfParallelism(noSubtasks);
		// set stub parameters
		filterO.setParameter("YEAR_FILTER", 1993);
		filterO.setParameter("PRIO_FILTER", "5");
		// set compiler hints
		filterO.getCompilerHints().setAvgBytesPerRecord(32);
		filterO.getCompilerHints().setAvgRecordsEmittedPerStubCall(0.05f);
		filterO.getCompilerHints().setAvgNumValuesPerKey(1);

		// create MapContract for projecting LineItems tuples
		MapContract<PactInteger, Tuple, PactInteger, Tuple> projectLi = new MapContract<PactInteger, Tuple, PactInteger, Tuple>(
			ProjectLi.class, "ProjectLi");
		projectLi.setDegreeOfParallelism(noSubtasks);
		// set compiler hints
		projectLi.getCompilerHints().setAvgBytesPerRecord(48);
		projectLi.getCompilerHints().setAvgRecordsEmittedPerStubCall(1.0f);
		projectLi.getCompilerHints().setAvgNumValuesPerKey(4);

		// create MatchContract for joining Orders and LineItems
		MatchContract<PactInteger, Tuple, Tuple, N_IntStringPair, Tuple> joinLiO = new MatchContract<PactInteger, Tuple, Tuple, N_IntStringPair, Tuple>(
			JoinLiO.class, "JoinLiO");
		joinLiO.setDegreeOfParallelism(noSubtasks);
		// set compiler hints
		joinLiO.getCompilerHints().setAvgBytesPerRecord(64);
		joinLiO.getCompilerHints().setAvgNumValuesPerKey(4);

		// create ReduceContract for aggregating the result
		ReduceContract<N_IntStringPair, Tuple, PactInteger, Tuple> aggLiO = new ReduceContract<N_IntStringPair, Tuple, PactInteger, Tuple>(
			AggLiO.class, "AggLio");
		aggLiO.setDegreeOfParallelism(noSubtasks);
		// set compiler hints
		aggLiO.getCompilerHints().setAvgBytesPerRecord(64);
		aggLiO.getCompilerHints().setAvgRecordsEmittedPerStubCall(1.0f);
		aggLiO.getCompilerHints().setAvgNumValuesPerKey(1);

		// create DataSinkContract for writing the result
		FileDataSinkContract<PactString, Tuple> result = new FileDataSinkContract<PactString, Tuple>(
			StringTupleDataOutFormat.class, output, "Output");
		result.setDegreeOfParallelism(noSubtasks);

		// assemble the PACT plan
		result.setInput(aggLiO);
		aggLiO.setInput(joinLiO);
		joinLiO.setFirstInput(filterO);
		filterO.setInput(orders);
		joinLiO.setSecondInput(projectLi);
		projectLi.setInput(lineitems);

		Plan plan = new Plan(result, "TPCH Q3");

		// return the PACT plan
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
