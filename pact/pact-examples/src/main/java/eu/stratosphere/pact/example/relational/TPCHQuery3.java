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

import eu.stratosphere.pact.common.contract.DataSinkContract;
import eu.stratosphere.pact.common.contract.DataSourceContract;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.contract.OutputContract.SameKey;
import eu.stratosphere.pact.common.contract.OutputContract.SuperKey;
import eu.stratosphere.pact.common.contract.OutputContract.UniqueKey;
import eu.stratosphere.pact.common.contract.ReduceContract.Combinable;
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

public class TPCHQuery3 implements PlanAssembler, PlanAssemblerDescription {

	/**
	 * Implements a modified query 3 of the TPC-H benchmark.
	 * SELECT l_orderkey, o_shippriority, sum(l_extendedprice) as revenue
	 * FROM orders, lineitem
	 * WHERE l_orderkey = o_orderkey
	 * AND o_custkey IN [X]
	 * AND o_orderdate > [Y]
	 * GROUP BY l_orderkey, o_shippriority;
	 */

	public static class N_IntStringPair extends PactPair<PactInteger, PactString> {

		public N_IntStringPair() {
			super();
		}

		public N_IntStringPair(PactInteger first, PactString second) {
			super(first, second);
		}
	}

	@SameKey
	public static class FilterO extends MapStub<PactInteger, Tuple, PactInteger, Tuple> {

		private final int YEAR_FILTER = 1993;

		private final String PRIO_FILTER = "5";

		@Override
		public void map(PactInteger oKey, Tuple value, Collector<PactInteger, Tuple> out) {

			if ((Integer.parseInt(value.getStringValueAt(4).substring(0, 4)) > YEAR_FILTER)
				&& (value.getStringValueAt(2).equals("F")) && (value.getStringValueAt(5).startsWith(PRIO_FILTER))) {

				// project
				value.project(129);

				out.collect(oKey, value);

				// Output Schema:
				// KEY: ORDERKEY
				// VALUE: 0:ORDERKEY, 1:SHIPPRIORITY

			}
		}
	}

	@SameKey
	public static class ProjectLi extends MapStub<PactInteger, Tuple, PactInteger, Tuple> {

		@Override
		public void map(PactInteger oKey, Tuple value, Collector<PactInteger, Tuple> out) {
			value.project(33);
			out.collect(oKey, value);

			// Output Schema:
			// Key: ORDERKEY
			// Value: 0:ORDERKEY, 1:EXTENDEDPRICE
		}
	}

	@SuperKey
	public static class JoinLiO extends MatchStub<PactInteger, Tuple, Tuple, N_IntStringPair, Tuple> {

		@Override
		public void match(PactInteger oKey, Tuple oVal, Tuple liVal, Collector<N_IntStringPair, Tuple> out) {

			oVal.concatenate(liVal);
			oVal.project(11);

			N_IntStringPair superKey = new N_IntStringPair(oKey, new PactString(oVal.getStringValueAt(1)));

			out.collect(superKey, oVal);

			// Output Schema:
			// KEY: ORDERKEY, SHIPPRIORITY
			// VALUE: 0:ORDERKEY, 1:SHIPPRIORITY, 2:EXTENDEDPRICE
		}
	}

	@Combinable
	public static class AggLiO extends ReduceStub<N_IntStringPair, Tuple, PactInteger, Tuple> {

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

			// Output Schema:
			// KEY: ORDERKEY
			// VALUE: 0:ORDERKEY, 1:SHIPPRIORITY, 2:EXTENDEDPRICESUM
		}

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

			// Output Schema:
			// KEY: ORDERKEY, SHIPPRIORITY
			// VALUE: 0:ORDERKEY, 1:SHIPPRIORITY, 2:EXTENDEDPRICESUM
		}
	}

	@Override
	public Plan getPlan(String... args) {

		// check for the correct number of job parameters
		if (args.length != 4) {
			throw new IllegalArgumentException(
				"Must provide four arguments: <parallelism> <orders_input> <lineitem_input> <result_directory>");
		}

		int degreeOfParallelism = Integer.parseInt(args[0]);
		String ordersPath = args[1];
		String lineitemsPath = args[2];
		String resultPath = args[3];

		DataSourceContract<PactInteger, Tuple> orders = new DataSourceContract<PactInteger, Tuple>(
			IntTupleDataInFormat.class, ordersPath, "Orders");
		orders.setFormatParameter("delimiter", "\n");
		orders.setDegreeOfParallelism(degreeOfParallelism);
		orders.setOutputContract(UniqueKey.class);
		orders.getCompilerHints().setAvgNumValuesPerKey(1);

		DataSourceContract<PactInteger, Tuple> lineitems = new DataSourceContract<PactInteger, Tuple>(
			IntTupleDataInFormat.class, lineitemsPath, "LineItems");
		lineitems.setFormatParameter("delimiter", "\n");
		lineitems.setDegreeOfParallelism(degreeOfParallelism);
		lineitems.getCompilerHints().setAvgNumValuesPerKey(4);

		MapContract<PactInteger, Tuple, PactInteger, Tuple> filterO = new MapContract<PactInteger, Tuple, PactInteger, Tuple>(
			FilterO.class, "FilterO");
		filterO.setDegreeOfParallelism(degreeOfParallelism);
		filterO.getCompilerHints().setAvgBytesPerRecord(32);
		filterO.getCompilerHints().setSelectivity(0.05f);
		filterO.getCompilerHints().setAvgNumValuesPerKey(1);

		MapContract<PactInteger, Tuple, PactInteger, Tuple> projectLi = new MapContract<PactInteger, Tuple, PactInteger, Tuple>(
			ProjectLi.class, "ProjectLi");
		projectLi.setDegreeOfParallelism(degreeOfParallelism);
		projectLi.getCompilerHints().setAvgBytesPerRecord(48);
		projectLi.getCompilerHints().setSelectivity(1.0f);
		projectLi.getCompilerHints().setAvgNumValuesPerKey(4);

		MatchContract<PactInteger, Tuple, Tuple, N_IntStringPair, Tuple> joinLiO = new MatchContract<PactInteger, Tuple, Tuple, N_IntStringPair, Tuple>(
			JoinLiO.class, "JoinLiO");
		joinLiO.setDegreeOfParallelism(degreeOfParallelism);
		joinLiO.getCompilerHints().setSelectivity(0.05f);
		joinLiO.getCompilerHints().setAvgBytesPerRecord(64);
		joinLiO.getCompilerHints().setAvgNumValuesPerKey(4);

		ReduceContract<N_IntStringPair, Tuple, PactInteger, Tuple> aggLiO = new ReduceContract<N_IntStringPair, Tuple, PactInteger, Tuple>(
			AggLiO.class, "AggLio");
		aggLiO.setDegreeOfParallelism(degreeOfParallelism);
		aggLiO.getCompilerHints().setAvgBytesPerRecord(64);
		aggLiO.getCompilerHints().setSelectivity(0.25f);
		aggLiO.getCompilerHints().setAvgNumValuesPerKey(1);

		DataSinkContract<PactString, Tuple> result = new DataSinkContract<PactString, Tuple>(
			StringTupleDataOutFormat.class, resultPath, "Output");
		result.setDegreeOfParallelism(degreeOfParallelism);

		result.setInput(aggLiO);
		aggLiO.setInput(joinLiO);
		joinLiO.setFirstInput(filterO);
		filterO.setInput(orders);
		joinLiO.setSecondInput(projectLi);
		projectLi.setInput(lineitems);

		return new Plan(result, "TPCH Q3");
	}

	@Override
	public String getDescription() {
		return "Parameters: dop, orders-input, lineitem-input, result";
	}

}
