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

package eu.stratosphere.pact.test.testPrograms.tpch10;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Iterator;

import org.apache.log4j.Logger;

import eu.stratosphere.pact.common.contract.FileDataSinkContract;
import eu.stratosphere.pact.common.contract.FileDataSourceContract;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.contract.ReduceContract.Combinable;
import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.io.TextOutputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MapStub;
import eu.stratosphere.pact.common.stub.MatchStub;
import eu.stratosphere.pact.common.stub.ReduceStub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.example.relational.util.IntTupleDataInFormat;
import eu.stratosphere.pact.example.relational.util.Tuple;

/**
 * @author rico
 */
public class TPCHQuery10 implements PlanAssembler, PlanAssemblerDescription {

	private static Logger LOGGER = Logger.getLogger(TPCHQuery10.class);

	public static class FilterO extends MapStub<PactInteger, Tuple, PactInteger, Tuple> {

		private static final int YEAR_FILTER = 1990;

		@Override
		public void map(PactInteger key, Tuple value, Collector<PactInteger, Tuple> out) {

			if (Integer.parseInt(value.getStringValueAt(4).substring(0, 4)) > FilterO.YEAR_FILTER) {

				// project
				value.project(2); // o_custkey

				out.collect(key, value);

			}
		}

	}

	public static class FilterLI extends MapStub<PactInteger, Tuple, PactInteger, Tuple> {

		@Override
		public void map(PactInteger key, Tuple value, Collector<PactInteger, Tuple> out) {
			if (value.getStringValueAt(8).equals("R")) {
				value.project(96); // l_extendedprice, l_discount

				out.collect(key, value);
			}
		}

	}

	public static class JoinOL extends MatchStub<PactInteger, Tuple, Tuple, PactInteger, Tuple> {

		@Override
		public void match(PactInteger key, Tuple oValue, Tuple liValue, Collector<PactInteger, Tuple> out) {
			int newKey = Integer.parseInt(oValue.getStringValueAt(0));
			out.collect(new PactInteger(newKey), liValue);
		}

	}

	public static class ProjectC extends MapStub<PactInteger, Tuple, PactInteger, Tuple> {

		@Override
		public void map(PactInteger key, Tuple value, Collector<PactInteger, Tuple> out) {
			value.project(190); // C_*: name,address,nationkey,phone,acctbal,comment
			out.collect(key, value);
		}

	}

	public static class ProjectN extends MapStub<PactInteger, Tuple, PactInteger, Tuple> {

		@Override
		public void map(PactInteger key, Tuple value, Collector<PactInteger, Tuple> out) {
			value.project(2);// n_name
			out.collect(key, value);
		}

	}

	public static class JoinCOL extends MatchStub<PactInteger, Tuple, Tuple, PactInteger, Tuple> {

		@Override
		public void match(PactInteger key, Tuple cValue, Tuple oValue, Collector<PactInteger, Tuple> out) {
			int newKey = Integer.parseInt(cValue.getStringValueAt(2));
			cValue.project(59);
			cValue.addAttribute(key.toString());
			cValue.concatenate(oValue);
			out.collect(new PactInteger(newKey), cValue);
		}

	}

	public static class GroupKey extends Tuple implements Key {

		public GroupKey() {
			super();
		}

		@Override
		public int compareTo(Key o) {

			int custKey = Integer.parseInt(this.getStringValueAt(6));
			Tuple other = (Tuple) o;
			int toCompareTo = Integer.parseInt(other.getStringValueAt(6));
			if (custKey == toCompareTo) {

				for (int i = 1; i < 6; i++) {
					if (!(this.getStringValueAt(i).equals(other.getStringValueAt(i)))) {
						return this.getStringValueAt(i).compareTo(other.getStringValueAt(i));
					}
				}
				return 0;
			} else {
				return custKey - toCompareTo;
			}
		}

	}

	public static class JoinNCOL extends MatchStub<PactInteger, Tuple, Tuple, GroupKey, Tuple> {

		@Override
		public void match(PactInteger key, Tuple cValue, Tuple nValue, Collector<GroupKey, Tuple> out) {
			GroupKey oKey = new GroupKey();
			oKey.concatenate(nValue);
			oKey.concatenate(cValue);
			oKey.project(127);

			cValue.project(192);

			out.collect(oKey, cValue);
		}
	}

	public static class TupleOutputFormat extends TextOutputFormat<GroupKey, Tuple> {

		@Override
		public byte[] writeLine(KeyValuePair<GroupKey, Tuple> pair) {
			return (pair.getKey().toString() + pair.getValue().toString() + "\n").getBytes();
		}

	}

	@Combinable
	public static class Sum extends ReduceStub<GroupKey, Tuple, GroupKey, Tuple> {

		private static final DecimalFormat FORMATTER = new DecimalFormat("#.####");
		static {
			DecimalFormatSymbols decimalFormatSymbol = new DecimalFormatSymbols();
			decimalFormatSymbol.setDecimalSeparator('.');
			FORMATTER.setDecimalFormatSymbols(decimalFormatSymbol);
		}

		@Override
		public void combine(GroupKey key, Iterator<Tuple> values, Collector<GroupKey, Tuple> out) {
			reduce(key, values, out);
		}

		@Override
		public void reduce(GroupKey key, Iterator<Tuple> values, Collector<GroupKey, Tuple> out) {
			double sum = 0;
			while (values.hasNext()) {
				Tuple v = values.next();
				if (v.getNumberOfColumns() > 1) {
					long val = Math.round(Double.parseDouble(v.getStringValueAt(0))
						* (1 - Double.parseDouble(v.getStringValueAt(1))) * 10000);
					sum += (((double) val) / 10000d);

				} else {
					sum += Double.parseDouble(v.getStringValueAt(0));
				}
			}
			Tuple summed = new Tuple();
			summed.addAttribute(FORMATTER.format(sum));

			LOGGER.info("Output: " + key);
			out.collect(key, summed);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see eu.stratosphere.pact.common.plan.PlanAssemblerDescription#getDescription()
	 */
	@Override
	public String getDescription() {
		// TODO Auto-generated method stub
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see eu.stratosphere.pact.common.plan.PlanAssembler#getPlan(java.lang.String[])
	 */
	@Override
	public Plan getPlan(String... args) throws IllegalArgumentException {
		int degreeOfParallelism = 1;
		String ordersPath = "";
		String lineitemsPath = "";
		String customersPath = "";
		String nationsPath = "";
		String resultPath = "";

		if (args.length != 6)
			LOGGER.warn("number of arguments do not match!");
		else {
			degreeOfParallelism = Integer.parseInt(args[0]);
			ordersPath = args[1];
			lineitemsPath = args[2];
			customersPath = args[3];
			nationsPath = args[4];
			resultPath = args[5];
		}

		FileDataSourceContract<PactInteger, Tuple> orders = new FileDataSourceContract<PactInteger, Tuple>(
			IntTupleDataInFormat.class, ordersPath, "Orders");
		orders.setParameter(TextInputFormat.RECORD_DELIMITER, "\n");
		orders.setDegreeOfParallelism(degreeOfParallelism);
		// orders.setOutputContract(UniqueKey.class);
		// orders.getCompilerHints().setAvgNumValuesPerKey(1);

		FileDataSourceContract<PactInteger, Tuple> lineitems = new FileDataSourceContract<PactInteger, Tuple>(
				IntTupleDataInFormat.class, lineitemsPath, "LineItems");
		lineitems.setParameter(TextInputFormat.RECORD_DELIMITER, "\n");
		lineitems.setDegreeOfParallelism(degreeOfParallelism);
		// lineitems.getCompilerHints().setAvgNumValuesPerKey(4);

		FileDataSourceContract<PactInteger, Tuple> customers = new FileDataSourceContract<PactInteger, Tuple>(
				IntTupleDataInFormat.class, customersPath, "Customers");
		customers.setParameter(TextInputFormat.RECORD_DELIMITER, "\n");
		customers.setDegreeOfParallelism(degreeOfParallelism);

		FileDataSourceContract<PactInteger, Tuple> nations = new FileDataSourceContract<PactInteger, Tuple>(
					IntTupleDataInFormat.class, nationsPath, "Nations");
		nations.setParameter(TextInputFormat.RECORD_DELIMITER, "\n");
		nations.setDegreeOfParallelism(degreeOfParallelism);

		MapContract<PactInteger, Tuple, PactInteger, Tuple> mapO = new MapContract<PactInteger, Tuple, PactInteger, Tuple>(
			FilterO.class, "FilterO");
		mapO.setDegreeOfParallelism(degreeOfParallelism);

		MapContract<PactInteger, Tuple, PactInteger, Tuple> mapLi = new MapContract<PactInteger, Tuple, PactInteger, Tuple>(
			FilterLI.class, "FilterLi");
		mapLi.setDegreeOfParallelism(degreeOfParallelism);

		MapContract<PactInteger, Tuple, PactInteger, Tuple> projectC = new MapContract<PactInteger, Tuple, PactInteger, Tuple>(
			ProjectC.class, "ProjectC");
		projectC.setDegreeOfParallelism(degreeOfParallelism);

		MapContract<PactInteger, Tuple, PactInteger, Tuple> projectN = new MapContract<PactInteger, Tuple, PactInteger, Tuple>(
			ProjectN.class, "ProjectN");
		projectN.setDegreeOfParallelism(degreeOfParallelism);

		MatchContract<PactInteger, Tuple, Tuple, PactInteger, Tuple> joinOL = new MatchContract<PactInteger, Tuple, Tuple, PactInteger, Tuple>(
			JoinOL.class, "JoinOL");
		joinOL.setDegreeOfParallelism(degreeOfParallelism);

		MatchContract<PactInteger, Tuple, Tuple, PactInteger, Tuple> joinCOL = new MatchContract<PactInteger, Tuple, Tuple, PactInteger, Tuple>(
			JoinCOL.class, "JoinCOL");
		joinCOL.setDegreeOfParallelism(degreeOfParallelism);

		MatchContract<PactInteger, Tuple, Tuple, GroupKey, Tuple> joinNCOL = new MatchContract<PactInteger, Tuple, Tuple, GroupKey, Tuple>(
			JoinNCOL.class, "JoinNCOL");
		joinNCOL.setDegreeOfParallelism(degreeOfParallelism);

		ReduceContract<GroupKey, Tuple, GroupKey, Tuple> reduce = new ReduceContract<TPCHQuery10.GroupKey, Tuple, TPCHQuery10.GroupKey, Tuple>(
			Sum.class, "Reduce");
		reduce.setDegreeOfParallelism(degreeOfParallelism);

		FileDataSinkContract<GroupKey, Tuple> result = new FileDataSinkContract<GroupKey, Tuple>(
				TupleOutputFormat.class, resultPath, "Output");
		result.setDegreeOfParallelism(degreeOfParallelism);

		result.setInput(reduce);
		reduce.setInput(joinNCOL);
		joinNCOL.setFirstInput(joinCOL);
		joinNCOL.setSecondInput(projectN);
		joinCOL.setFirstInput(projectC);
		joinCOL.setSecondInput(joinOL);
		joinOL.setFirstInput(mapO);
		joinOL.setSecondInput(mapLi);
		projectC.setInput(customers);
		projectN.setInput(nations);
		mapLi.setInput(lineitems);
		mapO.setInput(orders);

		// return the PACT plan
		return new Plan(result, "TPCH Q10");
	}
}
