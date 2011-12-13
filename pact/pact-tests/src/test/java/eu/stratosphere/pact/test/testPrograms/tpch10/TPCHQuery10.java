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

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Iterator;

import org.apache.log4j.Logger;

import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.contract.ReduceContract.Combinable;
import eu.stratosphere.pact.common.io.FileOutputFormat;
import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.example.relational.util.IntTupleDataInFormat;
import eu.stratosphere.pact.example.relational.util.Tuple;

/**
 * @author rico
 */
public class TPCHQuery10 implements PlanAssembler, PlanAssemblerDescription {

	private static Logger LOGGER = Logger.getLogger(TPCHQuery10.class);

	public static class FilterO extends MapStub {

		private static final int YEAR_FILTER = 1990;

		private Tuple tuple = new Tuple(); 
		
		@Override
		public void map(PactRecord record, Collector out) throws Exception {
			
			tuple = record.getField(1, tuple);
			if (Integer.parseInt(tuple.getStringValueAt(4).substring(0, 4)) > FilterO.YEAR_FILTER) {
				
				// project
				tuple.project(2); // o_custkey
				
				record.setField(1, tuple);
				
				out.collect(record);
			}
			
		}

	}

	//<PactInteger, Tuple, PactInteger, Tuple
	public static class FilterLI extends MapStub {

		private Tuple tuple = new Tuple();
		
		@Override
		public void map(PactRecord record, Collector out) throws Exception {
			tuple = record.getField(1, tuple);
			if (tuple.getStringValueAt(8).equals("R")) {
				tuple.project(96); // l_extendedprice, l_discount

				record.setField(1, tuple);
				out.collect(record);
			}
		}

	}

	public static class JoinOL extends MatchStub {

		private Tuple tuple = new Tuple();
		
		@Override
		public void match(PactRecord value1, PactRecord value2, Collector out)
				throws Exception {
			tuple = value1.getField(1, tuple);
			int newKey = Integer.parseInt(tuple.getStringValueAt(0));
			value2.setField(0, new PactInteger(newKey));
			out.collect(value2);
		}

	}

	public static class ProjectC extends MapStub {

		private Tuple tuple = new Tuple();

		@Override
		public void map(PactRecord record, Collector out) throws Exception {
			tuple = record.getField(1, tuple);
			tuple.project(190); // C_*: name,address,nationkey,phone,acctbal,comment
			record.setField(1, tuple);
			out.collect(record);
		}

	}

	public static class ProjectN extends MapStub {

		private Tuple tuple = new Tuple();
		
		@Override
		public void map(PactRecord record, Collector out) throws Exception {
			tuple = record.getField(1, tuple);
			tuple.project(2);// n_name
			record.setField(1, tuple);
			out.collect(record);
		}

	}

	public static class JoinCOL extends MatchStub {

		private Tuple cValue = new Tuple();
		private Tuple oValue = new Tuple();
		private PactInteger key = new PactInteger();

		@Override
		public void match(PactRecord value1, PactRecord value2, Collector out)
				throws Exception {
			
			key = value1.getField(0, key);
			cValue = value1.getField(1, cValue);
			oValue = value2.getField(1, oValue);
			
			int newKey = Integer.parseInt(cValue.getStringValueAt(2));
			cValue.project(59);
			cValue.addAttribute(key.toString());
			cValue.concatenate(oValue);

			key.setValue(newKey);
			value1.setField(0, key);
			value1.setField(1, cValue);
			
			out.collect(value1);
		}

	}

	public static class GroupKey extends Tuple implements Key {

		public GroupKey() {
			super();
		}
		
		public boolean equals(Object other) {
			return compareTo((Key) other) == 0;
		}
		
		public int hashCode() {
			//TODO replace with some light weight hash code
			return toString().hashCode();
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

	public static class JoinNCOL extends MatchStub {

		private Tuple cValue = new Tuple();
		private Tuple nValue = new Tuple();

		@Override
		public void match(PactRecord value1, PactRecord value2, Collector out)
				throws Exception {
			GroupKey oKey = new GroupKey();
			oKey.concatenate(value2.getField(1, nValue));
			oKey.concatenate(value1.getField(1, cValue));
			oKey.project(127);

			cValue.project(192);
			value2.setField(0, oKey);
			value2.setField(1, cValue);

			out.collect(value2);
		}
	}

	public static class TupleOutputFormat extends FileOutputFormat {

		
		private final StringBuilder buffer = new StringBuilder();
		private final GroupKey groupKey = new GroupKey();
		private final Tuple tuple = new Tuple();
		
		@Override
		public void writeRecord(PactRecord record) throws IOException {
			this.buffer.setLength(0);
			this.buffer.append(record.getField(0, groupKey).toString());
			this.buffer.append(record.getField(1, tuple).toString());
			this.buffer.append('\n');
			
			byte[] bytes = this.buffer.toString().getBytes();
			
			this.stream.write(bytes);
		}
	}

	@Combinable
	public static class Sum extends ReduceStub {

		private static final DecimalFormat FORMATTER = new DecimalFormat("#.####");
		static {
			DecimalFormatSymbols decimalFormatSymbol = new DecimalFormatSymbols();
			decimalFormatSymbol.setDecimalSeparator('.');
			FORMATTER.setDecimalFormatSymbols(decimalFormatSymbol);
		}

		@Override
		public void combine(Iterator<PactRecord> records, Collector out) throws Exception {
			reduce(records,out);
		}
		

		private Tuple v = new Tuple();
		private GroupKey key = new GroupKey();
		private PactRecord record = new PactRecord();

		@Override
		public void reduce(Iterator<PactRecord> records, Collector out)
				throws Exception {
			double sum = 0;
			while (records.hasNext()) {
				record = records.next();
				key = record.getField(0, key);
				v = record.getField(1, v);
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
			record.setField(1, summed);
			out.collect(record);
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
		FileDataSource orders = new FileDataSource(
			IntTupleDataInFormat.class, ordersPath, "Orders");
		orders.setParameter(TextInputFormat.RECORD_DELIMITER, "\n");
		orders.setDegreeOfParallelism(degreeOfParallelism);
		// orders.setOutputContract(UniqueKey.class);
		// orders.getCompilerHints().setAvgNumValuesPerKey(1);

		FileDataSource lineitems = new FileDataSource(
				IntTupleDataInFormat.class, lineitemsPath, "LineItems");
		lineitems.setParameter(TextInputFormat.RECORD_DELIMITER, "\n");
		lineitems.setDegreeOfParallelism(degreeOfParallelism);
		// lineitems.getCompilerHints().setAvgNumValuesPerKey(4);

		FileDataSource customers = new FileDataSource(
				IntTupleDataInFormat.class, customersPath, "Customers");
		customers.setParameter(TextInputFormat.RECORD_DELIMITER, "\n");
		customers.setDegreeOfParallelism(degreeOfParallelism);

		FileDataSource nations = new FileDataSource(
					IntTupleDataInFormat.class, nationsPath, "Nations");
		nations.setParameter(TextInputFormat.RECORD_DELIMITER, "\n");
		nations.setDegreeOfParallelism(degreeOfParallelism);

		MapContract mapO = new MapContract(FilterO.class, "FilterO");
		mapO.setDegreeOfParallelism(degreeOfParallelism);

		MapContract mapLi = new MapContract(FilterLI.class, "FilterLi");
		mapLi.setDegreeOfParallelism(degreeOfParallelism);

		MapContract projectC = new MapContract(ProjectC.class, "ProjectC");
		projectC.setDegreeOfParallelism(degreeOfParallelism);

		MapContract projectN = new MapContract(ProjectN.class, "ProjectN");
		projectN.setDegreeOfParallelism(degreeOfParallelism);

		MatchContract joinOL = new MatchContract(JoinOL.class, PactInteger.class, 0, 0, "JoinOL");
		joinOL.setDegreeOfParallelism(degreeOfParallelism);

		MatchContract joinCOL = new MatchContract(JoinCOL.class, PactInteger.class, 0, 0, "JoinCOL");
		joinCOL.setDegreeOfParallelism(degreeOfParallelism);

		MatchContract joinNCOL = new MatchContract(JoinNCOL.class, PactInteger.class, 0, 0, "JoinNCOL");
		joinNCOL.setDegreeOfParallelism(degreeOfParallelism);

		ReduceContract reduce = new ReduceContract(Sum.class, GroupKey.class, 0, "Reduce");
		reduce.setDegreeOfParallelism(degreeOfParallelism);

		FileDataSink result = new FileDataSink(TupleOutputFormat.class, resultPath, "Output");
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
