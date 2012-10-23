/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.test.testPrograms.util.IntTupleDataInFormat;
import eu.stratosphere.pact.test.testPrograms.util.Tuple;

/**
 * @author rico
 * @author Matthias Ringwald
 * @author Stephan Ewen
 */
public class TPCHQuery10 implements PlanAssembler, PlanAssemblerDescription
{
	// --------------------------------------------------------------------------------------------
	//                         Local Filters and Projections
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Forwards (0 = orderkey, 1 = custkey).
	 */
	public static class FilterO extends MapStub
	{
		private static final int YEAR_FILTER = 1990;
		
		private final PactInteger custKey = new PactInteger();
		
		@Override
		public void map(PactRecord record, Collector<PactRecord> out) throws Exception {
			
			Tuple t = record.getField(1, Tuple.class);
			if (Integer.parseInt(t.getStringValueAt(4).substring(0, 4)) > FilterO.YEAR_FILTER) {
				// project
				this.custKey.setValue((int) t.getLongValueAt(1));
				record.setField(1, this.custKey);
				out.collect(record);
			}
			
		}
	}

	/**
	 * Forwards (0 = lineitem, 1 = tuple (extendedprice, discount) )
	 */
	public static class FilterLI extends MapStub
	{
		private final Tuple tuple = new Tuple();
		
		@Override
		public void map(PactRecord record, Collector<PactRecord> out) throws Exception
		{
			Tuple t = record.getField(1, this.tuple);
			if (t.getStringValueAt(8).equals("R")) {
				t.project(0x60); // l_extendedprice, l_discount
				record.setField(1, t);
				out.collect(record);
			}
		}
	}
	
	/**
	 * Returns (0 = custkey, 1 = custName, 2 = NULL, 3 = balance, 4 = nationkey, 5 = address, 6 = phone, 7 = comment)
	 */
	public static class ProjectC extends MapStub {

		private final Tuple tuple = new Tuple();
		
		private final PactString custName = new PactString();
		
		private final PactString balance = new PactString();
		private final PactInteger nationKey = new PactInteger();
		private final PactString address = new PactString();
		private final PactString phone = new PactString();
		private final PactString comment = new PactString();
		
		@Override
		public void map(PactRecord record, Collector<PactRecord> out) throws Exception
		{
			final Tuple t = record.getField(1, this.tuple);
			
			this.custName.setValue(t.getStringValueAt(1));
			this.address.setValue(t.getStringValueAt(2));
			this.nationKey.setValue((int) t.getLongValueAt(3));
			this.phone.setValue(t.getStringValueAt(4));
			this.balance.setValue(t.getStringValueAt(5));
			this.comment.setValue(t.getStringValueAt(7));
			
			record.setField(1, this.custName);
			record.setField(3, this.balance);
			record.setField(4, this.nationKey);
			record.setField(5, this.address);
			record.setField(6, this.phone);
			record.setField(7, this.comment);
			
			out.collect(record);
		}
	}
	
	/**
	 * Returns (0 = nationkey, 1 = nation_name)
	 */
	public static class ProjectN extends MapStub
	{
		private final Tuple tuple = new Tuple();
		private final PactString nationName = new PactString();
		
		@Override
		public void map(PactRecord record, Collector<PactRecord> out) throws Exception
		{
			final Tuple t = record.getField(1, this.tuple);
			
			this.nationName.setValue(t.getStringValueAt(1));
			record.setField(1, this.nationName);
			out.collect(record);
		}
	}
	
	// --------------------------------------------------------------------------------------------
	//                                        Joins
	// --------------------------------------------------------------------------------------------

	/**
	 * Returns (0 = custKey, 1 = tuple (extendedprice, discount) )
	 */
	public static class JoinOL extends MatchStub
	{
		@Override
		public void match(PactRecord order, PactRecord lineitem, Collector<PactRecord> out) throws Exception {
			lineitem.setField(0, order.getField(1, PactInteger.class));
			out.collect(lineitem);
		}
	}

	/**
	 * Returns (0 = custkey, 1 = custName, 2 = extPrice * (1-discount), 3 = balance, 4 = nationkey, 5 = address, 6 = phone, 7 = comment)
	 */
	public static class JoinCOL extends MatchStub
	{
		private final PactDouble d = new PactDouble();
		
		@Override
		public void match(PactRecord custRecord, PactRecord olRecord, Collector<PactRecord> out) throws Exception
		{
			final Tuple t = olRecord.getField(1, Tuple.class);
			final double extPrice = Double.parseDouble(t.getStringValueAt(0));
			final double discount = Double.parseDouble(t.getStringValueAt(1));
			
			this.d.setValue(extPrice * (1 - discount));
			custRecord.setField(2, this.d);
			out.collect(custRecord);
		}

	}
	
	/**
	 * Returns (0 = custkey, 1 = custName, 2 = extPrice * (1-discount), 3 = balance, 4 = nationName, 5 = address, 6 = phone, 7 = comment)
	 */
	public static class JoinNCOL extends MatchStub
	{
		@Override
		public void match(PactRecord colRecord, PactRecord nation, Collector<PactRecord> out) throws Exception {
			colRecord.setField(4, nation.getField(1, PactString.class));
			out.collect(colRecord);
		}
	}
	
	@Combinable
	public static class Sum extends ReduceStub
	{
		private final PactDouble d = new PactDouble();
		
		@Override
		public void reduce(Iterator<PactRecord> records, Collector<PactRecord> out) throws Exception
		{
			PactRecord record = null;
			double sum = 0;
			while (records.hasNext()) {
				record = records.next();
				sum += record.getField(2, PactDouble.class).getValue();
			}
		
			this.d.setValue(sum);
			record.setField(2, this.d);
			out.collect(record);
		}
		
		@Override
		public void combine(Iterator<PactRecord> records, Collector<PactRecord> out) throws Exception {
			reduce(records,out);
		}
	}

	public static class TupleOutputFormat extends FileOutputFormat
	{
		private final DecimalFormat formatter;
		private final StringBuilder buffer = new StringBuilder();
		
		public TupleOutputFormat() {
			DecimalFormatSymbols decimalFormatSymbol = new DecimalFormatSymbols();
			decimalFormatSymbol.setDecimalSeparator('.');
			
			this.formatter = new DecimalFormat("#.####");
			this.formatter.setDecimalFormatSymbols(decimalFormatSymbol);
		}
		
		@Override
		public void writeRecord(PactRecord record) throws IOException
		{
			this.buffer.setLength(0);
			this.buffer.append(record.getField(0, PactInteger.class).toString()).append('|');
			this.buffer.append(record.getField(1, PactString.class).toString()).append('|');
			
			this.buffer.append(this.formatter.format(record.getField(2, PactDouble.class).getValue())).append('|');
			
			this.buffer.append(record.getField(3, PactString.class).toString()).append('|');
			this.buffer.append(record.getField(4, PactString.class).toString()).append('|');
			this.buffer.append(record.getField(5, PactString.class).toString()).append('|');
			this.buffer.append(record.getField(6, PactString.class).toString()).append('|');
			this.buffer.append(record.getField(7, PactString.class).toString()).append('|');
			
			this.buffer.append('\n');
			
			final byte[] bytes = this.buffer.toString().getBytes();
			this.stream.write(bytes);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see eu.stratosphere.pact.common.plan.PlanAssemblerDescription#getDescription()
	 */
	@Override
	public String getDescription()
	{
		return "TPC-H Query 10";
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see eu.stratosphere.pact.common.plan.PlanAssembler#getPlan(java.lang.String[])
	 */
	@Override
	public Plan getPlan(String... args) throws IllegalArgumentException
	{
		final String ordersPath;
		final String lineitemsPath;
		final String customersPath;
		final String nationsPath;
		final String resultPath;
		
		final int degreeOfParallelism;

		if (args.length < 6) {
			throw new IllegalArgumentException("Invalid number of parameters");
		} else {
			degreeOfParallelism = Integer.parseInt(args[0]);
			ordersPath = args[1];
			lineitemsPath = args[2];
			customersPath = args[3];
			nationsPath = args[4];
			resultPath = args[5];
		}
		
		FileDataSource orders = new FileDataSource(IntTupleDataInFormat.class, ordersPath, "Orders");
		orders.setParameter(TextInputFormat.RECORD_DELIMITER, "\n");
		orders.setDegreeOfParallelism(degreeOfParallelism);
		// orders.setOutputContract(UniqueKey.class);
		// orders.getCompilerHints().setAvgNumValuesPerKey(1);

		FileDataSource lineitems = new FileDataSource(IntTupleDataInFormat.class, lineitemsPath, "LineItems");
		lineitems.setParameter(TextInputFormat.RECORD_DELIMITER, "\n");
		lineitems.setDegreeOfParallelism(degreeOfParallelism);
		// lineitems.getCompilerHints().setAvgNumValuesPerKey(4);

		FileDataSource customers = new FileDataSource(IntTupleDataInFormat.class, customersPath, "Customers");
		customers.setParameter(TextInputFormat.RECORD_DELIMITER, "\n");
		customers.setDegreeOfParallelism(degreeOfParallelism);

		FileDataSource nations = new FileDataSource(IntTupleDataInFormat.class, nationsPath, "Nations");
		nations.setParameter(TextInputFormat.RECORD_DELIMITER, "\n");
		nations.setDegreeOfParallelism(degreeOfParallelism);

		MapContract mapO = MapContract.builder(FilterO.class)
			.name("FilterO")
			.build();
		mapO.setDegreeOfParallelism(degreeOfParallelism);

		MapContract mapLi = MapContract.builder(FilterLI.class)
			.name("FilterLi")
			.build();
		mapLi.setDegreeOfParallelism(degreeOfParallelism);

		MapContract projectC = MapContract.builder(ProjectC.class)
			.name("ProjectC")
			.build();
		projectC.setDegreeOfParallelism(degreeOfParallelism);

		MapContract projectN = MapContract.builder(ProjectN.class)
			.name("ProjectN")
			.build();
		projectN.setDegreeOfParallelism(degreeOfParallelism);

		MatchContract joinOL = MatchContract.builder(JoinOL.class, PactInteger.class, 0, 0)
			.name("JoinOL")
			.build();
		joinOL.setDegreeOfParallelism(degreeOfParallelism);

		MatchContract joinCOL = MatchContract.builder(JoinCOL.class, PactInteger.class, 0, 0)
			.name("JoinCOL")
			.build();
		joinCOL.setDegreeOfParallelism(degreeOfParallelism);

		MatchContract joinNCOL = MatchContract.builder(JoinNCOL.class, PactInteger.class, 4, 0)
			.name("JoinNCOL")
			.build();
		joinNCOL.setDegreeOfParallelism(degreeOfParallelism);

		ReduceContract reduce = ReduceContract.builder(Sum.class)
			.keyField(PactInteger.class, 0) 
			.keyField(PactString.class, 1)
			.keyField(PactString.class, 3)
			.keyField(PactString.class, 4)
			.keyField(PactString.class, 5)
			.keyField(PactString.class, 6)
			.keyField(PactString.class, 7)
			.name("Reduce")
			.build();
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
