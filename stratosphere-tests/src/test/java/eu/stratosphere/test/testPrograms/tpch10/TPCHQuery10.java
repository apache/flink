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

package eu.stratosphere.test.testPrograms.tpch10;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Iterator;

import eu.stratosphere.api.Plan;
import eu.stratosphere.api.Program;
import eu.stratosphere.api.ProgramDescription;
import eu.stratosphere.api.operators.FileDataSink;
import eu.stratosphere.api.operators.FileDataSource;
import eu.stratosphere.api.record.functions.MapFunction;
import eu.stratosphere.api.record.functions.JoinFunction;
import eu.stratosphere.api.record.functions.ReduceFunction;
import eu.stratosphere.api.record.io.FileOutputFormat;
import eu.stratosphere.api.record.operators.MapOperator;
import eu.stratosphere.api.record.operators.JoinOperator;
import eu.stratosphere.api.record.operators.ReduceOperator;
import eu.stratosphere.test.testPrograms.util.IntTupleDataInFormat;
import eu.stratosphere.test.testPrograms.util.Tuple;
import eu.stratosphere.types.DoubleValue;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.Collector;

/**
 * @author rico
 * @author Matthias Ringwald
 * @author Stephan Ewen
 */
public class TPCHQuery10 implements Program, ProgramDescription
{
	// --------------------------------------------------------------------------------------------
	//                         Local Filters and Projections
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Forwards (0 = orderkey, 1 = custkey).
	 */
	public static class FilterO extends MapFunction
	{
		private static final int YEAR_FILTER = 1990;
		
		private final IntValue custKey = new IntValue();
		
		@Override
		public void map(Record record, Collector<Record> out) throws Exception {
			
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
	public static class FilterLI extends MapFunction
	{
		private final Tuple tuple = new Tuple();
		
		@Override
		public void map(Record record, Collector<Record> out) throws Exception
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
	public static class ProjectC extends MapFunction {

		private final Tuple tuple = new Tuple();
		
		private final StringValue custName = new StringValue();
		
		private final StringValue balance = new StringValue();
		private final IntValue nationKey = new IntValue();
		private final StringValue address = new StringValue();
		private final StringValue phone = new StringValue();
		private final StringValue comment = new StringValue();
		
		@Override
		public void map(Record record, Collector<Record> out) throws Exception
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
	public static class ProjectN extends MapFunction
	{
		private final Tuple tuple = new Tuple();
		private final StringValue nationName = new StringValue();
		
		@Override
		public void map(Record record, Collector<Record> out) throws Exception
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
	public static class JoinOL extends JoinFunction
	{
		@Override
		public void match(Record order, Record lineitem, Collector<Record> out) throws Exception {
			lineitem.setField(0, order.getField(1, IntValue.class));
			out.collect(lineitem);
		}
	}

	/**
	 * Returns (0 = custkey, 1 = custName, 2 = extPrice * (1-discount), 3 = balance, 4 = nationkey, 5 = address, 6 = phone, 7 = comment)
	 */
	public static class JoinCOL extends JoinFunction
	{
		private final DoubleValue d = new DoubleValue();
		
		@Override
		public void match(Record custRecord, Record olRecord, Collector<Record> out) throws Exception
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
	public static class JoinNCOL extends JoinFunction
	{
		@Override
		public void match(Record colRecord, Record nation, Collector<Record> out) throws Exception {
			colRecord.setField(4, nation.getField(1, StringValue.class));
			out.collect(colRecord);
		}
	}
	
	@ReduceOperator.Combinable
	public static class Sum extends ReduceFunction
	{
		private final DoubleValue d = new DoubleValue();
		
		@Override
		public void reduce(Iterator<Record> records, Collector<Record> out) throws Exception
		{
			Record record = null;
			double sum = 0;
			while (records.hasNext()) {
				record = records.next();
				sum += record.getField(2, DoubleValue.class).getValue();
			}
		
			this.d.setValue(sum);
			record.setField(2, this.d);
			out.collect(record);
		}
		
		@Override
		public void combine(Iterator<Record> records, Collector<Record> out) throws Exception {
			reduce(records,out);
		}
	}

	public static class TupleOutputFormat extends FileOutputFormat {
		private static final long serialVersionUID = 1L;
		
		private final DecimalFormat formatter;
		private final StringBuilder buffer = new StringBuilder();
		
		public TupleOutputFormat() {
			DecimalFormatSymbols decimalFormatSymbol = new DecimalFormatSymbols();
			decimalFormatSymbol.setDecimalSeparator('.');
			
			this.formatter = new DecimalFormat("#.####");
			this.formatter.setDecimalFormatSymbols(decimalFormatSymbol);
		}
		
		@Override
		public void writeRecord(Record record) throws IOException
		{
			this.buffer.setLength(0);
			this.buffer.append(record.getField(0, IntValue.class).toString()).append('|');
			this.buffer.append(record.getField(1, StringValue.class).toString()).append('|');
			
			this.buffer.append(this.formatter.format(record.getField(2, DoubleValue.class).getValue())).append('|');
			
			this.buffer.append(record.getField(3, StringValue.class).toString()).append('|');
			this.buffer.append(record.getField(4, StringValue.class).toString()).append('|');
			this.buffer.append(record.getField(5, StringValue.class).toString()).append('|');
			this.buffer.append(record.getField(6, StringValue.class).toString()).append('|');
			this.buffer.append(record.getField(7, StringValue.class).toString()).append('|');
			
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
		
		FileDataSource orders = new FileDataSource(new IntTupleDataInFormat(), ordersPath, "Orders");
		// orders.setOutputContract(UniqueKey.class);
		// orders.getCompilerHints().setAvgNumValuesPerKey(1);

		FileDataSource lineitems = new FileDataSource(new IntTupleDataInFormat(), lineitemsPath, "LineItems");
		// lineitems.getCompilerHints().setAvgNumValuesPerKey(4);

		FileDataSource customers = new FileDataSource(new IntTupleDataInFormat(), customersPath, "Customers");

		FileDataSource nations = new FileDataSource(new IntTupleDataInFormat(), nationsPath, "Nations");


		MapOperator mapO = MapOperator.builder(FilterO.class)
			.name("FilterO")
			.build();

		MapOperator mapLi = MapOperator.builder(FilterLI.class)
			.name("FilterLi")
			.build();

		MapOperator projectC = MapOperator.builder(ProjectC.class)
			.name("ProjectC")
			.build();

		MapOperator projectN = MapOperator.builder(ProjectN.class)
			.name("ProjectN")
			.build();

		JoinOperator joinOL = JoinOperator.builder(JoinOL.class, IntValue.class, 0, 0)
			.name("JoinOL")
			.build();

		JoinOperator joinCOL = JoinOperator.builder(JoinCOL.class, IntValue.class, 0, 0)
			.name("JoinCOL")
			.build();

		JoinOperator joinNCOL = JoinOperator.builder(JoinNCOL.class, IntValue.class, 4, 0)
			.name("JoinNCOL")
			.build();

		ReduceOperator reduce = ReduceOperator.builder(Sum.class)
			.keyField(IntValue.class, 0) 
			.keyField(StringValue.class, 1)
			.keyField(StringValue.class, 3)
			.keyField(StringValue.class, 4)
			.keyField(StringValue.class, 5)
			.keyField(StringValue.class, 6)
			.keyField(StringValue.class, 7)
			.name("Reduce")
			.build();

		FileDataSink result = new FileDataSink(new TupleOutputFormat(), resultPath, "Output");

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
		Plan p = new Plan(result, "TPCH Q10");
		p.setDefaultParallelism(degreeOfParallelism);
		return p;
	}
}
