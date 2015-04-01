/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.recordJobs.relational;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Iterator;

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.Program;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.record.functions.JoinFunction;
import org.apache.flink.api.java.record.functions.MapFunction;
import org.apache.flink.api.java.record.functions.ReduceFunction;
import org.apache.flink.api.java.record.operators.FileDataSink;
import org.apache.flink.api.java.record.operators.FileDataSource;
import org.apache.flink.api.java.record.operators.JoinOperator;
import org.apache.flink.api.java.record.operators.MapOperator;
import org.apache.flink.api.java.record.operators.ReduceOperator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.test.recordJobs.util.IntTupleDataInFormat;
import org.apache.flink.test.recordJobs.util.StringTupleDataOutFormat;
import org.apache.flink.test.recordJobs.util.Tuple;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.Record;
import org.apache.flink.types.StringValue;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the TPC-H Query 4 as a Flink program.
 */

@SuppressWarnings({"serial", "deprecation"})
public class TPCHQuery4 implements Program, ProgramDescription {

	private static Logger LOG = LoggerFactory.getLogger(TPCHQuery4.class);
	
	private int parallelism = 1;
	private String ordersInputPath;
	private String lineItemInputPath;
	private String outputPath;
	
	
	/**
	 * Small {@link MapFunction} to filer out the irrelevant orders.
	 *
	 */
	//@SameKey
	public static class OFilter extends MapFunction {

		private final String dateParamString = "1995-01-01";
		private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		private final GregorianCalendar gregCal = new GregorianCalendar();
		
		private Date paramDate;
		private Date plusThreeMonths;
		
		@Override
		public void open(Configuration parameters) {				
			try {
				this.paramDate = sdf.parse(this.dateParamString);
				this.plusThreeMonths = getPlusThreeMonths(paramDate);
				
			} catch (ParseException e) {
				throw new RuntimeException(e);
			}
		}
		
		@Override
		public void map(Record record, Collector<Record> out) throws Exception {
			Tuple tuple = record.getField(1, Tuple.class);
			Date orderDate;
			
			String orderStringDate = tuple.getStringValueAt(4);
			
			try {
				orderDate = sdf.parse(orderStringDate);
			} catch (ParseException e) {
				throw new RuntimeException(e);
			}
			
			if(paramDate.before(orderDate) && plusThreeMonths.after(orderDate)) {
				out.collect(record);
			}

		}

		/**
		 * Calculates the {@link Date} which is three months after the given one.
		 * @param paramDate of type {@link Date}.
		 * @return a {@link Date} three month later.
		 */
		private Date getPlusThreeMonths(Date paramDate) {
			
			gregCal.setTime(paramDate);
			gregCal.add(Calendar.MONTH, 3);
			Date plusThreeMonths = gregCal.getTime();
			return plusThreeMonths;
		}
	}
	
	/**
	 * Simple filter for the line item selection. It filters all teh tuples that do
	 * not satisfy the &quot;l_commitdate &lt; l_receiptdate&quot; condition.
	 * 
	 */
	//@SameKey
	public static class LiFilter extends MapFunction {

		private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		
		@Override
		public void map(Record record, Collector<Record> out) throws Exception {
			Tuple tuple = record.getField(1, Tuple.class);
			String commitString = tuple.getStringValueAt(11);
			String receiptString = tuple.getStringValueAt(12);

			Date commitDate;
			Date receiptDate;
			
			try {
				commitDate = sdf.parse(commitString);
				receiptDate = sdf.parse(receiptString);
			} catch (ParseException e) {
				throw new RuntimeException(e);
			}

			if (commitDate.before(receiptDate)) {
				out.collect(record);
			}

		}
	}
	
	/**
	 * Implements the equijoin on the orderkey and performs the projection on 
	 * the order priority as well.
	 *
	 */
	public static class JoinLiO extends JoinFunction {
		
		@Override
		public void join(Record order, Record line, Collector<Record> out)
				throws Exception {
			Tuple orderTuple = order.getField(1, Tuple.class);
			
			orderTuple.project(32);
			String newOrderKey = orderTuple.getStringValueAt(0);
			
			order.setField(0, new StringValue(newOrderKey));
			out.collect(order);
		}
	}
	
	/**
	 * Implements the count(*) part. 
	 *
	 */
	//@SameKey
	public static class CountAgg extends ReduceFunction {
		
		@Override
		public void reduce(Iterator<Record> records, Collector<Record> out) throws Exception {	
			long count = 0;
			Record rec = null;
			
			while(records.hasNext()) {
			 	rec = records.next();
			 	count++;
			}
			
			if(rec != null)
			{
				Tuple tuple = new Tuple();
				tuple.addAttribute("" + count);
				rec.setField(1, tuple);
			}
			
			out.collect(rec);
		}
	}
	

	@Override
	public Plan getPlan(String... args) throws IllegalArgumentException {
		
		if(args == null || args.length != 4)
		{
			LOG.warn("number of arguments do not match!");
			this.ordersInputPath = "";
			this.lineItemInputPath = "";
			this.outputPath = "";
		}else
		{
			setArgs(args);
		}
		
		FileDataSource orders = 
			new FileDataSource(new IntTupleDataInFormat(), this.ordersInputPath, "Orders");
		orders.setParallelism(this.parallelism);
		//orders.setOutputContract(UniqueKey.class);
		
		FileDataSource lineItems =
			new FileDataSource(new IntTupleDataInFormat(), this.lineItemInputPath, "LineItems");
		lineItems.setParallelism(this.parallelism);
		
		FileDataSink result = 
				new FileDataSink(new StringTupleDataOutFormat(), this.outputPath, "Output");
		result.setParallelism(parallelism);
		
		MapOperator lineFilter = 
				MapOperator.builder(LiFilter.class)
			.name("LineItemFilter")
			.build();
		lineFilter.setParallelism(parallelism);
		
		MapOperator ordersFilter = 
				MapOperator.builder(OFilter.class)
			.name("OrdersFilter")
			.build();
		ordersFilter.setParallelism(parallelism);
		
		JoinOperator join = 
				JoinOperator.builder(JoinLiO.class, IntValue.class, 0, 0)
			.name("OrdersLineitemsJoin")
			.build();
			join.setParallelism(parallelism);
		
		ReduceOperator aggregation = 
				ReduceOperator.builder(CountAgg.class, StringValue.class, 0)
			.name("AggregateGroupBy")
			.build();
		aggregation.setParallelism(this.parallelism);
		
		lineFilter.setInput(lineItems);
		ordersFilter.setInput(orders);
		join.setFirstInput(ordersFilter);
		join.setSecondInput(lineFilter);
		aggregation.setInput(join);
		result.setInput(aggregation);
		
			
		return new Plan(result, "TPC-H 4");
	}

	/**
	 * Get the args into the members.
	 * @param args
	 */
	private void setArgs(String[] args) {
		this.parallelism = Integer.parseInt(args[0]);
		this.ordersInputPath = args[1];
		this.lineItemInputPath = args[2];
		this.outputPath = args[3];
	}


	@Override
	public String getDescription() {
		return "Parameters: [parallelism] [orders-input] [lineitem-input] [output]";
	}

}
