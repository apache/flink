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

package eu.stratosphere.pact.test.testPrograms.tpch4;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Iterator;

import org.apache.log4j.Logger;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.test.testPrograms.util.IntTupleDataInFormat;
import eu.stratosphere.pact.test.testPrograms.util.StringTupleDataOutFormat;
import eu.stratosphere.pact.test.testPrograms.util.Tuple;

/**
 * Implementation of the TPC-H Query 4 as a PACT program.
 * 
 * @author Mathias Peters <mathias.peters@informatik.hu-berlin.de>
 * @author Moritz Kaufmann <moritz.kaufmann@campus.tu-berlin.de>
 */
public class TPCHQuery4 implements PlanAssembler, PlanAssemblerDescription {

	private static Logger LOGGER = Logger.getLogger(TPCHQuery4.class);
	
	private int degreeOfParallelism = 1;
	private String ordersInputPath;
	private String lineItemInputPath;
	private String outputPath;
	
	
	/**
	 * Small {@link MapStub} to filer out the irrelevant orders.
	 *
	 */
	//@SameKey
	public static class OFilter extends MapStub {

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
		
		/* (non-Javadoc)
		 * @see eu.stratosphere.pact.common.stub.MapStub#map(eu.stratosphere.pact.common.type.Key, eu.stratosphere.pact.common.type.Value, eu.stratosphere.pact.common.stub.Collector)
		 */
		@Override
		public void map(PactRecord record, Collector out) throws Exception {
			Tuple tuple = record.getField(1, Tuple.class);
			Date orderDate;
			
			String orderStringDate = tuple.getStringValueAt(4);
			
			try {
				orderDate = sdf.parse(orderStringDate);
			} catch (ParseException e) {
				throw new RuntimeException(e);
			}
			
			if(paramDate.before(orderDate) && plusThreeMonths.after(orderDate))
			{
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
	public static class LiFilter extends MapStub {

		private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		
		@Override
		public void map(PactRecord record, Collector out) throws Exception {
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
	public static class JoinLiO extends MatchStub {
		
		@Override
		public void match(PactRecord order, PactRecord line, Collector out)
				throws Exception {
			Tuple orderTuple = order.getField(1, Tuple.class);
			
			orderTuple.project(32);
			String newOrderKey = orderTuple.getStringValueAt(0);
			
			order.setField(0, new PactString(newOrderKey));
			out.collect(order);
		}
	}
	
	/**
	 * Implements the count(*) part. 
	 *
	 */
	//@SameKey
	public static class CountAgg extends ReduceStub {
		
		/* (non-Javadoc)
		 * @see eu.stratosphere.pact.common.stub.ReduceStub#reduce(eu.stratosphere.pact.common.type.Key, java.util.Iterator, eu.stratosphere.pact.common.stub.Collector)
		 */
		@Override
		public void reduce(Iterator<PactRecord> records, Collector out) throws Exception {	
			long count = 0;
			PactRecord rec = null;
			
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
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public Plan getPlan(String... args) throws IllegalArgumentException {
		
		if(args == null || args.length != 4)
		{
			LOGGER.warn("number of arguments do not match!");
			this.ordersInputPath = "";
			this.lineItemInputPath = "";
			this.outputPath = "";
		}else
		{
			setArgs(args);
		}
		
		FileDataSource orders = 
			new FileDataSource(IntTupleDataInFormat.class, this.ordersInputPath, "Orders");
		orders.setDegreeOfParallelism(this.degreeOfParallelism);
		//orders.setOutputContract(UniqueKey.class);
		
		FileDataSource lineItems =
			new FileDataSource(IntTupleDataInFormat.class, this.lineItemInputPath, "LineItems");
		lineItems.setDegreeOfParallelism(this.degreeOfParallelism);
		
		FileDataSink result = 
				new FileDataSink(StringTupleDataOutFormat.class, this.outputPath, "Output");
		result.setDegreeOfParallelism(degreeOfParallelism);
		
		MapContract lineFilter = 
				new MapContract(LiFilter.class, "LineItemFilter");
		lineFilter.setDegreeOfParallelism(degreeOfParallelism);
		
		MapContract ordersFilter = 
				new MapContract(OFilter.class, "OrdersFilter");
		ordersFilter.setDegreeOfParallelism(degreeOfParallelism);
		
		MatchContract join = 
				new MatchContract(JoinLiO.class, PactInteger.class, 0, 0, "OrdersLineitemsJoin");
			join.setDegreeOfParallelism(degreeOfParallelism);
		
		ReduceContract aggregation = 
				new ReduceContract(CountAgg.class, PactString.class, 0, "AggregateGroupBy");
		aggregation.setDegreeOfParallelism(this.degreeOfParallelism);
		
		lineFilter.addInput(lineItems);
		ordersFilter.addInput(orders);
		join.addFirstInput(ordersFilter);
		join.addSecondInput(lineFilter);
		aggregation.addInput(join);
		result.addInput(aggregation);
		
			
		return new Plan(result, "TPC-H 4");
	}

	/**
	 * Get the args into the members.
	 * @param args
	 */
	private void setArgs(String[] args) {
		this.degreeOfParallelism = Integer.parseInt(args[0]);
		this.ordersInputPath = args[1];
		this.lineItemInputPath = args[2];
		this.outputPath = args[3];
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getDescription() {
		return "Parameters: [dop] [orders-input] [lineitem-input] [output]";
	}

}
