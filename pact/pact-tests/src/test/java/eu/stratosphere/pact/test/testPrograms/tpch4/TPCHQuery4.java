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
import eu.stratosphere.pact.common.contract.FileDataSinkContract;
import eu.stratosphere.pact.common.contract.FileDataSourceContract;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.contract.OutputContract.SameKey;
import eu.stratosphere.pact.common.contract.OutputContract.UniqueKey;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MapStub;
import eu.stratosphere.pact.common.stub.MatchStub;
import eu.stratosphere.pact.common.stub.ReduceStub;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.example.relational.util.IntTupleDataInFormat;
import eu.stratosphere.pact.example.relational.util.StringTupleDataOutFormat;
import eu.stratosphere.pact.example.relational.util.Tuple;

/**
 * Implementation of the TPC-H Query 4 as a PACT program.
 * 
 * @author Mathias Peters <mathias.peters@informatik.hu-berlin.de>
 *
 */
public class TPCHQuery4 implements PlanAssembler, PlanAssemblerDescription {

	private static Logger LOGGER = Logger.getLogger(TPCHQuery4.class);
	
	private int degreeOfParallelism = 1;
	private String ordersInputPath;
	private String lineItemInputPath;
	private String outputPath;
	
	
	/**
	 * Small {@link MapStub} to filer out the irrelevant orders.
	 * @author Mathias Peters <mathias.peters@informatik.hu-berlin.de>
	 *
	 */
	@SameKey
	public static class OFilter extends MapStub<PactInteger, Tuple, PactInteger, Tuple> {

		private final String dateParamString = "1995-01-01";
		private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		private final GregorianCalendar gregCal = new GregorianCalendar();
		
		private Date paramDate;
		private Date plusThreeMonths;
		
		@Override
		public void configure(Configuration parameters) {
			super.configure(parameters);
				
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
		public void map(PactInteger key, Tuple value,
				Collector<PactInteger, Tuple> out) {
			
			String orderStringDate = value.getStringValueAt(4);
			
			Date orderDate;
			try {
				orderDate = sdf.parse(orderStringDate);
			} catch (ParseException e) {
				throw new RuntimeException(e);
			}
			
			if(paramDate.before(orderDate) && plusThreeMonths.after(orderDate))
			{
				out.collect(key, value);
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
	 * @author Mathias Peters <mathias.peters@informatik.hu-berlin.de>
	 * 
	 */
	@SameKey
	public static class LiFilter extends
			MapStub<PactInteger, Tuple, PactInteger, Tuple> {

		private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		
		@Override
		public void map(PactInteger key, Tuple value,
				Collector<PactInteger, Tuple> out) {

			String commitString = value.getStringValueAt(11);
			String receiptString = value.getStringValueAt(12);

			Date commitDate;
			Date receiptDate;
			
			try {
				commitDate = sdf.parse(commitString);
				receiptDate = sdf.parse(receiptString);
			} catch (ParseException e) {
				throw new RuntimeException(e);
			}

			if (commitDate.before(receiptDate)) {
				out.collect(key, value);
			}

		}

	}
	
	/**
	 * Implements the equijoin on the orderkey and performs the projection on 
	 * the order priority as well.
	 * @author Mathias Peters <mathias.peters@informatik.hu-berlin.de>
	 *
	 */
	public static class JoinLiO extends MatchStub<PactInteger, Tuple, Tuple, PactString, Tuple> {

		@Override
		public void match(PactInteger key, Tuple orderValue, Tuple lineValue,
				Collector<PactString, Tuple> outputTuple) {
			
			orderValue.project(32);
			String newOrderKey = orderValue.getStringValueAt(0);
			outputTuple.collect(new PactString(newOrderKey), orderValue);

		}

	}
	
	/**
	 * Implements the count(*) part. 
	 * 
	 * @author Mathias Peters <mathias.peters@informatik.hu-berlin.de>
	 *
	 */
	@SameKey
	public static class CountAgg extends ReduceStub<PactString, Tuple, PactString, Tuple> {

		
		/* (non-Javadoc)
		 * @see eu.stratosphere.pact.common.stub.ReduceStub#reduce(eu.stratosphere.pact.common.type.Key, java.util.Iterator, eu.stratosphere.pact.common.stub.Collector)
		 */
		@Override
		public void reduce(PactString key, Iterator<Tuple> values, Collector<PactString, Tuple> out) {
			
			long count = 0;
			Tuple t = null;
			while(values.hasNext()) {
			 	t = values.next();
			 	count++;
			}
			
			if(t != null)
			{
				t.addAttribute("" + count);
			}
			
			out.collect(key, t);
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
		
		FileDataSourceContract<PactInteger, Tuple> orders = 
			new FileDataSourceContract<PactInteger, Tuple>(IntTupleDataInFormat.class, this.ordersInputPath, "Orders");
		orders.setDegreeOfParallelism(this.degreeOfParallelism);
		orders.setOutputContract(UniqueKey.class);
		
		FileDataSourceContract<PactInteger, Tuple> lineItems =
			new FileDataSourceContract<PactInteger, Tuple>(IntTupleDataInFormat.class, this.lineItemInputPath, "LineItems");
		lineItems.setDegreeOfParallelism(this.degreeOfParallelism);
		
		MatchContract<PactInteger, Tuple, Tuple, PactString, Tuple> join = 
			new MatchContract<PactInteger, Tuple, Tuple, PactString, Tuple>(
				JoinLiO.class, "OrdersLineitemsJoin");
		join.setDegreeOfParallelism(degreeOfParallelism);
		
		FileDataSinkContract<PactString, Tuple> result = new FileDataSinkContract<PactString, Tuple>(
				StringTupleDataOutFormat.class, this.outputPath, "Output");
		result.setDegreeOfParallelism(degreeOfParallelism);
		
		MapContract<PactInteger, Tuple, PactInteger, Tuple> lineFilter = new MapContract<PactInteger, Tuple, PactInteger, Tuple>(
				LiFilter.class, "LineItemFilter");
		lineFilter.setDegreeOfParallelism(degreeOfParallelism);
		
		MapContract<PactInteger, Tuple, PactInteger, Tuple> ordersFilter = new MapContract<PactInteger, Tuple, PactInteger, Tuple>(
				OFilter.class, "OrdersFilter");
		ordersFilter.setDegreeOfParallelism(degreeOfParallelism);
		
		ReduceContract<PactString, Tuple, PactString, Tuple> aggregation = new ReduceContract<PactString, Tuple, PactString, Tuple>(
				CountAgg.class, "AggregateGroupBy");
		aggregation.setDegreeOfParallelism(this.degreeOfParallelism);
		
		lineFilter.addInput(lineItems);
		ordersFilter.addInput(orders);
		join.addFirstInput(ordersFilter);
		join.addSecondInput(lineFilter);
		aggregation.addInput(join);
		result.setInput(aggregation);
		
			
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
