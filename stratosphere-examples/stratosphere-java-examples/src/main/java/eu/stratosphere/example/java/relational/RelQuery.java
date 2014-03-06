/***********************************************************************************************************************
 *
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
 *
 **********************************************************************************************************************/
package eu.stratosphere.example.java.relational;


import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.functions.FilterFunction;
import eu.stratosphere.api.java.functions.JoinFunction;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.api.java.tuple.Tuple5;

import static eu.stratosphere.api.java.aggregation.Aggregations.*;


public class RelQuery {
	
	@SuppressWarnings("serial")
	public static void main(String[] args) {
		if (args.length < 3) {
			System.out.println("Usage: <input orders> <input lineitem> <output path>");
			return;
		}
		
		final String ordersPath = args[0];
		final String lineitemsPath = args[1];
		final String outputPath = args[2];
		
		final String prioFilter = "0";
		final int yearFilter = 1990;
		
		// this will return the LocalExecutionContext, if invoked locally, and the ClusterExecutionContext, if invoked on the cluster
		final ExecutionEnvironment context = ExecutionEnvironment.getExecutionEnvironment();
		
		// orderkey, orderstatus, orderdate, orderprio, shipprio
		DataSet<Tuple5<Long, String, String, String, String>> orders = context.readCsvFile(ordersPath)
				.includeFields("10101101").types(Long.class, String.class, String.class, String.class, String.class);
		
		// orderkey, extendedprice
		DataSet<Tuple2<Long, Double>> lineitem = context.readCsvFile(lineitemsPath)
				.includeFields("TFFFFT").types(Long.class, Double.class);
		
		// filter
		DataSet<Tuple5<Long, String, String, String, String>> filtered = orders.filter(
			new FilterFunction<Tuple5<Long, String, String, String, String>>() {
				@Override
				public boolean filter(Tuple5<Long, String, String, String, String> value) throws Exception {
					String orderStatus = value.T2();
					String orderPrio = value.T4();
					String orderDate = value.T3();
					return orderStatus.equals("F") && orderPrio.startsWith(prioFilter) && 
							Integer.parseInt(orderDate.substring(0, 4)) > yearFilter;
				}
		});
		
		DataSet<Tuple3<Long, String, Double>> joined = filtered.join(lineitem).where(0).equalTo(0).with(new OLiJoinFunction());
		
		DataSet<Tuple3<Long, String, Double>> result = joined.groupBy(0, 1).aggregate(SUM, 2);
		
		result.writeAsCsv(outputPath);
	}
	
	
	public static class OLiJoinFunction extends JoinFunction<Tuple5<Long, String, String, String, String>, Tuple2<Long, Double>, Tuple3<Long, String, Double>> {

		private static final long serialVersionUID = 1L;

		@Override
		public Tuple3<Long, String, Double> join(Tuple5<Long, String, String, String, String> first,
				Tuple2<Long, Double> second) throws Exception
		{
			return new Tuple3<Long, String, Double>(first.T1(), first.T5(), second.T2());
		}
		
	}
}
