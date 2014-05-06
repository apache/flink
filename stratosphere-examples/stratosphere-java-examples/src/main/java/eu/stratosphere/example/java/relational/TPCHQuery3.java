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

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.functions.FilterFunction;
import eu.stratosphere.api.java.functions.JoinFunction;
import eu.stratosphere.api.java.functions.ReduceFunction;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.api.java.tuple.Tuple4;
import eu.stratosphere.api.java.tuple.Tuple5;

/**
 *	This program implements a modified version of the TPC-H query 3. The
 *	example demonstrates how to assign names to fields by extending the Tuple class.
 *
 *	The original query can be found at
 *	http://www.tpc.org/tpch/spec/tpch2.16.0.pdf (page 29).
 *
 *	This program implements the following SQL equivalent:
 *
 *	select l_orderkey, 
 *		sum(l_extendedprice*(1-l_discount)) as revenue,
 *		o_orderdate, 
 *		o_shippriority from customer, 
 *		orders, 
 *		lineitem 
 *	where
 *		c_mktsegment = '[SEGMENT]' and 
 *		c_custkey = o_custkey and 
 *		l_orderkey = o_orderkey and
 *		o_orderdate < date '[DATE]' and 
 *		l_shipdate > date '[DATE]'
 *	group by 
 *		l_orderkey, 
 *		o_orderdate, 
 *		o_shippriority 
 *	order by  					//not yet
 *		revenue desc,
 *		o_orderdate;
 *
 */
@SuppressWarnings("serial")
public class TPCHQuery3 {

	// --------------------------------------------------------------------------------------------
	//  Custom type classes
	// --------------------------------------------------------------------------------------------
	
	public static class Lineitem extends Tuple4<Integer, Double, Double, String> {

		public Integer getOrderkey() {
			return this.f0;
		}

		public Double getDiscount() {
			return this.f2;
		}

		public Double getExtendedprice() {
			return this.f1;
		}

		public String getShipdate() {
			return this.f3;
		}
	}

	public static class Customer extends Tuple2<Integer, String> {
		
		public Integer getCustKey() {
			return this.f0;
		}
		
		public String getMktsegment() {
			return this.f1;
		}
	}

	public static class Order extends Tuple3<Integer, String, Integer> {
		
		public Integer getOrderkey() {
			return this.f0;
		}

		public String getOrderdate() {
			return this.f1;
		}

		public Integer getShippriority() {
			return this.f2;
		}
	}

	public static class ShippingPriorityItem extends Tuple5<Integer, Double, String, Integer, Integer> {

		public ShippingPriorityItem() {
		}

		public ShippingPriorityItem(Integer l_orderkey, Double revenue,
				String o_orderdate, Integer o_shippriority, Integer o_orderkey) {
			this.f0 = l_orderkey;
			this.f1 = revenue;
			this.f2 = o_orderdate;
			this.f3 = o_shippriority;
			this.f4 = o_orderkey;
		}
		
		public Integer getL_Orderkey() {
			return this.f0;
		}

		public void setL_Orderkey(Integer l_orderkey) {
			this.f0 = l_orderkey;
		}

		public Double getRevenue() {
			return this.f1;
		}

		public void setRevenue(Double revenue) {
			this.f1 = revenue;
		}

		public String getOrderdate() {
			return this.f2;
		}

		public Integer getShippriority() {
			return this.f3;
		}

		public Integer getO_Orderkey() {
			return this.f4;
		}
	}
	
	
	// --------------------------------------------------------------------------------------------
	//  Query program
	// --------------------------------------------------------------------------------------------
	
	/*
	 * This example TPCH3 query uses custom objects.
	 */
	public static void main(String[] args) throws Exception {
		if (args.length < 3) {
			System.err.println("Parameters: <lineitem.tbl> <customer.tbl> <orders.tbl> [<output>].");
			return;
		}

		final String lineitemPath = args[0];
		final String customerPath = args[1];
		final String ordersPath = args[2];
		final String resultPath = args.length >= 4 ? args[4] : null;

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		/*
		 * Read Data from files
		 */
		DataSet<Lineitem> li = env
				.readCsvFile(lineitemPath).fieldDelimiter('|')
				.includeFields("1000011000100000")
				.tupleType(Lineitem.class);

		DataSet<Order> or = env
				.readCsvFile(ordersPath).fieldDelimiter('|')
				.includeFields("100010010")
				.tupleType(Order.class);
	
		DataSet<Customer> cust = env
				.readCsvFile(customerPath).fieldDelimiter('|')
				.includeFields("10000010")
				.tupleType(Customer.class);
		
		/*
		 * Filter market segment "AUTOMOBILE"
		 */
		cust = cust.filter(new FilterFunction<Customer>() {
			@Override
			public boolean filter(Customer value) {
				return value.getMktsegment().equals("AUTOMOBILE");
			}
		});

		/*
		 * Filter all Orders with o_orderdate < 12.03.1995
		 */
		or = or.filter(new FilterFunction<Order>() {
			private DateFormat format = new SimpleDateFormat("yyyy-MM-dd");
			private Date date;
			
			{
				Calendar cal = Calendar.getInstance();
				cal.set(1995, 3, 12);
				date = cal.getTime();
			}
			
			@Override
			public boolean filter(Order value) throws ParseException {
				Date orderDate = format.parse(value.getOrderdate());
				return orderDate.before(date);
			}
		});
		
		/*
		 * Filter all Lineitems with l_shipdate > 12.03.1995
		 */
		li = li.filter(new FilterFunction<Lineitem>() {
			private DateFormat format = new SimpleDateFormat("yyyy-MM-dd");
			private Date date;
			
			{
				Calendar cal = Calendar.getInstance();
				cal.set(1995, 3, 12);
				date = cal.getTime();
			}
			
			@Override
			public boolean filter(Lineitem value) throws ParseException {
				Date shipDate = format.parse(value.getShipdate());
				return shipDate.after(date);
			}
		});

		/*
		 * Join customers with orders and package them into a ShippingPriorityItem
		 */
		DataSet<ShippingPriorityItem> customerWithOrders = cust
				.join(or)
				.where(0)
				.equalTo(0)
				.with(new JoinFunction<Customer, Order, ShippingPriorityItem>() {
					@Override
					public ShippingPriorityItem join(Customer first, Order second) {
						return new ShippingPriorityItem(0, 0.0, second.getOrderdate(),
								second.getShippriority(), second.getOrderkey());
					}
				});
		
		/*
		 * Join the last join result with Orders
		 */
		DataSet<ShippingPriorityItem> joined = customerWithOrders
				.join(li)
				.where(4)
				.equalTo(0)
				.with(new JoinFunction<ShippingPriorityItem, Lineitem, ShippingPriorityItem>() {
					@Override
					public ShippingPriorityItem join(ShippingPriorityItem first, Lineitem second) {
						first.setL_Orderkey(second.getOrderkey());
						first.setRevenue(second.getExtendedprice() * (1 - second.getDiscount()));
						return first;
					}
				});
		
		/*
		 * GroupBy l_orderkey, o_orderdate and o_shippriority
		 * After that, the reduce function calculates the revenue.
		 */
		joined = joined
				.groupBy(0, 2, 3)
				.reduce(new ReduceFunction<TPCHQuery3.ShippingPriorityItem>() {	
					@Override
					public ShippingPriorityItem reduce(ShippingPriorityItem value1, ShippingPriorityItem value2) {
						value1.setRevenue(value1.getRevenue() + value2.getRevenue());
						return value1;
					}
				});
		
		if (resultPath == null) {
			joined.print();
		} else {
			joined.writeAsCsv(resultPath, "\n", "|");
		}
		
		env.execute();
	}
}
