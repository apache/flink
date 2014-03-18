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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.functions.FilterFunction;
import eu.stratosphere.api.java.functions.JoinFunction;
import eu.stratosphere.api.java.functions.MapFunction;
import eu.stratosphere.api.java.functions.ReduceFunction;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.api.java.tuple.Tuple4;
import eu.stratosphere.api.java.tuple.Tuple5;
import eu.stratosphere.api.java.tuple.Tuple6;

/**
 * This program implements a modified version of the TPC-H query 10.
 * 
 * The original query can be found at
 * http://www.tpc.org/tpch/spec/tpch2.16.0.pdf (page 45).
 * 
 * This program implements the following SQL equivalent:
 * 
 * SELECT 
 *        c_custkey, 
 *        c_name, 
 *        c_address,
 *        n_name, 
 *        c_acctbal
 *        sum(l_extendedprice * (1 - l_discount)) as revenue,  
 * FROM   
 *        customer, 
 *        orders, 
 *        lineitem, 
 *        nation 
 * WHERE 
 *        c_custkey = o_custkey 
 *        AND l_orderkey = o_orderkey 
 *        AND YEAR(o_orderdate) > '1990' 
 *        AND l_returnflag = 'R' 
 *        AND c_nationkey = n_nationkey 
 * GROUP BY 
 *        c_custkey, 
 *        c_name, 
 *        c_acctbal, 
 *        n_name, 
 *        c_address
 *        
 * Compared to the original TPC-H query this version does not print 
 * c_phone and c_comment, only filters by years greater than 1990 instead of  
 * a period of 3 months, and does not sort the result by revenue.
 * 
 */
public class TPCHQuery10 {
	
	public static void main(String[] args) throws Exception {
		
		String customerPath = (args.length > 0 ? args[0] : copyJarFileToTmp("/Testdata/customer.tbl", "customer.tbl"));
		String ordersPath =  (args.length > 1 ? args[1] : copyJarFileToTmp("/Testdata/orders.tbl", "orders.tbl"));
		String lineitemPath =  (args.length > 2 ? args[2] : copyJarFileToTmp("/Testdata/lineitem.tbl", "lineitem.tbl"));
		String nationPath =  (args.length > 3 ? args[3] : copyJarFileToTmp("/Testdata/nation.tbl", "nation.tbl"));		
		String outPath = (args.length > 4 ? args[4] : null);
		
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// read in customer table file
		// customer: custkey, name, address, nationkey, acctbal 
		DataSet<Tuple5<Integer, String, String, Integer, Double>> customers = env.readCsvFile(customerPath).fieldDelimiter('|')
				.includeFields("11110100").types(Integer.class, String.class, String.class, Integer.class, Double.class);

		// read in orders table file
		// order: orderkey, custkey, orderdate
		DataSet<Tuple3<Integer, Integer, String>> orders = env.readCsvFile(ordersPath).fieldDelimiter('|').includeFields("110010000")
				.types(Integer.class, Integer.class, String.class);

		// read in lineitem table file
		// lineitem: orderkey, extendedprice, discount, returnflag
		DataSet<Tuple4<Integer, Double, Double, String>> lineitems = env.readCsvFile(lineitemPath).fieldDelimiter('|')
				.includeFields("1000011010000000").types(Integer.class, Double.class, Double.class, String.class);

		// read in nation table file
		// nation: nationkey, name
		DataSet<Tuple2<Integer, String>> nations = env.readCsvFile(nationPath).fieldDelimiter('|').includeFields("1100")
				.types(Integer.class, String.class);

		// orders filtered by year: orderkey, custkey
		DataSet<Tuple2<Integer, Integer>> ordersFilteredByYear = orders
				// filter by year
				.filter(new FilterFunction<Tuple3<Integer,Integer, String>>() {
					private static final long serialVersionUID = 1L;
					private static final int YEAR_FILTER = 1990;
					
					@Override
					public boolean filter(Tuple3<Integer, Integer, String> t) throws Exception {
						int year = Integer.parseInt(t.T3().substring(0, 4));
						return year > YEAR_FILTER;
					}
				})
				// remove date as it is not necessary anymore
				.map(new MapFunction<Tuple3<Integer,Integer,String>, Tuple2<Integer, Integer>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Integer, Integer> map(Tuple3<Integer, Integer, String> t) throws Exception {
						return new Tuple2<Integer, Integer>(t.T1(), t.T2());
					}
				});

		// lineitems filtered by flag: orderkey, extendedprice, discount
		DataSet<Tuple3<Integer, Double, Double>> lineitemsFilteredByFlag = lineitems
				// filter by flag
				.filter(new FilterFunction<Tuple4<Integer, Double, Double, String>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public boolean filter(Tuple4<Integer, Double, Double, String> t)
							throws Exception {
						return t.T4().equals("R");
					}
				})
				// remove flag as it is not necessary anymore
				.map(new MapFunction<Tuple4<Integer, Double, Double, String>, Tuple3<Integer, Double, Double>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple3<Integer, Double, Double> map(Tuple4<Integer, Double, Double, String> t) throws Exception {
						return new Tuple3<Integer, Double, Double>(t.T1(), t.T2(), t.T3());
					}
				});

		// join orders with lineitems
		// custkey, extendedprice, discount
		DataSet<Tuple3<Integer, Double, Double>> lineitemsOfCustomerKey = ordersFilteredByYear.joinWithHuge(lineitemsFilteredByFlag)
				.where(0).equalTo(0)
				.with(new JoinFunction<Tuple2<Integer, Integer>, Tuple3<Integer, Double, Double>, Tuple3<Integer, Double, Double>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple3<Integer, Double, Double> join(Tuple2<Integer, Integer> o, Tuple3<Integer, Double, Double> l) {
						return new Tuple3<Integer, Double, Double>(o.T2(), l.T2(), l.T3());
					}
				});

		// aggregate for revenue
		// custkey, revenue
		DataSet<Tuple2<Integer, Double>> revenueOfCustomerKey = lineitemsOfCustomerKey
				// calculate the revenue for each item
				.map(new MapFunction<Tuple3<Integer, Double, Double>, Tuple2<Integer, Double>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Integer, Double> map(Tuple3<Integer, Double, Double> t) throws Exception {
						// revenue per item = l_extendedprice * (1 - l_discount)
						return new Tuple2<Integer, Double>(t.T1(), t.T2() * (1 - t.T3()));
					}
				})
				// aggregate the revenues per item to revenue per customer
				.groupBy(0).reduce(new ReduceFunction<Tuple2<Integer,Double>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Integer, Double> reduce(Tuple2<Integer, Double> t1, Tuple2<Integer, Double> t2) throws Exception {
						return new Tuple2<Integer, Double>(t1.T1(), t1.T2()+t2.T2());
					}
				});

		// join customer with nation
		// custkey, name, address, nationname, acctbal
		DataSet<Tuple5<Integer, String, String, String, Double>> customerWithNation = customers
				.joinWithTiny(nations)
				.where(3)
				.equalTo(0)
				.with(new JoinFunction<Tuple5<Integer, String, String, Integer, Double>, Tuple2<Integer, String>, Tuple5<Integer, String, String, String, Double>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple5<Integer, String, String, String, Double> join(Tuple5<Integer, String, String, Integer, Double> c,
							Tuple2<Integer, String> n) throws Exception {
						return new Tuple5<Integer, String, String, String, Double>(c.T1(), c.T2(), c.T3(), n.T2(), c.T5());
					}
				});

		// join customer (with nation) with revenue
		// custkey, name, address, nationname, acctbal, revenue
		DataSet<Tuple6<Integer, String, String, String, Double, Double>> customerWithRevenue = customerWithNation
				.join(revenueOfCustomerKey)
				.where(0)
				.equalTo(0)
				.with(new JoinFunction<Tuple5<Integer, String, String, String, Double>, Tuple2<Integer, Double>, Tuple6<Integer, String, String, String, Double, Double>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple6<Integer, String, String, String, Double, Double> join(Tuple5<Integer, String, String, String, Double> c,
							Tuple2<Integer, Double> r) throws Exception {
						return new Tuple6<Integer, String, String, String, Double, Double>(c.T1(), c.T2(), c.T3(), c.T4(), c.T5(), r.T2());
					}
				});

		// print the result and execute
		if (outPath == null) {
			customerWithRevenue.print();
		} else {
			customerWithRevenue.writeAsCsv(outPath);
		}
		env.execute();
	}
	
	// Helper function to extract a testdata file from jar to the default tmp-directory
	private static String copyJarFileToTmp(String jarPath, String tmpName) throws IOException {
		File tmpFile = new File(System.getProperty("java.io.tmpdir"), tmpName);
		
		InputStream is = TPCHQuery10.class.getResourceAsStream(jarPath);
		OutputStream os = new FileOutputStream(tmpFile);
		int read = 0;
		byte[] bytes = new byte[1024];
 
		while ((read = is.read(bytes)) != -1) {
			os.write(bytes, 0, read);
		}
		is.close();
		os.close();
		return "file://"+tmpFile.toString();
	}
}
