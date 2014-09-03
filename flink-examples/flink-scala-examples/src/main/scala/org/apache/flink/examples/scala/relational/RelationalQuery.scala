/**
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


package org.apache.flink.examples.scala.relational;

import org.apache.flink.client.LocalExecutor
import org.apache.flink.api.common.Program
import org.apache.flink.api.common.ProgramDescription

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.operators._


/**
 * The TPC-H is a decision support benchmark on relational data.
 * Its documentation and the data generator (DBGEN) can be found
 * on http://www.tpc.org/tpch/ .
 * 
 * This Flink program implements a modified version of the query 3 of
 * the TPC-H benchmark including one join, some filtering and an
 * aggregation. The query resembles the following SQL statement:
 * <pre>
 * SELECT l_orderkey, o_shippriority, sum(l_extendedprice) as revenue
 *   FROM orders, lineitem
 *   WHERE l_orderkey = o_orderkey
 *     AND o_orderstatus = "X"
 *     AND YEAR(o_orderdate) > Y
 *     AND o_orderpriority LIKE "Z%"
 *   GROUP BY l_orderkey, o_shippriority;
 * </pre>
 */
class RelationalQuery extends Program with ProgramDescription with Serializable {

  case class Order(orderId: Int, status: Char, year: Int, orderPriority: String, shipPriority: Int)
  case class LineItem(orderId: Int, extendedPrice: Double)
  case class PrioritizedOrder(orderId: Int, shipPriority: Int, revenue: Double)
  
  
  def getScalaPlan(numSubTasks: Int, ordersInput: String, lineItemsInput: String, ordersOutput: String, status: Char = 'F', minYear: Int = 1993, priority: String = "5") = {
    
    // ORDER intput: parse as CSV and select relevant fields
    val orders = DataSource(ordersInput, CsvInputFormat[(Int, String, String, String, String, String, String, Int)]("\n", '|'))
                         .map { t => Order(t._1, t._3.charAt(0), t._5.substring(0,4).toInt, t._6, t._8) }
      
    // ORDER intput: parse as CSV and select relevant fields
    val lineItems = DataSource(lineItemsInput, CsvInputFormat[(Int, String, String, String, String, Double)]("\n", '|'))
                         .map { t => LineItem(t._1, t._6) }
    
    // filter the orders input
    val filteredOrders = orders filter { o => o.status == status && o.year > minYear && o.orderPriority.startsWith(priority) }
    
    // join the filteres result with the lineitem input
    val prioritizedItems = filteredOrders join lineItems where { _.orderId } isEqualTo { _.orderId } map { (o, li) => PrioritizedOrder(o.orderId, o.shipPriority, li.extendedPrice) }
    
    // group by and sum the joined data
    val prioritizedOrders = prioritizedItems groupBy { pi => (pi.orderId, pi.shipPriority) } reduce { (po1, po2) => po1.copy(revenue = po1.revenue + po2.revenue) }

    // write the result as csv
    val output = prioritizedOrders.write(ordersOutput, CsvOutputFormat("\n", "|"))

    val plan = new ScalaPlan(Seq(output), "Relational Query")
    plan.setDefaultParallelism(numSubTasks)
    plan
  }

  override def getDescription() = {
    "Parameters: <orders>, <lineitem>, <output>, <degree-of-parallelism>"
  }
  override def getPlan(args: String*) = {
    getScalaPlan(args(3).toInt, args(0), args(1), args(2))
  }
}


/**
 * Entry point to make the example standalone runnable with the local executor
 */
object RunRelationalQuery {
  
  def main(args: Array[String]) {
    val query = new RelationalQuery
    
    if (args.size < 4) {
      println(query.getDescription)
      return
    }
    val plan = query.getScalaPlan(args(3).toInt, args(0), args(1), args(2))
    LocalExecutor.execute(plan)
  }
}

