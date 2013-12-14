package eu.stratosphere.scala.examples.relational;
/**
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
 */


import eu.stratosphere.pact.client.LocalExecutor
import eu.stratosphere.api.plan.PlanAssembler
import eu.stratosphere.api.plan.PlanAssemblerDescription

import eu.stratosphere.scala._
import eu.stratosphere.scala.operators._

object RunTPCHQuery3 {
  def main(args: Array[String]) {
    val tpch3 = new TPCHQuery3
    if (args.size < 4) {
      println(tpch3.getDescription)
      return
    }
    val plan = tpch3.getScalaPlan(args(0).toInt, args(1), args(2), args(3))
    LocalExecutor.execute(plan)
    System.exit(0)
  }
}

/**
 * The TPC-H is a decision support benchmark on relational data.
 * Its documentation and the data generator (DBGEN) can be found
 * on http://www.tpc.org/tpch/ .
 * The PACT program implements a modified version of the query 3 of
 * the TPC-H benchmark including one join, some filtering and an
 * aggregation.
 * SELECT l_orderkey, o_shippriority, sum(l_extendedprice) as revenue
 *   FROM orders, lineitem
 *   WHERE l_orderkey = o_orderkey
 *     AND o_orderstatus = "X"
 *     AND YEAR(o_orderdate) > Y
 *     AND o_orderpriority LIKE "Z%"
 *   GROUP BY l_orderkey, o_shippriority;
 */
class TPCHQuery3 extends PlanAssembler with PlanAssemblerDescription with Serializable {
  override def getDescription() = {
    "Parameters: [numSubStasks], [orders], [lineitem], [output]"
  }
  override def getPlan(args: String*) = {
    getScalaPlan(args(0).toInt, args(1), args(2), args(3))
  }

  def getScalaPlan(numSubTasks: Int, ordersInput: String, lineItemsInput: String, ordersOutput: String, status: Char = 'F', minYear: Int = 1993, priority: String = "5") = {
    val orders = DataSource(ordersInput, DelimitedInputFormat(parseOrder))
    val lineItems = DataSource(lineItemsInput, DelimitedInputFormat(parseLineItem))

    val filteredOrders = orders filter { o => o.status == status && o.year > minYear && o.orderPriority.startsWith(priority) }
    val prioritizedItems = filteredOrders join lineItems where { _.orderId } isEqualTo { _.orderId } map { (o, li) => PrioritizedOrder(o.orderId, o.shipPriority, li.extendedPrice) }
    val prioritizedOrders = prioritizedItems groupBy { pi => (pi.orderId, pi.shipPriority) } reduceGroup { _ reduce addRevenues }

    val output = prioritizedOrders.write(ordersOutput, DelimitedOutputFormat(formatOutput))

    filteredOrders observes { o => (o.status, o.year, o.orderPriority) }

    prioritizedItems.left neglects { o => o }
    prioritizedItems.left preserves ({ o => (o.orderId, o.shipPriority) }, { pi => (pi.orderId, pi.shipPriority) })

    prioritizedItems.right neglects { li => li }
    prioritizedItems.right preserves ({ li => li.extendedPrice }, { pi => pi.revenue })

    prioritizedOrders observes { po => po.revenue }
    prioritizedOrders preserves ({ pi => (pi.orderId, pi.shipPriority) }, { po => (po.orderId, po.shipPriority) })

    orders.avgBytesPerRecord(44).uniqueKey(_.orderId)
    lineItems.avgBytesPerRecord(28)
    filteredOrders.avgBytesPerRecord(44).avgRecordsEmittedPerCall(0.05f).uniqueKey(_.orderId)
    prioritizedItems.avgBytesPerRecord(32)
    prioritizedOrders.avgBytesPerRecord(32).avgRecordsEmittedPerCall(1)

    val plan = new ScalaPlan(Seq(output), "TPCH Query 3 (Immutable)")
    plan.setDefaultParallelism(numSubTasks)
    plan
  }

  case class Order(orderId: Int, status: Char, year: Int, month: Int, day: Int, orderPriority: String, shipPriority: Int)
  case class LineItem(orderId: Int, extendedPrice: Double)
  case class PrioritizedOrder(orderId: Int, shipPriority: Int, revenue: Double)
  
  def addRevenues(po1: PrioritizedOrder, po2: PrioritizedOrder) = po1.copy(revenue = po1.revenue + po2.revenue)

  def parseOrder = (line: String) => {
    val OrderInputPattern = """(\d+)\|[^\|]+\|([^\|])\|[^\|]+\|(\d\d\d\d)-(\d\d)-(\d\d)\|([^\|]+)\|[^\|]+\|(\d+)\|[^\|]+\|""".r
    val OrderInputPattern(orderId, status, year, month, day, oPr, sPr) = line
    Order(orderId.toInt, status(0), year.toInt, month.toInt, day.toInt, oPr, sPr.toInt)
  }

  def parseLineItem = (line: String) => {
    val LineItemInputPattern = """(\d+)\|[^\|]+\|[^\|]+\|[^\|]+\|[^\|]+\|(\d+\.\d\d)\|[^\|]+\|[^\|]+\|[^\|]\|[^\|]\|[^\|]+\|[^\|]+\|[^\|]+\|[^\|]+\|[^\|]+\|[^\|]+\|""".r
    val LineItemInputPattern(orderId, price) = line
    LineItem(orderId.toInt, price.toDouble)
  }

  def formatOutput = (item: PrioritizedOrder) => "%d|%d|%.2f".format(item.orderId, item.shipPriority, item.revenue)
}

