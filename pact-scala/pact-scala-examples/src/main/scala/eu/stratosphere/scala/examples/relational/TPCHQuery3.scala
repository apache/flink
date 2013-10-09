package eu.stratosphere.scala.examples.relational;
///**
// * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
// *
// * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
// * the License. You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// * specific language governing permissions and limitations under the License.
// */
//
//package eu.stratosphere.pact4s.examples.relational
//
//import eu.stratosphere.pact4s.common._
//import eu.stratosphere.pact4s.common.operators._
//
///**
// * The TPC-H is a decision support benchmark on relational data.
// * Its documentation and the data generator (DBGEN) can be found
// * on http://www.tpc.org/tpch/ .
// * The PACT program implements a modified version of the query 3 of
// * the TPC-H benchmark including one join, some filtering and an
// * aggregation.
// * SELECT l_orderkey, o_shippriority, sum(l_extendedprice) as revenue
// *   FROM orders, lineitem
// *   WHERE l_orderkey = o_orderkey
// *     AND o_orderstatus = "X"
// *     AND YEAR(o_orderdate) > Y
// *     AND o_orderpriority LIKE "Z%"
// *   GROUP BY l_orderkey, o_shippriority;
// */
//class TPCHQuery3Descriptor extends PactDescriptor[TPCHQuery3] {
//  override val name = "TPCH Query 3 (Immutable)"
//  override val parameters = "-orders <file> -lineItems <file> -output <file>"
//
//  override def createInstance(args: Pact4sArgs) = new TPCHQuery3(args("orders"), args("lineItems"), args("output"))
//}
//
//class TPCHQuery3(ordersInput: String, lineItemsInput: String, ordersOutput: String, status: Char = 'F', minYear: Int = 1993, priority: String = "5") extends PactProgram {
//
//  val orders = new DataSource(ordersInput, DelimetedDataSourceFormat(parseOrder))
//  val lineItems = new DataSource(lineItemsInput, DelimetedDataSourceFormat(parseLineItem))
//  val output = new DataSink(ordersOutput, DelimetedDataSinkFormat(formatOutput))
//
//  val filteredOrders = orders filter { o => o.status == status && o.year > minYear && o.orderPriority.startsWith(priority) }
//  val prioritizedItems = filteredOrders join lineItems on { _.orderId } isEqualTo { _.orderId } map { (o, li) => PrioritizedOrder(o.orderId, o.shipPriority, li.extendedPrice) }
//  val prioritizedOrders = prioritizedItems groupBy { pi => (pi.orderId, pi.shipPriority) } combine { _ reduce addRevenues }
//
//  override def outputs = output <~ prioritizedOrders
//
//  case class Order(orderId: Int, status: Char, year: Int, month: Int, day: Int, orderPriority: String, shipPriority: Int)
//  case class LineItem(orderId: Int, extendedPrice: Double)
//  case class PrioritizedOrder(orderId: Int, shipPriority: Int, revenue: Double)
//  
//  def addRevenues(po1: PrioritizedOrder, po2: PrioritizedOrder) = po1.copy(revenue = po1.revenue + po2.revenue)
//
//  def parseOrder = (line: String) => {
//    val OrderInputPattern = """(\d+)\|[^\|]+\|([^\|])\|[^\|]+\|(\d\d\d\d)-(\d\d)-(\d\d)\|([^\|]+)\|[^\|]+\|(\d+)\|[^\|]+\|""".r
//    val OrderInputPattern(orderId, status, year, month, day, oPr, sPr) = line
//    Order(orderId.toInt, status(0), year.toInt, month.toInt, day.toInt, oPr, sPr.toInt)
//  }
//
//  def parseLineItem = (line: String) => {
//    val LineItemInputPattern = """(\d+)\|[^\|]+\|[^\|]+\|[^\|]+\|[^\|]+\|(\d+\.\d\d)\|[^\|]+\|[^\|]+\|[^\|]\|[^\|]\|[^\|]+\|[^\|]+\|[^\|]+\|[^\|]+\|[^\|]+\|[^\|]+\|""".r
//    val LineItemInputPattern(orderId, price) = line
//    LineItem(orderId.toInt, price.toDouble)
//  }
//
//  def formatOutput = (item: PrioritizedOrder) => "%d|%d|%.2f".format(item.orderId, item.shipPriority, item.revenue)
//  
//  filteredOrders observes { o => (o.status, o.year, o.orderPriority) }
//
//  prioritizedItems.left neglects { o => o }
//  prioritizedItems.left preserves { o => (o.orderId, o.shipPriority) } as { pi => (pi.orderId, pi.shipPriority) }
//
//  prioritizedItems.right neglects { li => li }
//  prioritizedItems.right preserves { li => li.extendedPrice } as { pi => pi.revenue }
//
//  prioritizedOrders observes { po => po.revenue }
//  prioritizedOrders preserves { pi => (pi.orderId, pi.shipPriority) } as { po => (po.orderId, po.shipPriority) }
//
//  orders.avgBytesPerRecord(44).uniqueKey(_.orderId)
//  lineItems.avgBytesPerRecord(28)
//  filteredOrders.avgBytesPerRecord(44).avgRecordsEmittedPerCall(0.05f).uniqueKey(_.orderId)
//  prioritizedItems.avgBytesPerRecord(32)
//  prioritizedOrders.avgBytesPerRecord(32).avgRecordsEmittedPerCall(1)
//}
//
