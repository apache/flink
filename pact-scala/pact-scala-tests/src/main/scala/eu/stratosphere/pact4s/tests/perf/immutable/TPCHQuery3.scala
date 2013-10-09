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

package eu.stratosphere.pact4s.tests.perf.immutable

import eu.stratosphere.pact4s.common._
import eu.stratosphere.pact4s.common.operators._

class TPCHQuery3Descriptor extends PactDescriptor[TPCHQuery3] {
  override val name = "TPCH Query 3 (Immutable)"
  override val parameters = "-orders <file> -lineItems <file> -output <file>"

  override def createInstance(args: Pact4sArgs) = new TPCHQuery3(args("orders"), args("lineItems"), args("output"))
}

class TPCHQuery3(ordersInput: String, lineItemsInput: String, ordersOutput: String, status: String = "F", minYear: Int = 1993, priority: String = "5") extends PactProgram {

  import TPCHQuery3._
  
  val orders = new DataSource(ordersInput, new RecordDataSourceFormat[Order]("\n", "|"))
  val lineItems = new DataSource(lineItemsInput, new RecordDataSourceFormat[LineItem]("\n", "|"))
  val output = new DataSink(ordersOutput, new RecordDataSinkFormat[PrioritizedOrder]("\n", "|", true))

  val filteredOrders = orders filter { o => o.status == status && o.date.substring(0, 4).toInt > minYear && o.orderPriority.startsWith(priority) }
  
  val prioritizedItems = filteredOrders join lineItems on { _.orderId } isEqualTo { _.orderId } map { (o, li) => PrioritizedOrder(o.orderId, o.shipPriority, li.extendedPrice) }
  
  val prioritizedOrders = prioritizedItems groupBy { pi => (pi.orderId, pi.shipPriority) } combine { pis =>
    
    var pi: PrioritizedOrder = null
    var revenue = 0d
    
    while (pis.hasNext) {
      pi = pis.next
      revenue += pi.revenue
    }
    
    PrioritizedOrder(pi.orderId, pi.shipPriority, revenue)
  }

  override def outputs = output <~ prioritizedOrders
 
  filteredOrders observes { o => (o.status, o.date, o.orderPriority) }

  prioritizedItems.left neglects { o => o }
  prioritizedItems.left preserves { o => (o.orderId, o.shipPriority) } as { pi => (pi.orderId, pi.shipPriority) }

  prioritizedItems.right neglects { li => li }
  prioritizedItems.right preserves { li => li.extendedPrice } as { pi => pi.revenue }

  prioritizedOrders observes { po => po.revenue }
  prioritizedOrders preserves { pi => (pi.orderId, pi.shipPriority) } as { po => (po.orderId, po.shipPriority) }

  orders.avgBytesPerRecord(16).uniqueKey(_.orderId)
  orders.cardinality(_.orderId, avgNumRecords = 1)
  
  lineItems.avgBytesPerRecord(20)
  lineItems.cardinality(_.orderId, avgNumRecords = 4)
  
  filteredOrders.avgBytesPerRecord(16).avgRecordsEmittedPerCall(0.05f)
  filteredOrders.cardinality(_.orderId, avgNumRecords = 1)
  
  prioritizedItems.avgBytesPerRecord(24)
  prioritizedItems.cardinality({ pi => (pi.orderId, pi.shipPriority) }, avgNumRecords = 4)
  
  prioritizedOrders.avgBytesPerRecord(30).avgRecordsEmittedPerCall(1)
  prioritizedOrders.cardinality({ pi => (pi.orderId, pi.shipPriority) }, avgNumRecords = 1)
}

object TPCHQuery3 {
  case class Order(val orderId: Long, val _1: String, val status: String, val _3: String, val date: String, val orderPriority: String, val _6: String, val shipPriority: String, val _8: String)
  case class LineItem(val orderId: Long, val _1: String, val _2: String, val _3: String, val _4: String, val extendedPrice: Double, val _6: String, val _7: String, val _8: String, val _9: String, val _10: String, val _11: String, val _12: String, val _13: String, val _14: String, val _15: String)
  case class PrioritizedOrder(val orderId: Long, val shipPriority: String, val revenue: Double)
}

