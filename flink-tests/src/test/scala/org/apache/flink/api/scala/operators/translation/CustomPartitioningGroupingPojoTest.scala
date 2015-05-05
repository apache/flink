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

package org.apache.flink.api.scala.operators.translation

import org.apache.flink.api.java.io.DiscardingOutputFormat
import org.apache.flink.optimizer.util.CompilerTestBase
import org.junit.Assert._
import org.junit.Test
import org.apache.flink.api.scala._
import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.runtime.operators.shipping.ShipStrategyType
import org.apache.flink.optimizer.plan.SingleInputPlanNode
import scala.collection.immutable.Seq
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.common.InvalidProgramException


class CustomPartitioningGroupingPojoTest extends CompilerTestBase {

  @Test
  def testCustomPartitioningTupleReduce() {
    try {
      val env = ExecutionEnvironment.getExecutionEnvironment
      val data = env.fromElements(new Pojo2()).rebalance().setParallelism(4)
      
      data
          .groupBy("a").withPartitioner(new TestPartitionerInt())
          .reduce( (a,b) => a )
          .output(new DiscardingOutputFormat[Pojo2])
      
      val p = env.createProgramPlan()
      val op = compileNoStats(p)
      
      val sink = op.getDataSinks.iterator().next()
      val reducer = sink.getInput.getSource.asInstanceOf[SingleInputPlanNode]
      val combiner = reducer.getInput.getSource.asInstanceOf[SingleInputPlanNode]
      
      assertEquals(ShipStrategyType.FORWARD, sink.getInput.getShipStrategy)
      assertEquals(ShipStrategyType.PARTITION_CUSTOM, reducer.getInput.getShipStrategy)
      assertEquals(ShipStrategyType.FORWARD, combiner.getInput.getShipStrategy)
    }
    catch {
      case e: Exception => {
        e.printStackTrace()
        fail(e.getMessage)
      }
    }
  }
  
  @Test
  def testCustomPartitioningTupleGroupReduce() {
    try {
      val env = ExecutionEnvironment.getExecutionEnvironment
      
      val data = env.fromElements(new Pojo2()).rebalance().setParallelism(4)
      
      data
          .groupBy("a").withPartitioner(new TestPartitionerInt())
          .reduceGroup( iter => Seq(iter.next) )
          .output(new DiscardingOutputFormat[Seq[Pojo2]])
          
      val p = env.createProgramPlan()
      val op = compileNoStats(p)
      
      val sink = op.getDataSinks.iterator().next()
      val reducer = sink.getInput.getSource.asInstanceOf[SingleInputPlanNode]
      
      assertEquals(ShipStrategyType.FORWARD, sink.getInput.getShipStrategy)
      assertEquals(ShipStrategyType.PARTITION_CUSTOM, reducer.getInput.getShipStrategy)
    }
    catch {
      case e: Exception => {
        e.printStackTrace()
        fail(e.getMessage)
      }
    }
  }
  
  @Test
  def testCustomPartitioningTupleGroupReduceSorted() {
    try {
      val env = ExecutionEnvironment.getExecutionEnvironment
      
      val data = env.fromElements(new Pojo3()).rebalance().setParallelism(4)
      
      data
          .groupBy("a").withPartitioner(new TestPartitionerInt())
          .sortGroup("b", Order.ASCENDING)
          .reduceGroup( iter => Seq(iter.next) )
          .output(new DiscardingOutputFormat[Seq[Pojo3]])
          
      val p = env.createProgramPlan()
      val op = compileNoStats(p)
      
      val sink = op.getDataSinks.iterator().next()
      val reducer = sink.getInput.getSource.asInstanceOf[SingleInputPlanNode]
      
      assertEquals(ShipStrategyType.FORWARD, sink.getInput.getShipStrategy)
      assertEquals(ShipStrategyType.PARTITION_CUSTOM, reducer.getInput.getShipStrategy)
    }
    catch {
      case e: Exception => {
        e.printStackTrace()
        fail(e.getMessage)
      }
    }
  }
  
  @Test
  def testCustomPartitioningTupleGroupReduceSorted2() {
    try {
      val env = ExecutionEnvironment.getExecutionEnvironment
      
      val data = env.fromElements(new Pojo4()).rebalance().setParallelism(4)
      
      data
          .groupBy("a").withPartitioner(new TestPartitionerInt())
          .sortGroup("b", Order.ASCENDING)
          .sortGroup("c", Order.DESCENDING)
          .reduceGroup( iter => Seq(iter.next) )
          .output(new DiscardingOutputFormat[Seq[Pojo4]])
          
      val p = env.createProgramPlan()
      val op = compileNoStats(p)
      
      val sink = op.getDataSinks.iterator().next()
      val reducer = sink.getInput.getSource.asInstanceOf[SingleInputPlanNode]
      
      assertEquals(ShipStrategyType.FORWARD, sink.getInput.getShipStrategy)
      assertEquals(ShipStrategyType.PARTITION_CUSTOM, reducer.getInput.getShipStrategy)
    }
    catch {
      case e: Exception => {
        e.printStackTrace()
        fail(e.getMessage)
      }
    }
  }
  
  @Test
  def testCustomPartitioningTupleInvalidType() {
    try {
      val env = ExecutionEnvironment.getExecutionEnvironment
      
      val data = env.fromElements(new Pojo2()).rebalance().setParallelism(4)
      
      try {
        data.groupBy("a").withPartitioner(new TestPartitionerLong())
        fail("Should throw an exception")
      }
      catch {
        case e: InvalidProgramException => 
      }
    }
    catch {
      case e: Exception => {
        e.printStackTrace()
        fail(e.getMessage)
      }
    }
  }
  
  @Test
  def testCustomPartitioningTupleInvalidTypeSorted() {
    try {
      val env = ExecutionEnvironment.getExecutionEnvironment
    
      val data = env.fromElements(new Pojo3()).rebalance().setParallelism(4)
      
      try {
        data
            .groupBy("a")
            .sortGroup("b", Order.ASCENDING)
            .withPartitioner(new TestPartitionerLong())
        fail("Should throw an exception")
      }
      catch {
        case e: InvalidProgramException => 
      }
    }
    catch {
      case e: Exception => {
        e.printStackTrace()
        fail(e.getMessage)
      }
    }
  }
  
  @Test
  def testCustomPartitioningTupleRejectCompositeKey() {
    try {
      val env = ExecutionEnvironment.getExecutionEnvironment
      val data = env.fromElements(new Pojo2()).rebalance().setParallelism(4)
      try {
        data.groupBy("a", "b").withPartitioner(new TestPartitionerInt())
        fail("Should throw an exception")
      } catch {
        case e: InvalidProgramException => 
      }
    } catch {
      case e: Exception => {
        e.printStackTrace()
        fail(e.getMessage)
      }
    }
  }
  
  //-----------------------------------------------------------------------------------------------
  
  class Pojo2 {
  
    var a: Int = _
    var b: Int = _
  }
  
  class Pojo3 {
  
    var a: Int = _
    var b: Int = _
    var c: Int = _
  }
  
  class Pojo4 {
  
    var a: Int = _
    var b: Int = _
    var c: Int = _
    var d: Int = _
  }
  
  private class TestPartitionerInt extends Partitioner[Int] {
  
    override def partition(key: Int, numPartitions: Int): Int = 0
  }
  
  private class TestPartitionerLong extends Partitioner[Long] {
  
    override def partition(key: Long, numPartitions: Int): Int = 0
  }
}
