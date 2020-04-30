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
import org.junit.Assert._
import org.junit.Test

import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.api.scala._
import org.apache.flink.optimizer.util.CompilerTestBase
import org.apache.flink.runtime.operators.shipping.ShipStrategyType
import org.apache.flink.optimizer.plan.SingleInputPlanNode
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.common.InvalidProgramException


class CustomPartitioningGroupingTupleTest extends CompilerTestBase {

  @Test
  def testCustomPartitioningTupleAgg() {
    try {
      val env = ExecutionEnvironment.getExecutionEnvironment
      
      val data = env.fromElements((0, 0)).rebalance()
      
      data.groupBy(0)
          .withPartitioner(new TestPartitionerInt())
          .sum(1)
          .output(new DiscardingOutputFormat[(Int, Int)])
      
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
  def testCustomPartitioningTupleReduce() {
    try {
      val env = ExecutionEnvironment.getExecutionEnvironment
      
      val data = env.fromElements( (0, 0) ).rebalance().setParallelism(4)
      
      data
          .groupBy(0).withPartitioner(new TestPartitionerInt())
          .reduce( (a,b) => a )
          .output(new DiscardingOutputFormat[(Int, Int)])
          
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
      
      val data = env.fromElements( (0, 0) ).rebalance().setParallelism(4)
      
      data
          .groupBy(0).withPartitioner(new TestPartitionerInt())
          .reduceGroup( iter => Seq(iter.next) )
          .output(new DiscardingOutputFormat[Seq[(Int, Int)]])
      
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
      
      val data = env.fromElements( (0, 0, 0) ).rebalance().setParallelism(4)
      
      data
          .groupBy(0).withPartitioner(new TestPartitionerInt())
          .sortGroup(1, Order.ASCENDING)
          .reduceGroup( iter => Seq(iter.next) )
          .output(new DiscardingOutputFormat[Seq[(Int, Int, Int)]])
          
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
      
      val data = env.fromElements( (0, 0, 0, 0) ).rebalance().setParallelism(4)
      
      data
          .groupBy(0).withPartitioner(new TestPartitionerInt())
          .sortGroup(1, Order.ASCENDING)
          .sortGroup(2, Order.DESCENDING)
          .reduceGroup( iter => Seq(iter.next) )
          .output(new DiscardingOutputFormat[Seq[(Int, Int, Int, Int)]])
          
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
      
      val data = env.fromElements( (0, 0) ).rebalance().setParallelism(4)
      
      try {
        data.groupBy(0).withPartitioner(new TestPartitionerLong())
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
      val data = env.fromElements( (0, 0, 0) ).rebalance().setParallelism(4)
      try {
        data.groupBy(0).sortGroup(1, Order.ASCENDING).withPartitioner(new TestPartitionerLong())
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
  
  @Test
  def testCustomPartitioningTupleRejectCompositeKey() {
    try {
      val env = ExecutionEnvironment.getExecutionEnvironment
      
      val data = env.fromElements( (0, 0, 0) ).rebalance().setParallelism(4)
      try {
        data.groupBy(0, 1).withPartitioner(new TestPartitionerInt())
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
  
  // ----------------------------------------------------------------------------------------------
  
  class TestPartitionerInt extends Partitioner[Int] {
  
    override def partition(key: Int, numPartitions: Int): Int = 0
  }
  
  class TestPartitionerLong extends Partitioner[Long] {
  
    override def partition(key: Long, numPartitions: Int): Int = 0
  }
}
