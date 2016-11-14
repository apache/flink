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
import org.apache.flink.api.scala._
import org.apache.flink.optimizer.util.CompilerTestBase
import org.junit.Test
import org.junit.Assert._
import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.optimizer.plan.SingleInputPlanNode
import org.apache.flink.runtime.operators.shipping.ShipStrategyType
import org.apache.flink.api.common.InvalidProgramException


class CustomPartitioningTest extends CompilerTestBase {
  
  @Test
  def testPartitionTuples() {
    try {
      val part = new TestPartitionerInt()
      val parallelism = 4
      
      val env = ExecutionEnvironment.getExecutionEnvironment
      env.setParallelism(parallelism)
      env.getConfig.setMaxParallelism(parallelism);

      val data = env.fromElements( (0,0) ).rebalance()
      
      data.partitionCustom(part, 0)
          .mapPartition( x => x )
          .output(new DiscardingOutputFormat[(Int, Int)])

      val p = env.createProgramPlan()
      val op = compileNoStats(p)
      
      val sink = op.getDataSinks.iterator().next()
      val mapper = sink.getInput.getSource.asInstanceOf[SingleInputPlanNode]
      val partitioner = mapper.getInput.getSource.asInstanceOf[SingleInputPlanNode]
      val balancer = partitioner.getInput.getSource.asInstanceOf[SingleInputPlanNode]
      
      assertEquals(ShipStrategyType.FORWARD, sink.getInput.getShipStrategy)
      assertEquals(parallelism, sink.getParallelism)
      
      assertEquals(ShipStrategyType.FORWARD, mapper.getInput.getShipStrategy)
      assertEquals(parallelism, mapper.getParallelism)
      
      assertEquals(ShipStrategyType.PARTITION_CUSTOM, partitioner.getInput.getShipStrategy)
      assertEquals(part, partitioner.getInput.getPartitioner)
      assertEquals(parallelism, partitioner.getParallelism)
      
      assertEquals(ShipStrategyType.PARTITION_FORCED_REBALANCE, balancer.getInput.getShipStrategy)
      assertEquals(parallelism, balancer.getParallelism)
    }
    catch {
      case e: Exception => {
        e.printStackTrace()
        fail(e.getMessage)
      }
    }
  }
  
  @Test
  def testPartitionTuplesInvalidType() {
    try {
      val parallelism = 4
      
      val env = ExecutionEnvironment.getExecutionEnvironment
      env.setParallelism(parallelism)
      
      val data = env.fromElements( (0,0) ).rebalance()
      try {
        data.partitionCustom(new TestPartitionerLong(), 0)
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
  def testPartitionPojo() {
    try {
      val part = new TestPartitionerInt()
      val parallelism = 4
      
      val env = ExecutionEnvironment.getExecutionEnvironment
      env.setParallelism(parallelism)
      env.getConfig.setMaxParallelism(parallelism);

      val data = env.fromElements(new Pojo()).rebalance()
      
      data
          .partitionCustom(part, "a")
          .mapPartition( x => x)
          .output(new DiscardingOutputFormat[Pojo])
          
      val p = env.createProgramPlan()
      val op = compileNoStats(p)
      
      val sink = op.getDataSinks.iterator().next()
      val mapper = sink.getInput.getSource.asInstanceOf[SingleInputPlanNode]
      val partitioner = mapper.getInput.getSource.asInstanceOf[SingleInputPlanNode]
      val balancer = partitioner.getInput.getSource.asInstanceOf[SingleInputPlanNode]
      
      assertEquals(ShipStrategyType.FORWARD, sink.getInput.getShipStrategy)
      assertEquals(parallelism, sink.getParallelism)
      
      assertEquals(ShipStrategyType.FORWARD, mapper.getInput.getShipStrategy)
      assertEquals(parallelism, mapper.getParallelism)
      
      assertEquals(ShipStrategyType.PARTITION_CUSTOM, partitioner.getInput.getShipStrategy)
      assertEquals(part, partitioner.getInput.getPartitioner)
      assertEquals(parallelism, partitioner.getParallelism)
      
      assertEquals(ShipStrategyType.PARTITION_FORCED_REBALANCE, balancer.getInput.getShipStrategy)
      assertEquals(parallelism, balancer.getParallelism)
    }
    catch {
      case e: Exception => {
        e.printStackTrace()
        fail(e.getMessage)
      }
    }
  }
  
  @Test
  def testPartitionPojoInvalidType() {
    try {
      val parallelism = 4
      
      val env = ExecutionEnvironment.getExecutionEnvironment
      env.setParallelism(parallelism)
      
      val data = env.fromElements(new Pojo()).rebalance()
      
      try {
        data.partitionCustom(new TestPartitionerLong(), "a")
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
  def testPartitionKeySelector() {
    try {
      val part = new TestPartitionerInt()
      val parallelism = 4
      
      val env = ExecutionEnvironment.getExecutionEnvironment
      env.setParallelism(parallelism)
      env.getConfig.setMaxParallelism(parallelism);
      
      val data = env.fromElements(new Pojo()).rebalance()
      
      data
          .partitionCustom(part, pojo => pojo.a)
          .mapPartition( x => x)
          .output(new DiscardingOutputFormat[Pojo])
          
      val p = env.createProgramPlan()
      val op = compileNoStats(p)
      
      val sink = op.getDataSinks.iterator().next()
      val mapper = sink.getInput.getSource.asInstanceOf[SingleInputPlanNode]
      val keyRemover = mapper.getInput.getSource.asInstanceOf[SingleInputPlanNode]
      val partitioner = keyRemover.getInput.getSource.asInstanceOf[SingleInputPlanNode]
      val keyExtractor = partitioner.getInput.getSource.asInstanceOf[SingleInputPlanNode]
      val balancer = keyExtractor.getInput.getSource.asInstanceOf[SingleInputPlanNode]
      
      assertEquals(ShipStrategyType.FORWARD, sink.getInput.getShipStrategy)
      assertEquals(parallelism, sink.getParallelism)
      
      assertEquals(ShipStrategyType.FORWARD, mapper.getInput.getShipStrategy)
      assertEquals(parallelism, mapper.getParallelism)
      
      assertEquals(ShipStrategyType.FORWARD, keyRemover.getInput.getShipStrategy)
      assertEquals(parallelism, keyRemover.getParallelism)
      
      assertEquals(ShipStrategyType.PARTITION_CUSTOM, partitioner.getInput.getShipStrategy)
      assertEquals(part, partitioner.getInput.getPartitioner)
      assertEquals(parallelism, partitioner.getParallelism)
      
      assertEquals(ShipStrategyType.FORWARD, keyExtractor.getInput.getShipStrategy)
      assertEquals(parallelism, keyExtractor.getParallelism)
      
      assertEquals(ShipStrategyType.PARTITION_FORCED_REBALANCE, balancer.getInput.getShipStrategy)
      assertEquals(parallelism, balancer.getParallelism)
    }
    catch {
      case e: Exception => {
        e.printStackTrace()
        fail(e.getMessage)
      }
    }
  }
  
  // ----------------------------------------------------------------------------------------------
  
  class Pojo {
  
    var a: Int = _
    var b: Long = _
  }
  
  class TestPartitionerInt extends Partitioner[Int] {
  
    override def partition(key: Int, numPartitions: Int): Int = 0
  }
  
  class TestPartitionerLong extends Partitioner[Long] {
  
    override def partition(key: Long, numPartitions: Int): Int = 0
  }
}
