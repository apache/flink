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
import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.api.scala._
import org.apache.flink.runtime.operators.shipping.ShipStrategyType
import org.apache.flink.optimizer.plan.SingleInputPlanNode
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.common.InvalidProgramException
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint
import org.apache.flink.optimizer.plan.DualInputPlanNode

class CoGroupCustomPartitioningTest extends CompilerTestBase {
  
  @Test
  def testCoGroupWithTuples() {
    try {
      val partitioner = new TestPartitionerLong()
      
      val env = ExecutionEnvironment.getExecutionEnvironment
      
      val input1 = env.fromElements( (0L, 0L) )
      val input2 = env.fromElements( (0L, 0L, 0L) )
      
      input1
          .coGroup(input2)
          .where(1).equalTo(0)
          .withPartitioner(partitioner)
        .output(new DiscardingOutputFormat[(Array[(Long, Long)], Array[(Long, Long, Long)])])
      
      val p = env.createProgramPlan()
      val op = compileNoStats(p)
      
      val sink = op.getDataSinks.iterator().next()
      val join = sink.getInput.getSource.asInstanceOf[DualInputPlanNode]
      
      assertEquals(ShipStrategyType.PARTITION_CUSTOM, join.getInput1.getShipStrategy)
      assertEquals(ShipStrategyType.PARTITION_CUSTOM, join.getInput2.getShipStrategy)
      assertEquals(partitioner, join.getInput1.getPartitioner)
      assertEquals(partitioner, join.getInput2.getPartitioner)
    }
    catch {
      case e: Exception => {
        e.printStackTrace()
        fail(e.getMessage)
      }
    }
  }
  
  @Test
  def testCoGroupWithTuplesWrongType() {
    try {
      val partitioner = new TestPartitionerInt()
      
      val env = ExecutionEnvironment.getExecutionEnvironment
      
      val input1 = env.fromElements( (0L, 0L) )
      val input2 = env.fromElements( (0L, 0L, 0L) )
      
      try {
        input1
            .coGroup(input2)
            .where(1).equalTo(0)
            .withPartitioner(partitioner)
        fail("should throw an exception")
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
  def testCoGroupWithPojos() {
    try {
      val partitioner = new TestPartitionerInt()
      
      val env = ExecutionEnvironment.getExecutionEnvironment
      
      val input1 = env.fromElements(new Pojo2())
      val input2 = env.fromElements(new Pojo3())
      
      input1
          .coGroup(input2)
          .where("b").equalTo("a")
          .withPartitioner(partitioner)
        .output(new DiscardingOutputFormat[(Array[Pojo2], Array[Pojo3])])
        
      val p = env.createProgramPlan()
      val op = compileNoStats(p)
      
      val sink = op.getDataSinks.iterator().next()
      val join = sink.getInput.getSource.asInstanceOf[DualInputPlanNode]
      
      assertEquals(ShipStrategyType.PARTITION_CUSTOM, join.getInput1.getShipStrategy)
      assertEquals(ShipStrategyType.PARTITION_CUSTOM, join.getInput2.getShipStrategy)
      assertEquals(partitioner, join.getInput1.getPartitioner)
      assertEquals(partitioner, join.getInput2.getPartitioner)
    }
    catch {
      case e: Exception => {
        e.printStackTrace()
        fail(e.getMessage)
      }
    }
  }
  
  @Test
  def testCoGroupWithPojosWrongType() {
    try {
      val partitioner = new TestPartitionerLong()
      
      val env = ExecutionEnvironment.getExecutionEnvironment
      
      val input1 = env.fromElements(new Pojo2())
      val input2 = env.fromElements(new Pojo3())
      
      try {
        input1
            .coGroup(input2)
            .where("a").equalTo("b")
            .withPartitioner(partitioner)
        fail("should throw an exception")
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
  def testCoGroupWithKeySelectors() {
    try {
      val partitioner = new TestPartitionerInt()
      
      val env = ExecutionEnvironment.getExecutionEnvironment
      
      val input1 = env.fromElements(new Pojo2())
      val input2 = env.fromElements(new Pojo3())
      
      input1
          .coGroup(input2)
          .where( _.a ).equalTo( _.b )
          .withPartitioner(partitioner)
        .output(new DiscardingOutputFormat[(Array[Pojo2], Array[Pojo3])])
          
      val p = env.createProgramPlan()
      val op = compileNoStats(p)
      
      val sink = op.getDataSinks.iterator().next()
      val join = sink.getInput.getSource.asInstanceOf[DualInputPlanNode]
      
      assertEquals(ShipStrategyType.PARTITION_CUSTOM, join.getInput1.getShipStrategy)
      assertEquals(ShipStrategyType.PARTITION_CUSTOM, join.getInput2.getShipStrategy)
      assertEquals(partitioner, join.getInput1.getPartitioner)
      assertEquals(partitioner, join.getInput2.getPartitioner)
    }
    catch {
      case e: Exception => {
        e.printStackTrace()
        fail(e.getMessage)
      }
    }
  }
  
  @Test
  def testCoGroupWithKeySelectorsWrongType() {
    try {
      val partitioner = new TestPartitionerLong()
      
      val env = ExecutionEnvironment.getExecutionEnvironment
      
      val input1 = env.fromElements(new Pojo2())
      val input2 = env.fromElements(new Pojo3())
      
      try {
        input1
            .coGroup(input2)
            .where( _.a ).equalTo( _.b )
            .withPartitioner(partitioner)
        fail("should throw an exception")
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
}
