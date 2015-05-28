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
import org.apache.flink.api.common.operators.base.CoGroupOperatorBase
import org.junit.Ignore

class CoGroupGroupSortTranslationTest {

  @Test
  def testGroupSortTuples() {
    try {
      val env = ExecutionEnvironment.getExecutionEnvironment
      
      val input1 = env.fromElements( (0L, 0L) )
      val input2 = env.fromElements( (0L, 0L, 0L) )
      
      input1
          .coGroup(input2)
          .where(1).equalTo(2)
          .sortFirstGroup(0, Order.DESCENDING)
          .sortSecondGroup(1, Order.ASCENDING).sortSecondGroup(0, Order.DESCENDING) {
               (first, second) => first.buffered.head
            }
        .output(new DiscardingOutputFormat[(Long, Long)])
        
      val p = env.createProgramPlan()
      
      val sink = p.getDataSinks.iterator().next()
      val coGroup = sink.getInput.asInstanceOf[CoGroupOperatorBase[_, _, _, _]]
      
      assertNotNull(coGroup.getGroupOrderForInputOne)
      assertNotNull(coGroup.getGroupOrderForInputTwo)
      
      assertEquals(1, coGroup.getGroupOrderForInputOne.getNumberOfFields)
      assertEquals(0, coGroup.getGroupOrderForInputOne.getFieldNumber(0).intValue())
      assertEquals(Order.DESCENDING, coGroup.getGroupOrderForInputOne.getOrder(0))
      
      assertEquals(2, coGroup.getGroupOrderForInputTwo.getNumberOfFields)
      assertEquals(1, coGroup.getGroupOrderForInputTwo.getFieldNumber(0).intValue())
      assertEquals(0, coGroup.getGroupOrderForInputTwo.getFieldNumber(1).intValue())
      assertEquals(Order.ASCENDING, coGroup.getGroupOrderForInputTwo.getOrder(0))
      assertEquals(Order.DESCENDING, coGroup.getGroupOrderForInputTwo.getOrder(1))
    }
    catch {
      case e: Exception => {
        e.printStackTrace()
        fail(e.getMessage)
      }
    }
  }
  
  @Test
  def testSortTuplesAndPojos() {
    try {
      val env = ExecutionEnvironment.getExecutionEnvironment
      
      val input1 = env.fromElements(new Tuple2[Long, Long](0L, 0L))
      val input2 = env.fromElements(new CoGroupTestPoJo())
      
      input1
          .coGroup(input2)
          .where(1).equalTo("b")
          .sortFirstGroup(0, Order.DESCENDING)
          .sortSecondGroup("c", Order.ASCENDING).sortSecondGroup("a", Order.DESCENDING) {
               (first, second) => first.buffered.head
            }
          .output(new DiscardingOutputFormat[(Long, Long)])
          
      val p = env.createProgramPlan()
      
      val sink = p.getDataSinks.iterator().next()
      val coGroup = sink.getInput.asInstanceOf[CoGroupOperatorBase[_, _, _, _]]
      
      assertNotNull(coGroup.getGroupOrderForInputOne)
      assertNotNull(coGroup.getGroupOrderForInputTwo)

      assertEquals(1, coGroup.getGroupOrderForInputOne.getNumberOfFields)
      assertEquals(0, coGroup.getGroupOrderForInputOne.getFieldNumber(0).intValue())
      assertEquals(Order.DESCENDING, coGroup.getGroupOrderForInputOne.getOrder(0))
      
      assertEquals(2, coGroup.getGroupOrderForInputTwo.getNumberOfFields)
      assertEquals(2, coGroup.getGroupOrderForInputTwo.getFieldNumber(0).intValue())
      assertEquals(0, coGroup.getGroupOrderForInputTwo.getFieldNumber(1).intValue())
      assertEquals(Order.ASCENDING, coGroup.getGroupOrderForInputTwo.getOrder(0))
      assertEquals(Order.DESCENDING, coGroup.getGroupOrderForInputTwo.getOrder(1))
    }
    catch {
      case e: Exception => {
        e.printStackTrace()
        fail(e.getMessage)
      }
    }
  }
  
  @Test
  @Ignore
  def testGroupSortTuplesDefaultCoGroup() {
    try {
      val env = ExecutionEnvironment.getExecutionEnvironment
      
      val input1 = env.fromElements( (0L, 0L) )
      val input2 = env.fromElements( (0L, 0L, 0L) )
      
      input1
          .coGroup(input2)
          .where(1).equalTo(2)
          .sortFirstGroup(0, Order.DESCENDING)
          .sortSecondGroup(1, Order.ASCENDING).sortSecondGroup(0, Order.DESCENDING)
        .print()
        
      val p = env.createProgramPlan()
      
      val sink = p.getDataSinks.iterator().next()
      val coGroup = sink.getInput.asInstanceOf[CoGroupOperatorBase[_, _, _, _]]
      
      assertNotNull(coGroup.getGroupOrderForInputOne)
      assertNotNull(coGroup.getGroupOrderForInputTwo)
      
      assertEquals(1, coGroup.getGroupOrderForInputOne.getNumberOfFields)
      assertEquals(0, coGroup.getGroupOrderForInputOne.getFieldNumber(0).intValue())
      assertEquals(Order.DESCENDING, coGroup.getGroupOrderForInputOne.getOrder(0))
      
      assertEquals(2, coGroup.getGroupOrderForInputTwo.getNumberOfFields)
      assertEquals(1, coGroup.getGroupOrderForInputTwo.getFieldNumber(0).intValue())
      assertEquals(0, coGroup.getGroupOrderForInputTwo.getFieldNumber(1).intValue())
      assertEquals(Order.ASCENDING, coGroup.getGroupOrderForInputTwo.getOrder(0))
      assertEquals(Order.DESCENDING, coGroup.getGroupOrderForInputTwo.getOrder(1))
    }
    catch {
      case e: Exception => {
        e.printStackTrace()
        fail(e.getMessage)
      }
    }
  }
}

class CoGroupTestPoJo {
  
  var a: Long = _
  var b: Long = _
  var c: Long = _
}
