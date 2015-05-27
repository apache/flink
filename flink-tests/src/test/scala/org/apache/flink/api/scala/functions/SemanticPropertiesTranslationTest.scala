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
package org.apache.flink.api.scala.functions

import org.apache.flink.api.java.io.DiscardingOutputFormat
import org.junit.Assert._
import org.apache.flink.api.common.functions.RichJoinFunction
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.operators.{GenericDataSinkBase, SingleInputSemanticProperties}
import org.apache.flink.api.common.operators.base.{JoinOperatorBase, MapOperatorBase}
import org.apache.flink.api.common.operators.util.FieldSet
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsFirst
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsSecond
import org.junit.Test

import org.apache.flink.api.scala._

/**
 * This is a minimal test to verify that semantic annotations are evaluated against
 * the type information properly translated correctly to the common data flow API.
 *
 * This covers only the constant fields annotations currently !!!
 */
class SemanticPropertiesTranslationTest {
  /**
   * A mapper that preserves all fields over a tuple data set.
   */
  @Test
  def translateUnaryFunctionAnnotationTuplesWildCard(): Unit = {
    try {
      val env = ExecutionEnvironment.getExecutionEnvironment

      val input = env.fromElements((3L, "test", 42))
      input.map(new WildcardForwardMapper[(Long, String, Int)])
        .output(new DiscardingOutputFormat[(Long, String, Int)])

      val plan = env.createProgramPlan()

      val sink: GenericDataSinkBase[_] = plan.getDataSinks.iterator.next

      val mapper: MapOperatorBase[_, _, _] = sink.getInput.asInstanceOf[MapOperatorBase[_, _, _]]

      val semantics: SingleInputSemanticProperties = mapper.getSemanticProperties
      val fw1: FieldSet = semantics.getForwardingTargetFields(0, 0)
      val fw2: FieldSet = semantics.getForwardingTargetFields(0, 1)
      val fw3: FieldSet = semantics.getForwardingTargetFields(0, 2)

      assertNotNull(fw1)
      assertNotNull(fw2)
      assertNotNull(fw3)
      assertTrue(fw1.contains(0))
      assertTrue(fw2.contains(1))
      assertTrue(fw3.contains(2))
    } catch {
      case e: Exception => {
        System.err.println(e.getMessage)
        e.printStackTrace()
        fail("Exception in test: " + e.getMessage)
      }
    }
  }

  /**
   * A mapper that preserves fields 0, 1, 2 of a tuple data set.
   */
  @Test
  def translateUnaryFunctionAnnotationTuples1(): Unit = {
    try {
      val env = ExecutionEnvironment.getExecutionEnvironment

      val input = env.fromElements((3L, "test", 42))
      input.map(new IndividualForwardMapper[Long, String, Int])
        .output(new DiscardingOutputFormat[(Long, String, Int)])

      val plan = env.createProgramPlan()

      val sink: GenericDataSinkBase[_] = plan.getDataSinks.iterator.next

      val mapper: MapOperatorBase[_, _, _] = sink.getInput.asInstanceOf[MapOperatorBase[_, _, _]]

      val semantics: SingleInputSemanticProperties = mapper.getSemanticProperties
      val fw1: FieldSet = semantics.getForwardingTargetFields(0, 0)
      val fw2: FieldSet = semantics.getForwardingTargetFields(0, 1)
      val fw3: FieldSet = semantics.getForwardingTargetFields(0, 2)

      assertNotNull(fw1)
      assertNotNull(fw2)
      assertNotNull(fw3)
      assertTrue(fw1.contains(0))
      assertTrue(fw2.contains(1))
      assertTrue(fw3.contains(2))
    } catch {
      case e: Exception => {
        System.err.println(e.getMessage)
        e.printStackTrace()
        fail("Exception in test: " + e.getMessage)
      }
    }
  }

  /**
   * A mapper that preserves field 1 of a tuple data set.
   */
  @Test
  def translateUnaryFunctionAnnotationTuples2(): Unit = {
    try {
      val env = ExecutionEnvironment.getExecutionEnvironment

      val input = env.fromElements((3L, "test", 42))
      input.map(new FieldTwoForwardMapper[Long, String, Int])
        .output(new DiscardingOutputFormat[(Long, String, Int)])

      val plan = env.createProgramPlan()

      val sink: GenericDataSinkBase[_] = plan.getDataSinks.iterator.next

      val mapper: MapOperatorBase[_, _, _] = sink.getInput.asInstanceOf[MapOperatorBase[_, _, _]]

      val semantics: SingleInputSemanticProperties = mapper.getSemanticProperties
      val fw1: FieldSet = semantics.getForwardingTargetFields(0, 0)
      val fw2: FieldSet = semantics.getForwardingTargetFields(0, 1)
      val fw3: FieldSet = semantics.getForwardingTargetFields(0, 2)

      assertNotNull(fw1)
      assertNotNull(fw2)
      assertNotNull(fw3)
      assertTrue(fw1.size == 0)
      assertTrue(fw3.size == 0)
      assertTrue(fw2.contains(1))
    } catch {
      case e: Exception => {
        System.err.println(e.getMessage)
        e.printStackTrace()
        fail("Exception in test: " + e.getMessage)
      }
    }
  }

  /**
   * A join that preserves tuple fields from both sides.
   */
  @Test
  def translateBinaryFunctionAnnotationTuples1(): Unit = {
    try {
      val env = ExecutionEnvironment.getExecutionEnvironment

      val input1 = env.fromElements((3L, "test"))
      val input2 = env.fromElements((3L, 3.1415))

      input1.join(input2).where(0).equalTo(0)(
        new ForwardingTupleJoin[Long, String, Long, Double])
        .output(new DiscardingOutputFormat[(String, Long)])

      val plan = env.createProgramPlan()
      val sink: GenericDataSinkBase[_] = plan.getDataSinks.iterator.next

      val join: JoinOperatorBase[_, _, _, _] =
        sink.getInput.asInstanceOf[JoinOperatorBase[_, _, _, _]]

      val semantics = join.getSemanticProperties
      val fw11: FieldSet = semantics.getForwardingTargetFields(0, 0)
      val fw12: FieldSet = semantics.getForwardingTargetFields(0, 1)
      val fw21: FieldSet = semantics.getForwardingTargetFields(1, 0)
      val fw22: FieldSet = semantics.getForwardingTargetFields(1, 1)

      assertNotNull(fw11)
      assertNotNull(fw21)
      assertNotNull(fw12)
      assertNotNull(fw22)
      assertEquals(0, fw11.size)
      assertEquals(0, fw22.size)
      assertTrue(fw12.contains(0))
      assertTrue(fw21.contains(1))
    }
    catch {
      case e: Exception => {
        System.err.println(e.getMessage)
        e.printStackTrace()
        fail("Exception in test: " + e.getMessage)
      }
    }
  }

  /**
   * A join that preserves tuple fields from both sides.
   */
  @Test
  def translateBinaryFunctionAnnotationTuples2(): Unit = {
    try {
      val env = ExecutionEnvironment.getExecutionEnvironment

      val input1 = env.fromElements((3L, "test"))
      val input2 = env.fromElements((3L, 42))

      input1.join(input2).where(0).equalTo(0)(
        new ForwardingBasicJoin[(Long, String), (Long, Int)])
        .output(new DiscardingOutputFormat[((Long, String), (Long, Int))])

      val plan = env.createProgramPlan()
      val sink: GenericDataSinkBase[_] = plan.getDataSinks.iterator.next

      val join: JoinOperatorBase[_, _, _, _] =
        sink.getInput.asInstanceOf[JoinOperatorBase[_, _, _, _]]

      val semantics = join.getSemanticProperties
      val fw11: FieldSet = semantics.getForwardingTargetFields(0, 0)
      val fw12: FieldSet = semantics.getForwardingTargetFields(0, 1)
      val fw21: FieldSet = semantics.getForwardingTargetFields(1, 0)
      val fw22: FieldSet = semantics.getForwardingTargetFields(1, 1)

      assertNotNull(fw11)
      assertNotNull(fw12)
      assertNotNull(fw21)
      assertNotNull(fw22)
      assertTrue(fw11.contains(0))
      assertTrue(fw12.contains(1))
      assertTrue(fw21.contains(2))
      assertTrue(fw22.contains(3))
    }
    catch {
      case e: Exception => {
        System.err.println(e.getMessage)
        e.printStackTrace()
        fail("Exception in test: " + e.getMessage)
      }
    }
  }
}


@ForwardedFields(Array("*"))
class WildcardForwardMapper[T] extends RichMapFunction[T, T] {
  def map(value: T): T = {
    value
  }
}

@ForwardedFields(Array("0;1;2"))
class IndividualForwardMapper[X, Y, Z] extends RichMapFunction[(X, Y, Z), (X, Y, Z)] {
  def map(value: (X, Y, Z)): (X, Y, Z) = {
    value
  }
}

@ForwardedFields(Array("_2"))
class FieldTwoForwardMapper[X, Y, Z] extends RichMapFunction[(X, Y, Z), (X, Y, Z)] {
  def map(value: (X, Y ,Z)): (X, Y, Z) = {
    value
  }
}

@ForwardedFieldsFirst(Array("_2 -> _1"))
@ForwardedFieldsSecond(Array("_1 -> _2"))
class ForwardingTupleJoin[A, B, C, D] extends RichJoinFunction[(A, B),  (C, D), (B, C)] {
  def join(first: (A, B), second: (C, D)): (B, C) = {
    (first._2, second._1)
  }
}

@ForwardedFieldsFirst(Array("* -> 0.*"))
@ForwardedFieldsSecond(Array("* -> 1.*"))
class ForwardingBasicJoin[A, B] extends RichJoinFunction[A, B, (A, B)] {
  def join(first: A, second: B): (A, B) = {
    (first, second)
  }
}
