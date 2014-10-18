/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.api.scala.functions

import org.junit.Assert._
import org.apache.flink.api.common.functions.RichJoinFunction
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.operators.{GenericDataSinkBase, SingleInputSemanticProperties}
import org.apache.flink.api.common.operators.base.{JoinOperatorBase, MapOperatorBase}
import org.apache.flink.api.common.operators.util.FieldSet
import org.apache.flink.api.java.functions.FunctionAnnotation.ConstantFields
import org.apache.flink.api.java.functions.FunctionAnnotation.ConstantFieldsFirst
import org.apache.flink.api.java.functions.FunctionAnnotation.ConstantFieldsSecond
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
      input.map(new WildcardConstantMapper[(Long, String, Int)]).print()

      val plan = env.createProgramPlan()

      val sink: GenericDataSinkBase[_] = plan.getDataSinks.iterator.next

      val mapper: MapOperatorBase[_, _, _] = sink.getInput.asInstanceOf[MapOperatorBase[_, _, _]]

      val semantics: SingleInputSemanticProperties = mapper.getSemanticProperties
      val fw1: FieldSet = semantics.getForwardedField(0)
      val fw2: FieldSet = semantics.getForwardedField(1)
      val fw3: FieldSet = semantics.getForwardedField(2)

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
  def translateUnaryFunctionAnnotationTuples(): Unit = {
    try {
      val env = ExecutionEnvironment.getExecutionEnvironment

      val input = env.fromElements((3L, "test", 42))
      input.map(new IndividualConstantMapper[Long, String, Int]).print()

      val plan = env.createProgramPlan()

      val sink: GenericDataSinkBase[_] = plan.getDataSinks.iterator.next

      val mapper: MapOperatorBase[_, _, _] = sink.getInput.asInstanceOf[MapOperatorBase[_, _, _]]

      val semantics: SingleInputSemanticProperties = mapper.getSemanticProperties
      val fw1: FieldSet = semantics.getForwardedField(0)
      val fw2: FieldSet = semantics.getForwardedField(1)
      val fw3: FieldSet = semantics.getForwardedField(2)

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
   * A join that preserves tuple fields from both sides.
   */
  @Test
  def translateBinaryFunctionAnnotationTuples(): Unit = {
    try {
      val env = ExecutionEnvironment.getExecutionEnvironment

      val input1 = env.fromElements((3L, "test"))
      val input2 = env.fromElements((3L, 3.1415))

      input1.join(input2).where(0).equalTo(0)(
        new ForwardingTupleJoin[Long, String, Long, Double]).print()

      val plan = env.createProgramPlan()
      val sink: GenericDataSinkBase[_] = plan.getDataSinks.iterator.next

      val join: JoinOperatorBase[_, _, _, _] =
        sink.getInput.asInstanceOf[JoinOperatorBase[_, _, _, _]]

      val semantics = join.getSemanticProperties
      val fw11: FieldSet = semantics.getForwardedField1(0)
      val fw12: FieldSet = semantics.getForwardedField1(1)
      val fw21: FieldSet = semantics.getForwardedField2(0)
      val fw22: FieldSet = semantics.getForwardedField2(1)

      assertNull(fw11)
      assertNull(fw21)
      assertNotNull(fw12)
      assertNotNull(fw22)
      assertTrue(fw12.contains(0))
      assertTrue(fw22.contains(1))
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


@ConstantFields(Array("*"))
class WildcardConstantMapper[T] extends RichMapFunction[T, T] {
  def map(value: T): T = {
    value
  }
}

@ConstantFields(Array("0->0;1->1;2->2"))
class IndividualConstantMapper[X, Y, Z] extends RichMapFunction[(X, Y, Z), (X, Y, Z)] {
  def map(value: (X, Y, Z)): (X, Y, Z) = {
    value
  }
}

@ConstantFields(Array("0"))
class ZeroConstantMapper[T] extends RichMapFunction[T, T] {
  def map(value: T): T = {
    value
  }
}

@ConstantFieldsFirst(Array("1 -> 0"))
@ConstantFieldsSecond(Array("1 -> 1"))
class ForwardingTupleJoin[A, B, C, D] extends RichJoinFunction[(A, B),  (C, D), (B, D)] {
  def join(first: (A, B), second: (C, D)): (B, D) = {
    (first._2, second._2)
  }
}

@ConstantFieldsFirst(Array("0 -> 0"))
@ConstantFieldsSecond(Array("0 -> 1"))
class ForwardingBasicJoin[A, B] extends RichJoinFunction[A, B, (A, B)] {
  def join(first: A, second: B): (A, B) = {
    (first, second)
  }
}

