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

import org.apache.flink.api.common.operators.{GenericDataSourceBase, GenericDataSinkBase}
import org.apache.flink.api.java.io.DiscardingOutputFormat
import org.apache.flink.api.java.operators.translation.{KeyExtractingMapper,
PlanUnwrappingReduceOperator}
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.java.typeutils.TupleTypeInfo
import org.junit.Assert._
import org.apache.flink.api.common.operators.base.MapOperatorBase
import org.apache.flink.api.common.operators.base.ReduceOperatorBase
import org.junit.Test

import org.apache.flink.api.scala._

class ReduceTranslationTest {
  @Test
  def translateNonGroupedReduce(): Unit = {
    try {
      val parallelism = 8
      val env = ExecutionEnvironment.createLocalEnvironment(parallelism)

      val initialData = env.fromElements((3.141592, "foobar", 77L)).setParallelism(1)


      initialData reduce { (v1, v2) => v1 } output(
        new DiscardingOutputFormat[(Double, String, Long)])

      val p = env.createProgramPlan(

)
      val sink: GenericDataSinkBase[_] = p.getDataSinks.iterator.next
      val reducer: ReduceOperatorBase[_, _] = sink.getInput.asInstanceOf[ReduceOperatorBase[_, _]]

      assertEquals(initialData.javaSet.getType, reducer.getOperatorInfo.getInputType)
      assertEquals(initialData.javaSet.getType, reducer.getOperatorInfo.getOutputType)
      assertTrue(reducer.getKeyColumns(0) == null || reducer.getKeyColumns(0).length == 0)
      assertTrue(reducer.getParallelism == 1 || reducer.getParallelism == -1)
      assertTrue(reducer.getInput.isInstanceOf[GenericDataSourceBase[_, _]])
    }
    catch {
      case e: Exception => {
        System.err.println(e.getMessage)
        e.printStackTrace()
        fail("Test caused an error: " + e.getMessage)
      }
    }
  }

  @Test
  def translateGroupedReduceNoMapper(): Unit = {
    try {
      val parallelism: Int = 8
      val env = ExecutionEnvironment.createLocalEnvironment(parallelism)

      val initialData = env.fromElements((3.141592, "foobar", 77L)).setParallelism(1)

      initialData.groupBy(2) reduce { (v1, v2) => v1 } output(
        new DiscardingOutputFormat[(Double, String, Long)])

      val p = env.createProgramPlan()

      val sink: GenericDataSinkBase[_] = p.getDataSinks.iterator.next
      val reducer: ReduceOperatorBase[_, _] = sink.getInput.asInstanceOf[ReduceOperatorBase[_, _]]
      assertEquals(initialData.javaSet.getType, reducer.getOperatorInfo.getInputType)
      assertEquals(initialData.javaSet.getType, reducer.getOperatorInfo.getOutputType)
      assertTrue(reducer.getParallelism == parallelism || reducer.getParallelism == -1)
      assertArrayEquals(Array[Int](2), reducer.getKeyColumns(0))
      assertTrue(reducer.getInput.isInstanceOf[GenericDataSourceBase[_, _]])
    }
    catch {
      case e: Exception => {
        System.err.println(e.getMessage)
        e.printStackTrace()
        fail("Test caused an error: " + e.getMessage)
      }
    }
  }

  @Test
  def translateGroupedReduceWithKeyExtractor(): Unit = {
    try {
      val parallelism: Int = 8
      val env = ExecutionEnvironment.createLocalEnvironment(parallelism)

      val initialData = env.fromElements((3.141592, "foobar", 77L)).setParallelism(1)

      initialData.groupBy { _._2 }. reduce { (v1, v2) => v1 } setParallelism(4) output(
        new DiscardingOutputFormat[(Double, String, Long)])

      val p = env.createProgramPlan()
      val sink: GenericDataSinkBase[_] = p.getDataSinks.iterator.next
      val keyProjector: MapOperatorBase[_, _, _] = sink.getInput.asInstanceOf[MapOperatorBase[_,
        _, _]]
      val reducer: PlanUnwrappingReduceOperator[_, _] = keyProjector.getInput
        .asInstanceOf[PlanUnwrappingReduceOperator[_, _]]
      val keyExtractor: MapOperatorBase[_, _, _] = reducer.getInput
        .asInstanceOf[MapOperatorBase[_, _, _]]
      assertEquals(1, keyExtractor.getParallelism)
      assertEquals(4, reducer.getParallelism)
      assertEquals(4, keyProjector.getParallelism)
      val keyValueInfo = new TupleTypeInfo(
        BasicTypeInfo.STRING_TYPE_INFO,
        createTypeInformation[(Double, String, Long)])
      assertEquals(initialData.javaSet.getType, keyExtractor.getOperatorInfo.getInputType)
      assertEquals(keyValueInfo, keyExtractor.getOperatorInfo.getOutputType)
      assertEquals(keyValueInfo, reducer.getOperatorInfo.getInputType)
      assertEquals(keyValueInfo, reducer.getOperatorInfo.getOutputType)
      assertEquals(keyValueInfo, keyProjector.getOperatorInfo.getInputType)
      assertEquals(initialData.javaSet.getType, keyProjector.getOperatorInfo.getOutputType)
      assertEquals(
        classOf[KeyExtractingMapper[_, _]],
        keyExtractor.getUserCodeWrapper.getUserCodeClass)
      assertTrue(keyExtractor.getInput.isInstanceOf[GenericDataSourceBase[_, _]])
    }
    catch {
      case e: Exception => {
        System.err.println(e.getMessage)
        e.printStackTrace()
        fail("Test caused an error: " + e.getMessage)
      }
    }
  }
}

