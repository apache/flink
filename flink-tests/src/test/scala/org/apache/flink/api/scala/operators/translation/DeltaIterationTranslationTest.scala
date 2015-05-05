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

import org.apache.flink.api.common.functions.{RichCoGroupFunction, RichMapFunction,
RichJoinFunction}
import org.apache.flink.api.common.operators.GenericDataSinkBase
import org.apache.flink.api.java.io.DiscardingOutputFormat
import org.apache.flink.api.java.operators.translation.WrappingFunction
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.fail
import org.apache.flink.api.common.{InvalidProgramException, Plan}
import org.apache.flink.api.common.aggregators.LongSumAggregator
import org.apache.flink.api.common.operators.base.DeltaIterationBase
import org.apache.flink.api.common.operators.base.JoinOperatorBase
import org.apache.flink.api.common.operators.base.MapOperatorBase
import org.junit.Test

import org.apache.flink.util.Collector

import org.apache.flink.api.scala._

class DeltaIterationTranslationTest {

  @Test
  def testCorrectTranslation(): Unit = {
    try {
      val JOB_NAME = "Test JobName"
      val ITERATION_NAME = "Test Name"
      val BEFORE_NEXT_WORKSET_MAP = "Some Mapper"
      val AGGREGATOR_NAME = "AggregatorName"
      val ITERATION_KEYS = Array(2)
      val NUM_ITERATIONS = 13
      val DEFAULT_PARALLELISM = 133
      val ITERATION_PARALLELISM = 77

      val env = ExecutionEnvironment.getExecutionEnvironment
      env.setParallelism(DEFAULT_PARALLELISM)

      val initialSolutionSet = env.fromElements((3.44, 5L, "abc"))
      val initialWorkSet = env.fromElements((1.23, "abc"))

      val result = initialSolutionSet.iterateDelta(initialWorkSet, NUM_ITERATIONS, ITERATION_KEYS) {
        (s, ws) =>
          val wsSelfJoin = ws.map(new IdentityMapper[(Double, String)]())
            .join(ws).where(1).equalTo(1) { (l, r) => l }

          val joined = wsSelfJoin.join(s).where(1).equalTo(2).apply(new SolutionWorksetJoin)
          (joined, joined.map(new NextWorksetMapper).name(BEFORE_NEXT_WORKSET_MAP))
      }
      result.name(ITERATION_NAME)
        .setParallelism(ITERATION_PARALLELISM)
        .registerAggregator(AGGREGATOR_NAME, new LongSumAggregator)

      result.output(new DiscardingOutputFormat[(Double, Long, String)])
      result.writeAsText("/dev/null")

      val p: Plan = env.createProgramPlan(JOB_NAME)
      assertEquals(JOB_NAME, p.getJobName)
      assertEquals(DEFAULT_PARALLELISM, p.getDefaultParallelism)
      var sink1: GenericDataSinkBase[_] = null
      var sink2: GenericDataSinkBase[_] = null
      val sinks = p.getDataSinks.iterator
      sink1 = sinks.next
      sink2 = sinks.next

      val iteration: DeltaIterationBase[_, _] =
        sink1.getInput.asInstanceOf[DeltaIterationBase[_,_]]

      assertEquals(iteration, sink2.getInput)
      assertEquals(NUM_ITERATIONS, iteration.getMaximumNumberOfIterations)
      assertArrayEquals(ITERATION_KEYS, iteration.getSolutionSetKeyFields)
      assertEquals(ITERATION_PARALLELISM, iteration.getParallelism)
      assertEquals(ITERATION_NAME, iteration.getName)

      val nextWorksetMapper: MapOperatorBase[_, _, _] =
        iteration.getNextWorkset.asInstanceOf[MapOperatorBase[_, _, _]]
      val solutionSetJoin: JoinOperatorBase[_, _, _, _] =
        iteration.getSolutionSetDelta.asInstanceOf[JoinOperatorBase[_, _, _, _]]
      val worksetSelfJoin: JoinOperatorBase[_, _, _, _] =
        solutionSetJoin.getFirstInput.asInstanceOf[JoinOperatorBase[_, _, _, _]]
      val worksetMapper: MapOperatorBase[_, _, _] =
        worksetSelfJoin.getFirstInput.asInstanceOf[MapOperatorBase[_, _, _]]

      assertEquals(classOf[IdentityMapper[_]], worksetMapper.getUserCodeWrapper.getUserCodeClass)


      assertEquals(
        classOf[NextWorksetMapper],
        nextWorksetMapper.getUserCodeWrapper.getUserCodeClass)


      if (solutionSetJoin.getUserCodeWrapper.getUserCodeObject.isInstanceOf[WrappingFunction[_]]) {
        val wf: WrappingFunction[_] = solutionSetJoin.getUserCodeWrapper.getUserCodeObject
          .asInstanceOf[WrappingFunction[_]]
        assertEquals(classOf[SolutionWorksetJoin],
          wf.getWrappedFunction.getClass)
      }
      else {
        assertEquals(classOf[SolutionWorksetJoin],
          solutionSetJoin.getUserCodeWrapper.getUserCodeClass)
      }

      assertEquals(BEFORE_NEXT_WORKSET_MAP, nextWorksetMapper.getName)
      assertEquals(AGGREGATOR_NAME, iteration.getAggregators.getAllRegisteredAggregators.iterator
        .next.getName)
    }
    catch {
      case e: Exception => {
        System.err.println(e.getMessage)
        e.printStackTrace()
        fail(e.getMessage)
      }
    }
  }

  @Test
  def testRejectWhenSolutionSetKeysDontMatchJoin(): Unit = {
    try {
      val env = ExecutionEnvironment.getExecutionEnvironment

      val initialSolutionSet = env.fromElements((3.44, 5L, "abc"))
      val initialWorkSet = env.fromElements((1.23, "abc"))

      val iteration = initialSolutionSet.iterateDelta(initialWorkSet, 10, Array(0)) {
        (s, ws) =>
          try {
            ws.join(s).where(1).equalTo(2)
            fail("Accepted invalid program.")
          } catch {
            case e: InvalidProgramException => // all good
          }
          try {
            s.join(ws).where(2).equalTo(1)
            fail("Accepted invalid program.")
          } catch {
            case e: InvalidProgramException => // all good
          }
          (s, ws)
      }
    } catch {
      case e: Exception => {
        System.err.println(e.getMessage)
        e.printStackTrace()
        fail(e.getMessage)
      }
    }
  }

  @Test
  def testRejectWhenSolutionSetKeysDontMatchCoGroup(): Unit = {
    try {
      val env = ExecutionEnvironment.getExecutionEnvironment

      val initialSolutionSet = env.fromElements((3.44, 5L, "abc"))
      val initialWorkSet = env.fromElements((1.23, "abc"))

      val iteration = initialSolutionSet.iterateDelta(initialWorkSet, 10, Array(0)) {
        (s, ws) =>
          try {
            ws.coGroup(s).where(1).equalTo(2)
            fail("Accepted invalid program.")
          } catch {
            case e: InvalidProgramException => // all good
          }
          try {
            s.coGroup(ws).where(2).equalTo(1)
            fail("Accepted invalid program.")
          } catch {
            case e: InvalidProgramException => // all good
          }
          (s, ws)
      }
    } catch {
      case e: Exception => {
        System.err.println(e.getMessage)
        e.printStackTrace()
        fail(e.getMessage)
      }
    }
  }
//
//  @Test def testRejectWhenSolutionSetKeysDontMatchCoGroup {
//    try {
//      val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
//      @SuppressWarnings(Array("unchecked")) val initialSolutionSet: DataSet[Tuple3[Double, Long,
//        String]] = env.fromElements(new Tuple3[Double, Long, String](3.44, 5L, "abc"))
//      @SuppressWarnings(Array("unchecked")) val initialWorkSet: DataSet[Tuple2[Double,
//        String]] = env.fromElements(new Tuple2[Double, String](1.23, "abc"))
//      val iteration: DeltaIteration[Tuple3[Double, Long, String], Tuple2[Double,
//        String]] = initialSolutionSet.iterateDelta(initialWorkSet, 10, 1)
//      try {
//        iteration.getWorkset.coGroup(iteration.getSolutionSet).where(1).equalTo(2).`with`(
// new DeltaIterationTranslationTest.SolutionWorksetCoGroup1)
//        fail("Accepted invalid program.")
//      }
//      catch {
//        case e: InvalidProgramException => {
//        }
//      }
//      try {
//        iteration.getSolutionSet.coGroup(iteration.getWorkset).where(2).equalTo(1).`with`(
// new DeltaIterationTranslationTest.SolutionWorksetCoGroup2)
//        fail("Accepted invalid program.")
//      }
//      catch {
//        case e: InvalidProgramException => {
//        }
//      }
//    }
//    catch {
//      case e: Exception => {
//        System.err.println(e.getMessage)
//        e.printStackTrace
//        fail(e.getMessage)
//      }
//    }
//  }
}

class SolutionWorksetJoin
  extends RichJoinFunction[(Double, String), (Double, Long, String), (Double, Long, String)] {
  def join(first: (Double, String), second: (Double, Long, String)): (Double, Long, String) = {
    null
  }
}

class NextWorksetMapper extends RichMapFunction[(Double, Long, String), (Double, String)] {
  def map(value: (Double, Long, String)): (Double, String) = {
    null
  }
}

class IdentityMapper[T] extends RichMapFunction[T, T] {
  def map(value: T): T = {
    value
  }
}

