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
package org.apache.flink.api.scala.operators

import java.io.Serializable
import java.lang

import scala.collection.JavaConverters._

import org.apache.flink.api.common.functions.MapPartitionFunction
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.common.InvalidProgramException
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.util.CollectionDataSets
import org.apache.flink.test.util.MultipleProgramsTestBase.TestExecutionMode
import org.apache.flink.test.util.{TestBaseUtils, MultipleProgramsTestBase}
import org.apache.flink.util.Collector
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.Test

@RunWith(classOf[Parameterized])
class SortPartitionITCase(mode: TestExecutionMode) extends MultipleProgramsTestBase(mode) {

  @Test
  @throws(classOf[Exception])
  def testSortPartitionByKeyField(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    val ds = CollectionDataSets.get3TupleDataSet(env)

    val result = ds
      .map { x => x }.setParallelism(4)
      .sortPartition(1, Order.DESCENDING)
      .mapPartition(new OrderCheckMapper(new Tuple3Checker))
      .distinct()
      .collect()

    val expected: String = "(true)\n"
    TestBaseUtils.compareResultAsText(result.asJava, expected)
  }

  @Test
  @throws(classOf[Exception])
  def testSortPartitionByTwoKeyFields(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)
    val ds = CollectionDataSets.get5TupleDataSet(env)

    val result = ds
      .map { x => x }.setParallelism(2)
      .sortPartition(4, Order.ASCENDING)
        .sortPartition(2, Order.DESCENDING)
      .mapPartition(new OrderCheckMapper(new Tuple5Checker))
      .distinct()
      .collect()

    val expected: String = "(true)\n"
    TestBaseUtils.compareResultAsText(result.asJava, expected)
  }

  @Test
  @throws(classOf[Exception])
  def testSortPartitionByFieldExpression(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    val ds = CollectionDataSets.get3TupleDataSet(env)

    val result = ds
      .map { x => x }.setParallelism(4)
      .sortPartition("_2", Order.DESCENDING)
      .mapPartition(new OrderCheckMapper(new Tuple3Checker))
      .distinct()
      .collect()

    val expected: String = "(true)\n"
    TestBaseUtils.compareResultAsText(result.asJava, expected)
  }

  @Test
  @throws(classOf[Exception])
  def testSortPartitionByTwoFieldExpressions(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)
    val ds = CollectionDataSets.get5TupleDataSet(env)

    val result = ds
      .map { x => x }.setParallelism(2)
      .sortPartition("_5", Order.ASCENDING)
        .sortPartition("_3", Order.DESCENDING)
      .mapPartition(new OrderCheckMapper(new Tuple5Checker))
      .distinct()
      .collect()

    val expected: String = "(true)\n"
    TestBaseUtils.compareResultAsText(result.asJava, expected)
  }

  @Test
  @throws(classOf[Exception])
  def testSortPartitionByNestedFieldExpression(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(3)
    val ds = CollectionDataSets.getGroupSortedNestedTupleDataSet(env)

    val result = ds
      .map { x => x }.setParallelism(3)
      .sortPartition("_1._2", Order.ASCENDING)
        .sortPartition("_2", Order.DESCENDING)
      .mapPartition(new OrderCheckMapper(new NestedTupleChecker))
      .distinct()
      .collect()

    val expected: String = "(true)\n"
    TestBaseUtils.compareResultAsText(result.asJava, expected)
  }

  @Test
  @throws(classOf[Exception])
  def testSortPartitionPojoByNestedFieldExpression(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(3)
    val ds = CollectionDataSets.getMixedPojoDataSet(env)

    val result = ds
      .map { x => x }.setParallelism(3)
      .sortPartition("nestedTupleWithCustom._2.myString", Order.ASCENDING)
        .sortPartition("number", Order.DESCENDING)
      .mapPartition(new OrderCheckMapper(new PojoChecker))
      .distinct()
      .collect()

    val expected: String = "(true)\n"
    TestBaseUtils.compareResultAsText(result.asJava, expected)
  }

  @Test
  @throws(classOf[Exception])
  def testSortPartitionParallelismChange(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(3)
    val ds = CollectionDataSets.get3TupleDataSet(env)

    val result = ds
      .sortPartition(1, Order.DESCENDING).setParallelism(3)
      .mapPartition(new OrderCheckMapper(new Tuple3Checker))
      .distinct()
      .collect()

    val expected: String = "(true)\n"
    TestBaseUtils.compareResultAsText(result.asJava, expected)
  }

  @Test
  def testSortPartitionWithKeySelector1(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    val ds = CollectionDataSets.get3TupleDataSet(env)

    val result = ds
      .map { x => x }.setParallelism(4)
      .sortPartition(_._2, Order.ASCENDING)
      .mapPartition(new OrderCheckMapper(new Tuple3AscendingChecker))
      .distinct()
      .collect()

    val expected: String = "(true)\n"
    TestBaseUtils.compareResultAsText(result.asJava, expected)
  }

  @Test
  def testSortPartitionWithKeySelector2(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    val ds = CollectionDataSets.get3TupleDataSet(env)

    val result = ds
      .map { x => x }.setParallelism(4)
      .sortPartition(x => (x._2, x._1), Order.DESCENDING)
      .mapPartition(new OrderCheckMapper(new Tuple3Checker))
      .distinct()
      .collect()

    val expected: String = "(true)\n"
    TestBaseUtils.compareResultAsText(result.asJava, expected)
  }

  @Test(expected = classOf[InvalidProgramException])
  def testSortPartitionWithKeySelector3(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    val ds = CollectionDataSets.get3TupleDataSet(env)

    val result = ds
      .map { x => x }.setParallelism(4)
      .sortPartition(x => (x._2, x._1), Order.DESCENDING)
      .sortPartition(0, Order.DESCENDING)
      .mapPartition(new OrderCheckMapper(new Tuple3Checker))
      .distinct()
      .collect()

    val expected: String = "(true)\n"
    TestBaseUtils.compareResultAsText(result.asJava, expected)
  }

}

trait OrderChecker[T] extends Serializable {
  def inOrder(t1: T, t2: T): Boolean
}

class Tuple3Checker extends OrderChecker[(Int, Long, String)] {
  def inOrder(t1: (Int, Long, String), t2: (Int, Long, String)): Boolean = {
    t1._2 >= t2._2
  }
}

class Tuple3AscendingChecker extends OrderChecker[(Int, Long, String)] {
  def inOrder(t1: (Int, Long, String), t2: (Int, Long, String)): Boolean = {
    t1._2 <= t2._2
  }
}

class Tuple5Checker extends OrderChecker[(Int, Long, Int, String, Long)] {
  def inOrder(t1: (Int, Long, Int, String, Long), t2: (Int, Long, Int, String, Long)): Boolean = {
    t1._5 < t2._5 || t1._5 == t2._5 && t1._3 >= t2._3
  }
}

class NestedTupleChecker extends OrderChecker[((Int, Int), String)] {
  def inOrder(t1: ((Int, Int), String), t2: ((Int, Int), String)): Boolean = {
    t1._1._2 < t2._1._2 || t1._1._2 == t2._1._2 && t1._2.compareTo(t2._2) >= 0
  }
}

class PojoChecker extends OrderChecker[CollectionDataSets.POJO] {
  def inOrder(t1: CollectionDataSets.POJO, t2: CollectionDataSets.POJO): Boolean = {
    t1.nestedTupleWithCustom._2.myString.compareTo(t2.nestedTupleWithCustom._2.myString) < 0 ||
      t1.nestedTupleWithCustom._2.myString.compareTo(t2.nestedTupleWithCustom._2.myString) == 0 &&
        t1.number >= t2.number
  }
}

class OrderCheckMapper[T](checker: OrderChecker[T])
  extends MapPartitionFunction[T, Tuple1[Boolean]] {

  override def mapPartition(values: lang.Iterable[T], out: Collector[Tuple1[Boolean]]): Unit = {
    val it = values.iterator()
    if (!it.hasNext) {
      out.collect(new Tuple1(true))
    }
    else {
      var last: T = it.next()
      while (it.hasNext) {
        val next: T = it.next()
        if (!checker.inOrder(last, next)) {
          out.collect(new Tuple1(false))
          return
        }
        last = next
      }
      out.collect(new Tuple1(true))
    }
  }
}
