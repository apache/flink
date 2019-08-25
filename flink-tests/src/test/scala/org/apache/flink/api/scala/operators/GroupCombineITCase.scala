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

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.java.io.DiscardingOutputFormat
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.util.CollectionDataSets
import org.apache.flink.test.operators.GroupCombineITCase.ScalaGroupCombineFunctionExample
import org.apache.flink.test.util.MultipleProgramsTestBase.TestExecutionMode
import org.apache.flink.test.util.MultipleProgramsTestBase
import org.apache.flink.util.Collector
import org.junit._
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

/**
 * Java interoperability tests. Main tests are in GroupCombineITCase Java.
 */
@RunWith(classOf[Parameterized])
class GroupCombineITCase(mode: TestExecutionMode) extends MultipleProgramsTestBase(mode) {

  @Test
  def testApi(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val ds: DataSet[Tuple1[String]] = CollectionDataSets.getStringDataSet(env)
      .map(str => Tuple1(str))

    // all methods on DataSet
    ds.combineGroup(new ScalaGroupCombineFunctionExample())
      .output(new DiscardingOutputFormat[Tuple1[String]])

    ds
      .combineGroup(
        (in: Iterator[Tuple1[String]], out: Collector[Tuple1[String]]) =>
          in.toSet foreach (out.collect))
      .output(new DiscardingOutputFormat[Tuple1[String]])

    // all methods on UnsortedGrouping
    ds.groupBy(0)
      .combineGroup(new ScalaGroupCombineFunctionExample())
      .output(new DiscardingOutputFormat[Tuple1[String]])

    ds.groupBy(0)
      .combineGroup(
        (in: Iterator[Tuple1[String]], out: Collector[Tuple1[String]]) =>
          in.toSet foreach (out.collect))
      .output(new DiscardingOutputFormat[Tuple1[String]])

    // all methods on SortedGrouping
    ds.groupBy(0).sortGroup(0, Order.ASCENDING)
      .combineGroup(new ScalaGroupCombineFunctionExample())
      .output(new DiscardingOutputFormat[Tuple1[String]])

    ds.groupBy(0).sortGroup(0, Order.ASCENDING)
      .combineGroup(
        (in: Iterator[Tuple1[String]], out: Collector[Tuple1[String]]) =>
          in.toSet foreach (out.collect))
      .output(new DiscardingOutputFormat[Tuple1[String]])

    env.execute()
  }
}


