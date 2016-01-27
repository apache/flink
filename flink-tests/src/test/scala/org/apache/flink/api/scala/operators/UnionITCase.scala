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

import org.apache.flink.api.scala.util.CollectionDataSets
import org.apache.flink.test.util.MultipleProgramsTestBase.TestExecutionMode
import org.apache.flink.test.util.{TestBaseUtils, MultipleProgramsTestBase}
import org.junit._
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import org.apache.flink.api.scala._

import scala.collection.mutable.ArrayBuffer

@RunWith(classOf[Parameterized])
class UnionITCase(mode: TestExecutionMode) extends MultipleProgramsTestBase(mode) {
  private var result: Seq[String] = null
  private var expected: String = null

  private final val FULL_TUPLE_3_STRING: String = "(1,1,Hi)\n" + "(2,2,Hello)\n" + "(3,2," +
    "Hello world)\n" + "(4,3,Hello world, how are you?)\n" + "(5,3,I am fine.)\n" + "(6,3," +
    "Luke Skywalker)\n" + "(7,4,Comment#1)\n" + "(8,4,Comment#2)\n" + "(9,4,Comment#3)\n" +
    "(10,4," +
    "Comment#4)\n" + "(11,5,Comment#5)\n" + "(12,5,Comment#6)\n" + "(13,5,Comment#7)\n" + "(14,5," +
    "Comment#8)\n" + "(15,5,Comment#9)\n" + "(16,6,Comment#10)\n" + "(17,6,Comment#11)\n" +
    "(18,6," +
    "Comment#12)\n" + "(19,6,Comment#13)\n" + "(20,6,Comment#14)\n" + "(21,6,Comment#15)\n"


  @After
  def after(): Unit = {
    import collection.JavaConverters._
    TestBaseUtils.compareResultAsText(ArrayBuffer(result: _*).asJava, expected)
  }

  @Test
  def testUnionOf2IdenticalDS(): Unit = {
    /*
     * Union of 2 Same Data Sets
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.get3TupleDataSet(env)
    val unionDs = ds.union(CollectionDataSets.get3TupleDataSet(env))
    result = unionDs.collect().map(_.toString)
    expected = FULL_TUPLE_3_STRING + FULL_TUPLE_3_STRING
  }

  @Test
  def testUnionOf5IdenticalDSWithMultipleUnions(): Unit = {
    /*
     * Union of 5 same Data Sets, with multiple unions
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.get3TupleDataSet(env)
    val unionDs = ds
      .union(CollectionDataSets.get3TupleDataSet(env))
      .union(CollectionDataSets.get3TupleDataSet(env))
      .union(CollectionDataSets.get3TupleDataSet(env))
      .union(CollectionDataSets.get3TupleDataSet(env))
    result = unionDs.collect().map(_.toString)
    expected = FULL_TUPLE_3_STRING + FULL_TUPLE_3_STRING + FULL_TUPLE_3_STRING +
      FULL_TUPLE_3_STRING + FULL_TUPLE_3_STRING
  }

  @Test
  def testUnionWithEmptyDS(): Unit = {
    /*
     * Test on union with empty dataset
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    // Don't know how to make an empty result in an other way than filtering it
    val empty = CollectionDataSets.get3TupleDataSet(env).filter( t => false )
    val unionDs = CollectionDataSets.get3TupleDataSet(env).union(empty)
    result = unionDs.collect().map(_.toString())
    expected = FULL_TUPLE_3_STRING
  }

  @Test
  def testUnionWithOptionType(): Unit = {
    /*
     * Union of a tuple with an Option field
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val data = Seq((Some(1), 1), (None, -1), (Some(42), 42))
    val input1 = env.fromCollection(data)
    val input2 = env.fromCollection(data)

    result = input1.union(input2).collect().map(_.toString())
    expected = data ++ data mkString("\n")
  }
}
