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

import java.lang.Iterable

import org.apache.flink.api.common.functions._
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.util.CollectionDataSets
import org.apache.flink.api.scala.util.CollectionDataSets.{MutableTuple3, CustomType}
import org.apache.flink.optimizer.Optimizer
import org.apache.flink.configuration.Configuration
import org.apache.flink.test.util.MultipleProgramsTestBase.TestExecutionMode
import org.apache.flink.test.util.MultipleProgramsTestBase
import org.apache.flink.util.Collector

import org.junit.Test
import org.junit.Assert._
import org.junit.Assume._
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.JavaConverters._

@RunWith(classOf[Parameterized])
class GroupReduceITCase(mode: TestExecutionMode) extends MultipleProgramsTestBase(mode) {
  
  /**
   * check correctness of groupReduce on tuples with key field selector
   */
  @Test
  def testCorrectnessOfGroupReduceOnTuplesWithKeyFieldSelector(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds =  CollectionDataSets.get3TupleDataSet(env)
    val reduceDs =  ds.groupBy(1).reduceGroup {
      in =>
        in.map(t => (t._1, t._2)).reduce((l, r) => (l._1 + r._1, l._2))
    }
    val result: Seq[(Int, Long)] = reduceDs.collect().sortBy(_._1)
    
    val expected = Seq[(Int, Long)]( (1,1), (5,2), (15,3), (34,4), (65,5), (111,6) )

    assertEquals(expected, result)
  }

  /**
   * check correctness of groupReduce on tuples with multiple key field selector
   */
  @Test
  def testCorrectnessOfGroupReduceOnTuplesWithMultipleKeyFieldSelector(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds =  CollectionDataSets.get5TupleDataSet(env)
    
    val reduceDs =  ds.groupBy(4, 0).reduceGroup {
      in =>
        val (i, l, l2) = in
          .map( t => (t._1, t._2, t._5))
          .reduce((l, r) => (l._1, l._2 + r._2, l._3))
        (i, l, 0, "P-)", l2)
    }
    
    val result: Seq[(Int, Long, Int, String, Long)] = reduceDs.collect().sortBy( t=> (t._1, t._5))

    val expected = Seq[(Int, Long, Int, String, Long)](
      (1,1,0,"P-)",1),
      (2,3,0,"P-)",1),
      (2,2,0,"P-)",2),
      (3,9,0,"P-)",2),
      (3,6,0,"P-)",3),
      (4,17,0,"P-)",1),
      (4,17,0,"P-)",2),
      (5,11,0,"P-)",1),
      (5,29,0,"P-)",2),
      (5,25,0,"P-)",3) )
    
    assertEquals(expected, result)
  }

  /**
   * check correctness of groupReduce on tuples with key field selector and group sorting
   */
  @Test
  def testCorrectnessOfGroupReduceOnTuplesWithKeyFieldSelectorAndGroupSorting(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val ds =  CollectionDataSets.get3TupleDataSet(env)
    val reduceDs =  ds.groupBy(1).sortGroup(2, Order.ASCENDING).reduceGroup {
      in =>
        in.reduce((l, r) => (l._1 + r._1, l._2, l._3 + "-" + r._3))
    }
    
    val result: Seq[(Int, Long, String)] = reduceDs.collect().sortBy(_._1)
    
    val expected = Seq[(Int, Long, String)] (
      (1,1,"Hi"),
      (5,2,"Hello-Hello world"),
      (15,3,"Hello world, how are you?-I am fine.-Luke Skywalker"),
      (34,4,"Comment#1-Comment#2-Comment#3-Comment#4"),
      (65,5,"Comment#5-Comment#6-Comment#7-Comment#8-Comment#9"),
      (111,6,"Comment#10-Comment#11-Comment#12-Comment#13-Comment#14-Comment#15") )
    
    assertEquals(expected, result)
  }

  /**
   * check correctness of groupReduce on tuples with key extractor
   */
  @Test
  def testCorrectnessOfGroupReduceOnTuplesWithKeyExtractor(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds =  CollectionDataSets.get3TupleDataSet(env)
    val reduceDs =  ds.groupBy(_._2).reduceGroup {
      in =>
        in.map(t => (t._1, t._2)).reduce((l, r) => (l._1 + r._1, l._2))
    }
    
    val result: Seq[(Int, Long)] = reduceDs.collect().sortBy(_._1)
    
    val expected = Seq[(Int, Long)]( (1,1), (5,2), (15,3), (34,4), (65,5), (111,6) )
    
    assertEquals(expected, result)
  }

  /**
   * check correctness of groupReduce on custom type with type extractor
   */
  @Test
  def testCorrectnessOfGroupReduceOnCustomTypeWithTypeExtractor(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds =  CollectionDataSets.getCustomTypeDataSet(env)
    val reduceDs =  ds.groupBy(_.myInt).reduceGroup {
      in =>
        val o = new CustomType
        val c = in.next()

        o.myString = "Hello!"
        o.myInt = c.myInt
        o.myLong = c.myLong

        while (in.hasNext) {
          val next = in.next()
          o.myLong += next.myLong
        }
        o
    }
    
    val result: Seq[String] = reduceDs.map(_.toString).collect().sorted

    val expected = Seq[String]( "1,0,Hello!", "2,3,Hello!", "3,12,Hello!",
      "4,30,Hello!", "5,60,Hello!", "6,105,Hello!")
    
    assertEquals(expected, result)
  }

  /**
   * check correctness of all-groupreduce for tuples
   */
  @Test
  def testCorrectnessOfAllGroupReduceForTuples(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds =  CollectionDataSets.get3TupleDataSet(env)
    val reduceDs =  ds.reduceGroup {
      in =>
        var i = 0
        var l = 0L
        for (t <- in) {
          i += t._1
          l += t._2
        }
        (i, l, "Hello World")
    }
    
    val result: (Int, Long, String) = reduceDs.collect().head
    val expected: (Int, Long, String) = (231,91,"Hello World")
    assertEquals(expected, result)
  }

  /**
   * check correctness of all-groupreduce for custom types
   */
  @Test
  def testCorrectnessOfAllGroupReduceForCustomTypes(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds =  CollectionDataSets.getCustomTypeDataSet(env)
    val reduceDs =  ds.reduceGroup {
      in =>
        val o = new CustomType(0, 0, "Hello!")
        for (t <- in) {
          o.myInt += t.myInt
          o.myLong += t.myLong
        }
        o
    }
    val result : String = reduceDs.collect().head.toString()
    val expected = "91,210,Hello!"
    assertEquals(expected, result)
  }

  /**
   * check correctness of groupReduce with broadcast set
   */
  @Test
  def testCorrectnessOfGroupReduceWithBroadcastSet(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val intDs =  CollectionDataSets.getIntDataSet(env)
    val ds =  CollectionDataSets.get3TupleDataSet(env)
    val reduceDs =  ds.groupBy(1).reduceGroup(
      new RichGroupReduceFunction[(Int, Long, String), (Int, Long, String)] {
        private var f2Replace = ""

        override def open(config: Configuration) {
          val ints = this.getRuntimeContext.getBroadcastVariable[Int]("ints").asScala
          f2Replace = ints.sum + ""
        }

        override def reduce(
                             values: Iterable[(Int, Long, String)],
                             out: Collector[(Int, Long, String)]): Unit = {
          var i: Int = 0
          var l: Long = 0L
          for (t <- values.asScala) {
            i += t._1
            l = t._2
          }
          out.collect((i, l, f2Replace))
        }
      }).withBroadcastSet(intDs, "ints")
    
    
    val result: Seq[(Int, Long, String)] = reduceDs.collect().sortBy(_._1)
    val expected = Seq[(Int, Long, String)](
      (1,1,"55"),
      (5,2,"55"),
      (15,3,"55"),
      (34,4,"55"), 
      (65,5,"55"), 
      (111,6,"55") )
    
    assertEquals(expected, result)
  }

  /**
   * check correctness of groupReduce if UDF returns input objects multiple times and
   * changes it in between
   */
  @Test
  def testCorrectnessOfGroupReduceIfUDFReturnsInputObjectMultipleTimesWhileChangingIt(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds =  CollectionDataSets.get3TupleDataSet(env)
      .map( t => MutableTuple3(t._1, t._2, t._3) )
    
    val reduceDs =  ds.groupBy(1).reduceGroup {
      (in, out: Collector[MutableTuple3[Int, Long, String]]) =>
        for (t <- in) {
          if (t._1 < 4) {
            t._3 = "Hi!"
            t._1 += 10
            out.collect(t)
            t._1 += 10
            t._3 = "Hi again!"
            out.collect(t)
          }
        }
    }
    val result: Seq[String] = reduceDs.collect().map(x => s"${x._1},${x._2},${x._3}").sorted
    
    val expected = Seq[String](
      "11,1,Hi!", "21,1,Hi again!",
      "12,2,Hi!", "22,2,Hi again!",
      "13,2,Hi!", "23,2,Hi again!").sorted

    assertEquals(expected, result)
  }
  
  /**
   * check correctness of groupReduce on custom type with key extractor and combine
   */
  @Test
  def testCorrectnessOfGroupReduceOnCustomTypeWithKeyExtractorAndCombine(): Unit = {

    org.junit.Assume.assumeFalse(mode == TestExecutionMode.COLLECTION)

    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds =  CollectionDataSets.getCustomTypeDataSet(env)

    val reduceDs =  ds.groupBy(_.myInt).reduceGroup(new CustomTypeGroupReduceWithCombine())
    
    val result: Seq[String] = reduceDs.collect().map(_.toString()).sorted
    
    val expected = Seq[String](
      "1,0,test1",
      "2,3,test2",
      "3,12,test3",
      "4,30,test4",
      "5,60,test5",
      "6,105,test6")

    assertEquals(expected, result)
  }

  /**
   * check correctness of groupReduce on tuples with combine
   */
  @Test
  def testCorrectnessOfGroupReduceOnTuplesWithCombine(): Unit = {

    org.junit.Assume.assumeFalse(mode == TestExecutionMode.COLLECTION)

    val env = ExecutionEnvironment.getExecutionEnvironment
    // important because it determines how often the combiner is called
    env.setParallelism(2)
    val ds =  CollectionDataSets.get3TupleDataSet(env)

    val reduceDs =  ds.groupBy(1).reduceGroup(new Tuple3GroupReduceWithCombine())
    
    val result: Seq[(Int, String)] = reduceDs.collect().sortBy(_._1)
    val expected = Seq[(Int, String)](
      (1,"test1"),
      (5,"test2"),
      (15,"test3"),
      (34,"test4"),
      (65,"test5"),
      (111,"test6") )
    assertEquals(expected, result)
  }

  /**
   * check correctness of all-groupreduce for tuples with combine
   */
  @Test
  def testCorrectnessOfAllGroupReduceForTuplesWithCombine(): Unit = {

    org.junit.Assume.assumeFalse(mode == TestExecutionMode.COLLECTION)

    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds =  CollectionDataSets.get3TupleDataSet(env).map(t => t).setParallelism(4)

    val cfg: Configuration = new Configuration
    cfg.setString(Optimizer.HINT_SHIP_STRATEGY, Optimizer.HINT_SHIP_STRATEGY_REPARTITION)

    val reduceDs =  ds.reduceGroup(new Tuple3AllGroupReduceWithCombine).withParameters(cfg)

    val result: (Int, String) = reduceDs.collect().head
    val expected = (322,
        "testtesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttest")
    assertEquals(expected, result)
  }

  /**
   * check correctness of groupReduce with descending group sort
   */
  @Test
  def testCorrectnessOfGroupReduceWithDescendingGroupSort(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds =  CollectionDataSets.get3TupleDataSet(env)
    val reduceDs =  ds.groupBy(1).sortGroup(2, Order.DESCENDING).reduceGroup {
      in =>
        in.reduce((l, r) => (l._1 + r._1, l._2, l._3 + "-" + r._3))
    }

    val result: Seq[(Int, Long, String)] = reduceDs.collect().sortBy(_._1)
    val expected = Seq[(Int, Long, String)](
      (1,1,"Hi"),
      (5,2,"Hello world-Hello"),
      (15,3,"Luke Skywalker-I am fine.-Hello world, how are you?"),
      (34,4,"Comment#4-Comment#3-Comment#2-Comment#1"),
      (65,5,"Comment#9-Comment#8-Comment#7-Comment#6-Comment#5"),
      (111,6,"Comment#15-Comment#14-Comment#13-Comment#12-Comment#11-Comment#10") )

    assertEquals(expected, result)
  }

  /**
   * check correctness of groupReduce on tuples with tuple-returning key selector
   */
  @Test
  def testCorrectnessOfGroupReduceOnTuplesWithTupleReturningKeySelector(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds =  CollectionDataSets.get5TupleDataSet(env)
    
    val reduceDs = ds.groupBy( t => (t._1, t._5)).reduceGroup {
      in =>
        val (i, l, l2) = in
          .map( t => (t._1, t._2, t._5))
          .reduce((l, r) => (l._1, l._2 + r._2, l._3))
        (i, l, 0, "P-)", l2)
    }

    val result: Seq[(Int, Long, Int, String, Long)] = reduceDs.collect().sortBy(x => (x._1, x._5))
    
    val expected = Seq[(Int, Long, Int, String, Long)](
      (1,1,0,"P-)",1),
      (2,3,0,"P-)",1),
      (2,2,0,"P-)",2),
      (3,9,0,"P-)",2),
      (3,6,0,"P-)",3),
      (4,17,0,"P-)",1),
      (4,17,0,"P-)",2),
      (5,11,0,"P-)",1),
      (5,29,0,"P-)",2),
      (5,25,0,"P-)",3) )

    assertEquals(expected, result)
  }

  /**
   * check that input of combiner is also sorted for combinable groupReduce with group
   * sorting
   */
  @Test
  def testInputOfCombinerIsSortedForCombinableGroupReduceWithGroupSorting(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds =  CollectionDataSets.get3TupleDataSet(env).map { t => 
      MutableTuple3(t._1, t._2, t._3)
    }

    val reduceDs =  ds.groupBy(1).sortGroup(0, Order.ASCENDING)
      .reduceGroup(new OrderCheckingCombinableReduce())
    
    
    val result: Seq[String] = reduceDs.collect().sortBy(_._1).map(x => s"${x._1},${x._2},${x._3}")
    
    val expected = Seq[String] (
      "1,1,Hi",
      "2,2,Hello",
      "4,3,Hello world, how are you?",
      "7,4,Comment#1",
      "11,5,Comment#5",
      "16,6,Comment#10")
    assertEquals(expected, result)
  }

  /**
   * Deep nesting test
   * + null value in pojo
   */
  @Test
  def testDeepNestingAndNullValueInPojo(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds =  CollectionDataSets.getCrazyNestedDataSet(env)
    
    val reduceDs =  ds.groupBy("nest_Lvl1.nest_Lvl2.nest_Lvl3.nest_Lvl4.f1nal")
      .reduceGroup {
      in =>
        var c = 0
        var n: String = null
        for (v <- in) {
          c += 1
          n = v.nest_Lvl1.nest_Lvl2.nest_Lvl3.nest_Lvl4.f1nal
        }
        (n, c)
    }
    
    val result: Seq[(String, Int)] = reduceDs.collect().sortBy(_._1)
    val expected = Seq[(String, Int)](
      ("aa",1),
      ("bb",2),
      ("cc",3) )
    assertEquals(expected, result)
  }

  /**
   * Test Pojo containing a Writable and Tuples
   */
  @Test
  def testPojoContainigAWritableAndTuples(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds =  CollectionDataSets.getPojoContainingTupleAndWritable(env)
    
    val reduceDs = ds.groupBy("hadoopFan", "theTuple.*").reduceGroup {
      (values, out: Collector[Int]) => {
        var c: Int = 0
        for (v <- values) {
          c += 1
        }
        out.collect(c)
      }
    }
    
    val result: Seq[Int] = reduceDs.collect().sorted
    val expected = Seq[Int](1, 5)
    assertEquals(expected, result)
  }

  /**
   * Test Tuple containing pojos and regular fields
   */
  @Test
  def testTupleContainingPojosAndRegularFields(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.getTupleContainingPojos(env)
    
    val reduceDs =  ds.groupBy("_1", "_2.*").reduceGroup {
      (values, out: Collector[Int]) => {
        out.collect(values.size)
      }
    }
    
    val result: Seq[Int] = reduceDs.collect().sorted
    val expected = Seq[Int](1, 3)
    assertEquals(expected, result)
  }

  /**
   * Test string-based definition on group sort, based on test:
   * check correctness of groupReduce with descending group sort
   */
  @Test
  def testStringBasedDefinitionOnGroupSort(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds =  CollectionDataSets.get3TupleDataSet(env)
    
    val reduceDs =  ds.groupBy(1)
      .sortGroup("_3", Order.DESCENDING)
      .reduceGroup {
      in =>
        in.reduce((l, r) => (l._1 + r._1, l._2, l._3 + "-" + r._3))
    }
    
    val result: Seq[(Int, Long, String)] = reduceDs.collect().sortBy(_._1)
    val expected = Seq[(Int, Long, String)](
      (1,1,"Hi"),
      (5,2,"Hello world-Hello"),
      (15,3,"Luke Skywalker-I am fine.-Hello world, how are you?"),
      (34,4,"Comment#4-Comment#3-Comment#2-Comment#1"),
      (65,5,"Comment#9-Comment#8-Comment#7-Comment#6-Comment#5"),
      (111,6,"Comment#15-Comment#14-Comment#13-Comment#12-Comment#11-Comment#10") )
    assertEquals(expected, result)
  }

  /**
   * Test int-based definition on group sort, for (full) nested Tuple
   */
  @Test
  def testIntBasedDefinitionOnGroupSortForFullNestedTuple(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds =  CollectionDataSets.getGroupSortedNestedTupleDataSet(env)
    
    val reduceDs =  ds.groupBy("_2").sortGroup(0, Order.DESCENDING)
      .reduceGroup(new NestedTupleReducer())
    
    val result: Seq[String] = reduceDs.map(_.toString()).collect().sorted
    val expected = Seq[String](
      "a--(2,1)-(1,3)-(1,2)-",
      "b--(2,2)-",
      "c--(4,9)-(3,6)-(3,3)-")
    assertEquals(expected, result)
  }

  /**
   * Test int-based definition on group sort, for (partial) nested Tuple ASC
   */
  @Test
  def testIntBasedDefinitionOnGroupSortForPartialNestedTuple(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds =  CollectionDataSets.getGroupSortedNestedTupleDataSet(env)
    
    val reduceDs =  ds.groupBy("_2")
      .sortGroup("_1._1", Order.ASCENDING)
      .sortGroup("_1._2", Order.ASCENDING)
      .reduceGroup(new NestedTupleReducer)
    
    val result: Seq[String] = reduceDs.map(_.toString).collect().sorted
    val expected = Seq[String](
      "a--(1,2)-(1,3)-(2,1)-",
      "b--(2,2)-",
      "c--(3,3)-(3,6)-(4,9)-")
    assertEquals(expected, result)
  }

  /**
   * Test string-based definition on group sort, for (partial) nested Tuple DESC
   */
  @Test
  def testStringBasedDefinitionOnGroupSortForPartialNestedTuple(): Unit = {
    
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds =  CollectionDataSets.getGroupSortedNestedTupleDataSet(env)
    
    val reduceDs =  ds.groupBy("_2")
      .sortGroup("_1._1", Order.DESCENDING)
      .sortGroup("_1._2", Order.ASCENDING)
      .reduceGroup(new NestedTupleReducer)
    
    val result: Seq[String] = reduceDs.map(_.toString()).collect().sorted
    val expected = Seq[String](
      "a--(2,1)-(1,2)-(1,3)-",
      "b--(2,2)-",
      "c--(4,9)-(3,3)-(3,6)-")
    assertEquals(expected, result)
  }

  /**
   * Test string-based definition on group sort, for two grouping keys
   */
  @Test
  def testStringBasedDefinitionOnGroupSortForTwoGroupingKeys(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds =  CollectionDataSets.getGroupSortedNestedTupleDataSet(env)
    
    val reduceDs =  ds.groupBy("_2")
      .sortGroup("_1._1", Order.DESCENDING)
      .sortGroup("_1._2", Order.DESCENDING)
      .reduceGroup(new NestedTupleReducer())
    
    val result: Seq[String] = reduceDs.map(_.toString()).collect().sorted
    val expected = Seq[String] (
      "a--(2,1)-(1,3)-(1,2)-",
      "b--(2,2)-",
      "c--(4,9)-(3,6)-(3,3)-")
    assertEquals(expected, result)
  }

  /**
   * Test string-based definition on group sort, for two grouping keys with Pojos
   */
  @Test
  def testStringBasedDefinitionOnGroupSortForTwoGroupingKeysWithPojos(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val ds =  CollectionDataSets.getGroupSortedPojoContainingTupleAndWritable(env)
    val reduceDs =  ds.groupBy("hadoopFan")
      .sortGroup("theTuple._1", Order.DESCENDING)
      .sortGroup("theTuple._2", Order.DESCENDING)
      .reduceGroup {
        (values, out: Collector[String]) => {
          var once: Boolean = false
          val concat: StringBuilder = new StringBuilder
          for (value <- values) {
            if (!once) {
              concat.append(value.hadoopFan.get)
              concat.append("---")
              once = true
            }
            concat.append(value.theTuple)
            concat.append("-")
          }
          out.collect(concat.toString())
        }
      }
    
    val result: Seq[String] = reduceDs.map(_.toString()).collect().sorted
    val expected = Seq[String](
      "1---(10,100)-",
      "2---(30,600)-(30,400)-(30,200)-(20,201)-(20,200)-")
    assertEquals(expected, result)
  }

  /**
   * check correctness of sorted groupReduce on tuples with keyselector sorting
   */
  @Test
  def testTupleKeySelectorGroupSort(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds =  CollectionDataSets.get3TupleDataSet(env)
    
    val reduceDs =  ds.groupBy(_._2).sortGroup(_._3, Order.DESCENDING).reduceGroup {
      in =>
        in.reduce((l, r) => (l._1 + r._1, l._2, l._3 + "-" + r._3))
    }
    
    val result: Seq[(Int, Long, String)] = reduceDs.collect().sortBy(_._1)
    
    val expected = Seq[(Int, Long, String)](
      (1,1,"Hi"),
      (5,2,"Hello world-Hello"),
      (15,3,"Luke Skywalker-I am fine.-Hello world, how are you?"),
      (34,4,"Comment#4-Comment#3-Comment#2-Comment#1"),
      (65,5,"Comment#9-Comment#8-Comment#7-Comment#6-Comment#5"),
      (111,6,"Comment#15-Comment#14-Comment#13-Comment#12-Comment#11-Comment#10") )
    
    assertEquals(expected, result)
  }

  /**
   * check correctness of sorted groupReduce on custom type with keyselector sorting
   */
  @Test
  def testPojoKeySelectorGroupSort(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds =  CollectionDataSets.getCustomTypeDataSet(env)
    
    val reduceDs =  ds.groupBy(_.myInt).sortGroup(_.myString, Order.DESCENDING).reduceGroup {
      in =>
        val o = new CustomType
        val c = in.next()

        val concat: StringBuilder = new StringBuilder(c.myString)
        o.myInt = c.myInt
        o.myLong = c.myLong

        while (in.hasNext) {
          val next = in.next()
          o.myLong += next.myLong
          concat.append("-").append(next.myString)
        }
        o.myString = concat.toString()
        o
    }
    
    val result: Seq[String] = reduceDs.map(_.toString()).collect().sorted
    
    val expected = Seq[String]( "1,0,Hi",
      "2,3,Hello world-Hello",
      "3,12,Luke Skywalker-I am fine.-Hello world, how are you?",
      "4,30,Comment#4-Comment#3-Comment#2-Comment#1",
      "5,60,Comment#9-Comment#8-Comment#7-Comment#6-Comment#5",
      "6,105,Comment#15-Comment#14-Comment#13-Comment#12-Comment#11-Comment#10" )
    
    assertEquals(expected, result)
  }

  /**
   * check correctness of sorted groupReduce with combine on tuples with keyselector sorting
   */
  @Test
  def testTupleKeySelectorSortWithCombine(): Unit = {
    
    assumeTrue(mode != TestExecutionMode.COLLECTION)
    
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds =  CollectionDataSets.get3TupleDataSet(env)

    val reduceDs =  ds.groupBy(_._2).sortGroup(_._3, Order.DESCENDING)
      .reduceGroup(new Tuple3SortedGroupReduceWithCombine())
    
    val result : Seq[(Int, String)] = reduceDs.collect().sortBy(_._1)
    
    val expected = Seq[(Int, String)]( (1,"Hi"),
        (5,"Hello world-Hello"),
        (15,"Luke Skywalker-I am fine.-Hello world, how are you?"),
        (34,"Comment#4-Comment#3-Comment#2-Comment#1"),
        (65,"Comment#9-Comment#8-Comment#7-Comment#6-Comment#5"),
        (111,"Comment#15-Comment#14-Comment#13-Comment#12-Comment#11-Comment#10") )
    
    assertEquals(expected, result)
  }

  /**
   * check correctness of sorted groupReduceon with Tuple2 keyselector sorting
   */
  @Test
  def testTupleKeySelectorSortCombineOnTuple(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds =  CollectionDataSets.get5TupleDataSet(env)

    val reduceDs = ds.groupBy(_._1).sortGroup(t => (t._5, t._3), Order.DESCENDING).reduceGroup{
      in =>
        val concat: StringBuilder = new StringBuilder
        var sum: Long = 0
        var key = 0
        var s: Long = 0
        while (in.hasNext) {
          val next = in.next()
          sum += next._2
          key = next._1
          s = next._5
          concat.append(next._4).append("-")
        }
        if (concat.nonEmpty) {
          concat.setLength(concat.length - 1)
        }
        (key, sum, 0, concat.toString(), s)
      //            in.reduce((l, r) => (l._1, l._2 + r._2, 0, l._4 + "-" + r._4, l._5))
    }
    
    val result: Seq[(Int, Long, Int, String, Long)] = reduceDs.collect().sortBy(_._1)
    val expected = Seq[(Int, Long, Int, String, Long)](
      (1,1,0,"Hallo",1),
      (2,5,0,"Hallo Welt-Hallo Welt wie",1),
      (3,15,0,"BCD-ABC-Hallo Welt wie gehts?",2),
      (4,34,0,"FGH-CDE-EFG-DEF",1),
      (5,65,0,"IJK-HIJ-KLM-JKL-GHI",1) )
    assertEquals(expected, result)
  }

  /**
   * Test grouping with pojo containing multiple pojos (was a bug)
   */
  @Test
  def testGroupingWithPojoContainingMultiplePojos(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds =  CollectionDataSets.getPojoWithMultiplePojos(env)
    
    val reduceDs =  ds.groupBy("p2.a2").reduceGroup {
      (values, out: Collector[String]) => {
        val concat: StringBuilder = new StringBuilder()
        for (value <- values) {
          concat.append(value.p2.a2)
        }
        out.collect(concat.toString())
      }
    }
    
    val result : Seq[String] = reduceDs.map(_.toString()).collect().sorted
    val expected = Seq[String]("b", "ccc", "ee")
    assertEquals(expected, result)
  }

  @Test
  def testWithAtomic1(): Unit = {
    
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = env.fromElements(0, 1, 1, 2)
    val reduceDs = ds.groupBy("*").reduceGroup((ints: Iterator[Int]) => ints.next())
    
    val result: Seq[Int] = reduceDs.collect().sorted
    val expected = Seq[Int](0, 1, 2)
    assertEquals(expected, result)
  }
}

class OrderCheckingCombinableReduce
  extends GroupReduceFunction[MutableTuple3[Int, Long, String], MutableTuple3[Int, Long, String]]
  with GroupCombineFunction[MutableTuple3[Int, Long, String], MutableTuple3[Int, Long, String]]
{
  
  def reduce(
              values: Iterable[MutableTuple3[Int, Long, String]],
              out: Collector[MutableTuple3[Int, Long, String]]) {
    val it = values.iterator()
    var t = it.next()
    val i = t._1
    out.collect(t)

    while (it.hasNext) {
      t = it.next()
      if (i > t._1 || (t._3 == "INVALID-ORDER!")) {
        t._3 = "INVALID-ORDER!"
        out.collect(t)
      }
    }
  }

  override def combine(
                        values: Iterable[MutableTuple3[Int, Long, String]],
                        out: Collector[MutableTuple3[Int, Long, String]]) {
    val it = values.iterator()
    var t = it.next
    val i: Int = t._1
    out.collect(t)
    while (it.hasNext) {
      t = it.next
      if (i > t._1) {
        t._3 = "INVALID-ORDER!"
        out.collect(t)
      }
    }
  }
}

class CustomTypeGroupReduceWithCombine
  extends GroupReduceFunction[CustomType, CustomType]
  with GroupCombineFunction[CustomType, CustomType]
{
  override def combine(values: Iterable[CustomType], out: Collector[CustomType]): Unit = {
    val o = new CustomType()
    for (c <- values.asScala) {
      o.myInt = c.myInt
      o.myLong += c.myLong
      o.myString = "test" + c.myInt
    }
    out.collect(o)
  }

  override def reduce(values: Iterable[CustomType], out: Collector[CustomType]): Unit = {
    val o = new CustomType(0, 0, "")
    for (c <- values.asScala) {
      o.myInt = c.myInt
      o.myLong += c.myLong
      o.myString = c.myString
    }
    out.collect(o)
  }
}

class Tuple3GroupReduceWithCombine
  extends GroupReduceFunction[(Int, Long, String), (Int, String)]
  with GroupCombineFunction[(Int, Long, String), (Int, Long, String)]
{
  override def combine(
                        values: Iterable[(Int, Long, String)],
                        out: Collector[(Int, Long, String)]): Unit = {
    var i = 0
    var l = 0L
    var s = ""
    for (t <- values.asScala) {
      i += t._1
      l = t._2
      s = "test" + t._2
    }
    out.collect((i, l, s))
  }

  override def reduce(
                       values: Iterable[(Int, Long, String)],
                       out: Collector[(Int, String)]): Unit = {
    var i = 0
    var s = ""
    for (t <- values.asScala) {
      i += t._1
      s = t._3
    }
    out.collect((i, s))
  }
}

class Tuple3AllGroupReduceWithCombine
  extends GroupReduceFunction[(Int, Long, String), (Int, String)]
  with GroupCombineFunction[(Int, Long, String), (Int, Long, String)]
{
  override def combine(
    values: Iterable[(Int, Long, String)], out: Collector[(Int, Long, String)]): Unit =
  {
    var i = 0
    var l = 0L
    var s = ""
    for (t <- values.asScala) {
      i += t._1
      l += t._2
      s += "test"
    }
    out.collect((i, l, s))
  }

  override def reduce(
                       values: Iterable[(Int, Long, String)],
                       out: Collector[(Int, String)]): Unit = {
    var i = 0
    var s = ""
    for (t <- values.asScala) {
      i += t._1 + t._2.toInt
      s += t._3
    }
    out.collect((i, s))
  }
}

class NestedTupleReducer extends GroupReduceFunction[((Int, Int), String), String] {
  def reduce(values: Iterable[((Int, Int), String)], out: Collector[String]) {
    var once: Boolean = false
    val concat: StringBuilder = new StringBuilder
    for (value <- values.asScala) {
      if (!once) {
        concat.append(value._2).append("--")
        once = true
      }
      concat.append(value._1)
      concat.append("-")
    }
    out.collect(concat.toString())
  }
}

class Tuple3SortedGroupReduceWithCombine
  extends GroupReduceFunction[(Int, Long, String), (Int, String)]
  with GroupCombineFunction[(Int, Long, String), (Int, Long, String)]
{

  override def combine(
    values: Iterable[(Int, Long, String)], out: Collector[(Int, Long, String)]): Unit =
  {

    val concat: StringBuilder = new StringBuilder
    var sum = 0
    var key: Long = 0
    for (t <- values.asScala) {
      sum += t._1
      key = t._2
      concat.append(t._3).append("-")
    }
    if (concat.nonEmpty) {
      concat.setLength(concat.length - 1)
    }
    out.collect((sum, key, concat.toString()))
  }

  override def reduce(
                       values: Iterable[(Int, Long, String)],
                       out: Collector[(Int, String)]): Unit = {
    var i = 0
    var s = ""
    for (t <- values.asScala) {
      i += t._1
      s = t._3
    }
    out.collect((i, s))
  }
}


