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
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala.util.CollectionDataSets
import org.apache.flink.api.scala.util.CollectionDataSets.{CrazyNested, POJO, MutableTuple3,
CustomType}
import org.apache.flink.optimizer.Optimizer
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.test.util.MultipleProgramsTestBase.TestExecutionMode
import org.apache.flink.test.util.{TestBaseUtils, MultipleProgramsTestBase}
import org.apache.flink.util.Collector
import org.hamcrest.core.{IsNot, IsEqual}
import org.junit._
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.JavaConverters._

import org.apache.flink.api.scala._

@RunWith(classOf[Parameterized])
class GroupReduceITCase(mode: TestExecutionMode) extends MultipleProgramsTestBase(mode) {
  private var resultPath: String = null
  private var expected: String = null
  private val _tempFolder = new TemporaryFolder()

  @Rule
  def tempFolder = _tempFolder

  @Before
  def before(): Unit = {
    resultPath = tempFolder.newFile().toURI.toString
  }

  @After
  def after(): Unit = {
    if(expected != null) {
      TestBaseUtils.compareResultsByLinesInMemory(expected, resultPath)
    }
  }

  @Test
  def testCorrectnessOfGroupReduceOnTuplesWithKeyFieldSelector(): Unit = {
    /*
     * check correctness of groupReduce on tuples with key field selector
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds =  CollectionDataSets.get3TupleDataSet(env)
    val reduceDs =  ds.groupBy(1).reduceGroup {
      in =>
        in.map(t => (t._1, t._2)).reduce((l, r) => (l._1 + r._1, l._2))
    }
    reduceDs.writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expected = "1,1\n" + "5,2\n" + "15,3\n" + "34,4\n" + "65,5\n" + "111,6\n"
  }

  @Test
  def testCorrectnessOfGroupReduceOnTuplesWithMultipleKeyFieldSelector(): Unit = {
    /*
     * check correctness of groupReduce on tuples with multiple key field selector
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds =  CollectionDataSets
      .get5TupleDataSet(env)
    val reduceDs =  ds.groupBy(4, 0).reduceGroup {
      in =>
        val (i, l, l2) = in
          .map( t => (t._1, t._2, t._5))
          .reduce((l, r) => (l._1, l._2 + r._2, l._3))
        (i, l, 0, "P-)", l2)
    }
    reduceDs.writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expected = "1,1,0,P-),1\n" + "2,3,0,P-),1\n" + "2,2,0,P-),2\n" + "3,9,0,P-),2\n" + "3,6,0," +
      "P-),3\n" + "4,17,0,P-),1\n" + "4,17,0,P-),2\n" + "5,11,0,P-),1\n" + "5,29,0,P-)," +
      "2\n" + "5,25,0,P-),3\n"
  }

  @Test
  def testCorrectnessOfGroupReduceOnTuplesWithKeyFieldSelectorAndGroupSorting(): Unit = {
    /*
     * check correctness of groupReduce on tuples with key field selector and group sorting
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val ds =  CollectionDataSets.get3TupleDataSet(env)
    val reduceDs =  ds.groupBy(1).sortGroup(2, Order.ASCENDING).reduceGroup {
      in =>
        in.reduce((l, r) => (l._1 + r._1, l._2, l._3 + "-" + r._3))
    }
    reduceDs.writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expected = "1,1,Hi\n" +
      "5,2,Hello-Hello world\n" +
      "15,3,Hello world, how are you?-I am fine.-Luke Skywalker\n" +
      "34,4,Comment#1-Comment#2-Comment#3-Comment#4\n" +
      "65,5,Comment#5-Comment#6-Comment#7-Comment#8-Comment#9\n" +
      "111,6,Comment#10-Comment#11-Comment#12-Comment#13-Comment#14-Comment#15\n"
  }

  @Test
  def testCorrectnessOfGroupReduceOnTuplesWithKeyExtractor(): Unit = {
    /*
     * check correctness of groupReduce on tuples with key extractor
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds =  CollectionDataSets.get3TupleDataSet(env)
    val reduceDs =  ds.groupBy(_._2).reduceGroup {
      in =>
        in.map(t => (t._1, t._2)).reduce((l, r) => (l._1 + r._1, l._2))
    }
    reduceDs.writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expected = "1,1\n" + "5,2\n" + "15,3\n" + "34,4\n" + "65,5\n" + "111,6\n"
  }

  @Test
  def testCorrectnessOfGroupReduceOnCustomTypeWithTypeExtractor(): Unit = {
    /*
     * check correctness of groupReduce on custom type with type extractor
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds =  CollectionDataSets.getCustomTypeDataSet(env)
    val reduceDs =  ds.groupBy(_.myInt).reduceGroup {
      in =>
        val iter = in.toIterator
        val o = new CustomType
        val c = iter.next()

        o.myString = "Hello!"
        o.myInt = c.myInt
        o.myLong = c.myLong

        while (iter.hasNext) {
          val next = iter.next()
          o.myLong += next.myLong
        }
        o
    }
    reduceDs.writeAsText(resultPath, WriteMode.OVERWRITE)
    env.execute()
    expected = "1,0,Hello!\n" + "2,3,Hello!\n" + "3,12,Hello!\n" + "4,30,Hello!\n" + "5,60," +
      "Hello!\n" + "6,105,Hello!\n"
  }

  @Test
  def testCorrectnessOfAllGroupReduceForTuples(): Unit = {
    /*
     * check correctness of all-groupreduce for tuples
     */
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
    reduceDs.writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expected = "231,91,Hello World\n"
  }

  @Test
  def testCorrectnessOfAllGroupReduceForCustomTypes(): Unit = {
    /*
     * check correctness of all-groupreduce for custom types
     */
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
    reduceDs.writeAsText(resultPath, WriteMode.OVERWRITE)
    env.execute()
    expected = "91,210,Hello!"
  }

  @Test
  def testCorrectnessOfGroupReduceWithBroadcastSet(): Unit = {
    /*
     * check correctness of groupReduce with broadcast set
     */
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
    reduceDs.writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expected = "1,1,55\n" + "5,2,55\n" + "15,3,55\n" + "34,4,55\n" + "65,5,55\n" + "111,6,55\n"
  }

  @Test
  def testCorrectnessOfGroupReduceIfUDFReturnsInputObjectMultipleTimesWhileChangingIt(): Unit = {
    /*
     * check correctness of groupReduce if UDF returns input objects multiple times and
     * changes it in between
     */
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
    reduceDs.writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expected = "11,1,Hi!\n" + "21,1,Hi again!\n" + "12,2,Hi!\n" + "22,2,Hi again!\n" + "13,2," +
      "Hi!\n" + "23,2,Hi again!\n"
  }

  @Test
  def testCorrectnessOfGroupReduceOnCustomTypeWithKeyExtractorAndCombine(): Unit = {
    /*
     * check correctness of groupReduce on custom type with key extractor and combine
     */
    org.junit.Assume.assumeThat(mode, new IsNot(new IsEqual(TestExecutionMode.COLLECTION)))

    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds =  CollectionDataSets.getCustomTypeDataSet(env)

    val reduceDs =  ds.groupBy(_.myInt).reduceGroup(new CustomTypeGroupReduceWithCombine)

    reduceDs.writeAsText(resultPath, WriteMode.OVERWRITE)
    env.execute()
    expected =
      "1,0,test1\n" + "2,3,test2\n" + "3,12,test3\n" + "4,30,test4\n" + "5,60," +
        "test5\n" + "6,105,test6\n"

  }

  @Test
  def testCorrectnessOfGroupReduceOnTuplesWithCombine(): Unit = {
    /*
     * check correctness of groupReduce on tuples with combine
     */
    org.junit.Assume.assumeThat(mode, new IsNot(new IsEqual(TestExecutionMode.COLLECTION)))

    val env = ExecutionEnvironment.getExecutionEnvironment
    // important because it determines how often the combiner is called
    env.setParallelism(2)
    val ds =  CollectionDataSets.get3TupleDataSet(env)

    val reduceDs =  ds.groupBy(1).reduceGroup(new Tuple3GroupReduceWithCombine)
    reduceDs.writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expected =
      "1,test1\n" + "5,test2\n" + "15,test3\n" + "34,test4\n" + "65,test5\n" + "111," +
        "test6\n"

  }

  @Test
  def testCorrectnessOfAllGroupReduceForTuplesWithCombine(): Unit = {
    /*
     * check correctness of all-groupreduce for tuples with combine
     */
    org.junit.Assume.assumeThat(mode, new IsNot(new IsEqual(TestExecutionMode.COLLECTION)))

    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds =  CollectionDataSets.get3TupleDataSet(env).map(t => t).setParallelism(4)

    val cfg: Configuration = new Configuration
    cfg.setString(Optimizer.HINT_SHIP_STRATEGY, Optimizer.HINT_SHIP_STRATEGY_REPARTITION)

    val reduceDs =  ds.reduceGroup(new Tuple3AllGroupReduceWithCombine).withParameters(cfg)

    reduceDs.writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expected = "322," +
        "testtesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttest\n"

  }

  @Test
  def testCorrectnessOfGroupReduceWithDescendingGroupSort(): Unit = {
    /*
     * check correctness of groupReduce with descending group sort
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val ds =  CollectionDataSets.get3TupleDataSet(env)
    val reduceDs =  ds.groupBy(1).sortGroup(2, Order.DESCENDING).reduceGroup {
      in =>
        in.reduce((l, r) => (l._1 + r._1, l._2, l._3 + "-" + r._3))
    }

    reduceDs.writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expected = "1,1,Hi\n" + "5,2,Hello world-Hello\n" + "15,3,Luke Skywalker-I am fine.-Hello " +
      "world, how are you?\n" + "34,4,Comment#4-Comment#3-Comment#2-Comment#1\n" + "65,5," +
      "Comment#9-Comment#8-Comment#7-Comment#6-Comment#5\n" + "111,6," +
      "Comment#15-Comment#14-Comment#13-Comment#12-Comment#11-Comment#10\n"
  }

  @Test
  def testCorrectnessOfGroupReduceOnTuplesWithTupleReturningKeySelector(): Unit = {
    /*
     * check correctness of groupReduce on tuples with tuple-returning key selector
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds =  CollectionDataSets
      .get5TupleDataSet(env)
    val reduceDs = ds.groupBy( t => (t._1, t._5)).reduceGroup {
      in =>
        val (i, l, l2) = in
          .map( t => (t._1, t._2, t._5))
          .reduce((l, r) => (l._1, l._2 + r._2, l._3))
        (i, l, 0, "P-)", l2)
    }

    reduceDs.writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expected = "1,1,0,P-),1\n" + "2,3,0,P-),1\n" + "2,2,0,P-),2\n" + "3,9,0,P-),2\n" + "3,6,0," +
      "P-),3\n" + "4,17,0,P-),1\n" + "4,17,0,P-),2\n" + "5,11,0,P-),1\n" + "5,29,0,P-)," +
      "2\n" + "5,25,0,P-),3\n"
  }

  @Test
  def testInputOfCombinerIsSortedForCombinableGroupReduceWithGroupSorting(): Unit = {
    /*
     * check that input of combiner is also sorted for combinable groupReduce with group
     * sorting
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val ds =  CollectionDataSets.get3TupleDataSet(env).map { t =>
      MutableTuple3(t._1, t._2, t._3)
    }

    val reduceDs =  ds.groupBy(1)
      .sortGroup(0, Order.ASCENDING).reduceGroup(new OrderCheckingCombinableReduce)
    reduceDs.writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expected = "1,1,Hi\n" + "2,2,Hello\n" + "4,3,Hello world, how are you?\n" + "7,4," +
      "Comment#1\n" + "11,5,Comment#5\n" + "16,6,Comment#10\n"
  }

  @Test
  def testDeepNestingAndNullValueInPojo(): Unit = {
    /*
     * Deep nesting test
     * + null value in pojo
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds =  CollectionDataSets
      .getCrazyNestedDataSet(env)
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
    reduceDs.writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expected = "aa,1\nbb,2\ncc,3\n"
  }

  @Test
  def testPojoContainigAWritableAndTuples(): Unit = {
    /*
     * Test Pojo containing a Writable and Tuples
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds =  CollectionDataSets
      .getPojoContainingTupleAndWritable(env)
    val reduceDs =  ds.groupBy("hadoopFan", "theTuple.*").reduceGroup(new
        GroupReduceFunction[CollectionDataSets.PojoContainingTupleAndWritable, Integer] {
      def reduce(
                  values: Iterable[CollectionDataSets.PojoContainingTupleAndWritable],
                  out: Collector[Integer]) {
        var c: Int = 0
        for (v <- values.asScala) {
          c += 1
        }
        out.collect(c)
      }
    })
    reduceDs.writeAsText(resultPath, WriteMode.OVERWRITE)
    env.execute()
    expected = "1\n5\n"
  }

  @Test
  def testTupleContainingPojosAndRegularFields(): Unit ={
    /*
     * Test Tuple containing pojos and regular fields
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.getTupleContainingPojos(env)
    val reduceDs =  ds.groupBy("_1", "_2.*").reduceGroup(
      new GroupReduceFunction[(Int, CrazyNested, POJO), Int] {
        def reduce(values: Iterable[(Int, CrazyNested, POJO)], out: Collector[Int]) {
          var c: Int = 0
          for (v <- values.asScala) {
            c += 1
          }
          out.collect(c)
        }
      })
    reduceDs.writeAsText(resultPath, WriteMode.OVERWRITE)
    env.execute()
    expected = "3\n1\n"
  }

  @Test
  def testStringBasedDefinitionOnGroupSort(): Unit = {
    /*
     * Test string-based definition on group sort, based on test:
     * check correctness of groupReduce with descending group sort
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val ds =  CollectionDataSets.get3TupleDataSet(env)
    val reduceDs =  ds.groupBy(1)
      .sortGroup("_3", Order.DESCENDING)
      .reduceGroup {
      in =>
        in.reduce((l, r) => (l._1 + r._1, l._2, l._3 + "-" + r._3))
    }
    reduceDs.writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expected = "1,1,Hi\n" + "5,2,Hello world-Hello\n" + "15,3,Luke Skywalker-I am fine.-Hello " +
      "world, how are you?\n" + "34,4,Comment#4-Comment#3-Comment#2-Comment#1\n" + "65,5," +
      "Comment#9-Comment#8-Comment#7-Comment#6-Comment#5\n" + "111,6," +
      "Comment#15-Comment#14-Comment#13-Comment#12-Comment#11-Comment#10\n"
  }

  @Test
  def testIntBasedDefinitionOnGroupSortForFullNestedTuple(): Unit = {
    /*
     * Test int-based definition on group sort, for (full) nested Tuple
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val ds =  CollectionDataSets.getGroupSortedNestedTupleDataSet(env)
    val reduceDs =  ds.groupBy("_2").sortGroup(0, Order.DESCENDING)
      .reduceGroup(new NestedTupleReducer)
    reduceDs.writeAsText(resultPath, WriteMode.OVERWRITE)
    env.execute()
    expected = "a--(2,1)-(1,3)-(1,2)-\n" + "b--(2,2)-\n" + "c--(4,9)-(3,6)-(3,3)-\n"
  }

  @Test
  def testIntBasedDefinitionOnGroupSortForPartialNestedTuple(): Unit = {
    /*
     * Test int-based definition on group sort, for (partial) nested Tuple ASC
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val ds =  CollectionDataSets.getGroupSortedNestedTupleDataSet(env)
    val reduceDs =  ds.groupBy("_2")
      .sortGroup("_1._1", Order.ASCENDING)
      .sortGroup("_1._2", Order.ASCENDING)
      .reduceGroup(new NestedTupleReducer)
    reduceDs.writeAsText(resultPath, WriteMode.OVERWRITE)
    env.execute()
    expected = "a--(1,2)-(1,3)-(2,1)-\n" + "b--(2,2)-\n" + "c--(3,3)-(3,6)-(4,9)-\n"
  }

  @Test
  def testStringBasedDefinitionOnGroupSortForPartialNestedTuple(): Unit = {
    /*
     * Test string-based definition on group sort, for (partial) nested Tuple DESC
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val ds =  CollectionDataSets.getGroupSortedNestedTupleDataSet(env)
    val reduceDs =  ds.groupBy("_2")
      .sortGroup("_1._1", Order.DESCENDING)
      .sortGroup("_1._2", Order.ASCENDING)
      .reduceGroup(new NestedTupleReducer)
    reduceDs.writeAsText(resultPath, WriteMode.OVERWRITE)
    env.execute()
    expected = "a--(2,1)-(1,2)-(1,3)-\n" + "b--(2,2)-\n" + "c--(4,9)-(3,3)-(3,6)-\n"
  }

  @Test
  def testStringBasedDefinitionOnGroupSortForTwoGroupingKeys(): Unit = {
    /*
     * Test string-based definition on group sort, for two grouping keys
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val ds =  CollectionDataSets.getGroupSortedNestedTupleDataSet(env)
    val reduceDs =  ds.groupBy("_2")
      .sortGroup("_1._1", Order.DESCENDING)
      .sortGroup("_1._2", Order.DESCENDING)
      .reduceGroup(new NestedTupleReducer)
    reduceDs.writeAsText(resultPath, WriteMode.OVERWRITE)
    env.execute()
    expected = "a--(2,1)-(1,3)-(1,2)-\n" + "b--(2,2)-\n" + "c--(4,9)-(3,6)-(3,3)-\n"
  }

  @Test
  def testStringBasedDefinitionOnGroupSortForTwoGroupingKeysWithPojos(): Unit = {
    /*
     * Test string-based definition on group sort, for two grouping keys with Pojos
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val ds =  CollectionDataSets.getGroupSortedPojoContainingTupleAndWritable(env)
    val reduceDs =  ds.groupBy("hadoopFan")
      .sortGroup("theTuple._1", Order.DESCENDING)
      .sortGroup("theTuple._2", Order.DESCENDING)
      .reduceGroup(
        new GroupReduceFunction[CollectionDataSets.PojoContainingTupleAndWritable, String] {
          def reduce(
                      values: Iterable[CollectionDataSets.PojoContainingTupleAndWritable],
                      out: Collector[String]) {
            var once: Boolean = false
            val concat: StringBuilder = new StringBuilder
            for (value <- values.asScala) {
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
        })
    reduceDs.writeAsText(resultPath, WriteMode.OVERWRITE)
    env.execute()
    expected = "1---(10,100)-\n" + "2---(30,600)-(30,400)-(30,200)-(20,201)-(20,200)-\n"
  }

  @Test
  def testTupleKeySelectorGroupSort: Unit = {
    /*
     * check correctness of sorted groupReduce on tuples with keyselector sorting
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val ds =  CollectionDataSets.get3TupleDataSet(env)
    val reduceDs =  ds.groupBy(_._2).sortGroup(_._3, Order.DESCENDING).reduceGroup {
      in =>
        in.reduce((l, r) => (l._1 + r._1, l._2, l._3 + "-" + r._3))
    }
    reduceDs.writeAsCsv(resultPath)
    env.execute()
    expected = "1,1,Hi\n" +
      "5,2,Hello world-Hello\n" +
      "15,3,Luke Skywalker-I am fine.-Hello world, how are you?\n" +
      "34,4,Comment#4-Comment#3-Comment#2-Comment#1\n" +
      "65,5,Comment#9-Comment#8-Comment#7-Comment#6-Comment#5\n" +
      "111,6,Comment#15-Comment#14-Comment#13-Comment#12-Comment#11-Comment#10\n"
  }

  @Test
  def testPojoKeySelectorGroupSort: Unit = {
    /*
   * check correctness of sorted groupReduce on custom type with keyselector sorting
   */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds =  CollectionDataSets.getCustomTypeDataSet(env)
    val reduceDs =  ds.groupBy(_.myInt).sortGroup(_.myString, Order.DESCENDING).reduceGroup {
      in =>
        val iter = in.toIterator
        val o = new CustomType
        val c = iter.next()

        val concat: StringBuilder = new StringBuilder(c.myString)
        o.myInt = c.myInt
        o.myLong = c.myLong

        while (iter.hasNext) {
          val next = iter.next()
          o.myLong += next.myLong
          concat.append("-").append(next.myString)
        }
        o.myString = concat.toString()
        o
    }
    reduceDs.writeAsText(resultPath)
    env.execute()
    expected = "1,0,Hi\n" +
      "2,3,Hello world-Hello\n" +
      "3,12,Luke Skywalker-I am fine.-Hello world, how are you?\n" +
      "4,30,Comment#4-Comment#3-Comment#2-Comment#1\n" +
      "5,60,Comment#9-Comment#8-Comment#7-Comment#6-Comment#5\n" +
      "6,105,Comment#15-Comment#14-Comment#13-Comment#12-Comment#11-Comment#10\n"
  }

  @Test
  def testTupleKeySelectorSortWithCombine: Unit = {
    /*
     * check correctness of sorted groupReduce with combine on tuples with keyselector sorting
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val ds =  CollectionDataSets.get3TupleDataSet(env)

    val reduceDs =  ds.groupBy(_._2).sortGroup(_._3, Order.DESCENDING)
      .reduceGroup(new Tuple3SortedGroupReduceWithCombine)
    reduceDs.writeAsCsv(resultPath)
    env.execute()
    if (mode == TestExecutionMode.COLLECTION) {
      expected = null
    } else {
      expected = "1,Hi\n" +
        "5,Hello world-Hello\n" +
        "15,Luke Skywalker-I am fine.-Hello world, how are you?\n" +
        "34,Comment#4-Comment#3-Comment#2-Comment#1\n" +
        "65,Comment#9-Comment#8-Comment#7-Comment#6-Comment#5\n" +
        "111,Comment#15-Comment#14-Comment#13-Comment#12-Comment#11-Comment#10\n"
    }
  }

  @Test
  def testTupleKeySelectorSortCombineOnTuple: Unit = {
    /*
     * check correctness of sorted groupReduceon with Tuple2 keyselector sorting
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val ds =  CollectionDataSets.get5TupleDataSet(env)

    val reduceDs = ds.groupBy(_._1).sortGroup(t => (t._5, t._3), Order.DESCENDING).reduceGroup{
      in =>
        val iter = in.toIterator
        val concat: StringBuilder = new StringBuilder
        var sum: Long = 0
        var key = 0
        var s: Long = 0
        while (iter.hasNext) {
          val next = iter.next()
          sum += next._2
          key = next._1
          s = next._5
          concat.append(next._4).append("-")
        }
        if (concat.length > 0) {
          concat.setLength(concat.length - 1)
        }
        (key, sum, 0, concat.toString(), s)
      //            in.reduce((l, r) => (l._1, l._2 + r._2, 0, l._4 + "-" + r._4, l._5))
    }
    reduceDs.writeAsCsv(resultPath)
    env.execute()
    expected = "1,1,0,Hallo,1\n" +
      "2,5,0,Hallo Welt-Hallo Welt wie,1\n" +
      "3,15,0,BCD-ABC-Hallo Welt wie gehts?,2\n" +
      "4,34,0,FGH-CDE-EFG-DEF,1\n" +
      "5,65,0,IJK-HIJ-KLM-JKL-GHI,1\n"
  }


  @Test
  def testGroupingWithPojoContainingMultiplePojos: Unit = {
    /*
     * Test grouping with pojo containing multiple pojos (was a bug)
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val ds =  CollectionDataSets.getPojoWithMultiplePojos(env)
    val reduceDs =  ds.groupBy("p2.a2")
      .reduceGroup {
      new GroupReduceFunction[CollectionDataSets.PojoWithMultiplePojos, String] {
        def reduce(
                    values: Iterable[CollectionDataSets.PojoWithMultiplePojos],
                    out: Collector[String]) {
          val concat: StringBuilder = new StringBuilder
          for (value <- values.asScala) {
            concat.append(value.p2.a2)
          }
          out.collect(concat.toString())
        }
      }
    }
    reduceDs.writeAsText(resultPath, WriteMode.OVERWRITE)
    env.execute()
    expected = "b\nccc\nee\n"
  }

  @Test
  def testWithAtomic1: Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = env.fromElements(0, 1, 1, 2)
    val reduceDs = ds.groupBy("*").reduceGroup((ints: Iterator[Int]) => ints.next())
    reduceDs.writeAsText(resultPath, WriteMode.OVERWRITE)
    env.execute()
    expected = "0\n1\n2"
  }
}

@RichGroupReduceFunction.Combinable
class OrderCheckingCombinableReduce
  extends RichGroupReduceFunction[MutableTuple3[Int, Long, String],
    MutableTuple3[Int, Long, String]] {
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

@RichGroupReduceFunction.Combinable
class CustomTypeGroupReduceWithCombine
  extends RichGroupReduceFunction[CustomType, CustomType] {
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

@RichGroupReduceFunction.Combinable
class Tuple3GroupReduceWithCombine
  extends RichGroupReduceFunction[(Int, Long, String), (Int, String)] {
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

@RichGroupReduceFunction.Combinable
class Tuple3AllGroupReduceWithCombine
  extends RichGroupReduceFunction[(Int, Long, String), (Int, String)] {
  override def combine(
                        values: Iterable[(Int, Long, String)],
                        out: Collector[(Int, Long, String)]): Unit = {
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

@RichGroupReduceFunction.Combinable
class Tuple3SortedGroupReduceWithCombine
  extends RichGroupReduceFunction[(Int, Long, String), (Int, String)] {

  override def combine(
                        values: Iterable[(Int, Long, String)],
                        out: Collector[(Int, Long, String)]): Unit = {
    val concat: StringBuilder = new StringBuilder
    var sum = 0
    var key: Long = 0
    for (t <- values.asScala) {
      sum += t._1
      key = t._2
      concat.append(t._3).append("-")
    }
    if (concat.length > 0) {
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


