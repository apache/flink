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
package org.apache.flink.api.scala.operators

import java.lang.Iterable

import org.apache.flink.api.common.functions._
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala.util.CollectionDataSets
import org.apache.flink.api.scala.util.CollectionDataSets.{CrazyNested, POJO, MutableTuple3,
CustomType}
import org.apache.flink.compiler.PactCompiler
import org.apache.flink.configuration.Configuration
import org.apache.flink.test.util.JavaProgramTestBase
import org.apache.flink.util.Collector
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.runners.Parameterized.Parameters

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.flink.api.scala._


object GroupReduceProgs {
  var NUM_PROGRAMS: Int = 26

  def runProgram(progId: Int, resultPath: String, onCollection: Boolean): String = {
    progId match {
      case 1 =>
        /*
         * check correctness of groupReduce on tuples with key field selector
         */
        val env = ExecutionEnvironment.getExecutionEnvironment
        val ds =  CollectionDataSets.get3TupleDataSet(env)
        val reduceDs =  ds.groupBy(1).reduceGroup {
          in =>
            in.map(t => (t._1, t._2)).reduce((l, r) => (l._1 + r._1, l._2))
        }
        reduceDs.writeAsCsv(resultPath)
        env.execute()
        "1,1\n" + "5,2\n" + "15,3\n" + "34,4\n" + "65,5\n" + "111,6\n"

      case 2 =>
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
        reduceDs.writeAsCsv(resultPath)
        env.execute()
        "1,1,0,P-),1\n" + "2,3,0,P-),1\n" + "2,2,0,P-),2\n" + "3,9,0,P-),2\n" + "3,6,0," +
          "P-),3\n" + "4,17,0,P-),1\n" + "4,17,0,P-),2\n" + "5,11,0,P-),1\n" + "5,29,0,P-)," +
          "2\n" + "5,25,0,P-),3\n"

      case 3 =>
        /*
         * check correctness of groupReduce on tuples with key field selector and group sorting
         */
        val env = ExecutionEnvironment.getExecutionEnvironment
        env.setDegreeOfParallelism(1)
        val ds =  CollectionDataSets.get3TupleDataSet(env)
        val reduceDs =  ds.groupBy(1).sortGroup(2, Order.ASCENDING).reduceGroup {
          in =>
            in.reduce((l, r) => (l._1 + r._1, l._2, l._3 + "-" + r._3))
        }
        reduceDs.writeAsCsv(resultPath)
        env.execute()
        "1,1,Hi\n" +
          "5,2,Hello-Hello world\n" +
          "15,3,Hello world, how are you?-I am fine.-Luke Skywalker\n" +
          "34,4,Comment#1-Comment#2-Comment#3-Comment#4\n" +
          "65,5,Comment#5-Comment#6-Comment#7-Comment#8-Comment#9\n" +
          "111,6,Comment#10-Comment#11-Comment#12-Comment#13-Comment#14-Comment#15\n"

      case 4 =>
        /*
         * check correctness of groupReduce on tuples with key extractor
         */
        val env = ExecutionEnvironment.getExecutionEnvironment
        val ds =  CollectionDataSets.get3TupleDataSet(env)
        val reduceDs =  ds.groupBy(_._2).reduceGroup {
          in =>
            in.map(t => (t._1, t._2)).reduce((l, r) => (l._1 + r._1, l._2))
        }
        reduceDs.writeAsCsv(resultPath)
        env.execute()
        "1,1\n" + "5,2\n" + "15,3\n" + "34,4\n" + "65,5\n" + "111,6\n"

      case 5 =>
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
        reduceDs.writeAsText(resultPath)
        env.execute()
        "1,0,Hello!\n" + "2,3,Hello!\n" + "3,12,Hello!\n" + "4,30,Hello!\n" + "5,60," +
          "Hello!\n" + "6,105,Hello!\n"

      case 6 =>
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
        reduceDs.writeAsCsv(resultPath)
        env.execute()
        "231,91,Hello World\n"

      case 7 =>
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
        reduceDs.writeAsText(resultPath)
        env.execute()
        "91,210,Hello!"

      case 8 =>
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
        reduceDs.writeAsCsv(resultPath)
        env.execute()
        "1,1,55\n" + "5,2,55\n" + "15,3,55\n" + "34,4,55\n" + "65,5,55\n" + "111,6,55\n"

      case 9 =>
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
        reduceDs.writeAsCsv(resultPath)
        env.execute()
        "11,1,Hi!\n" + "21,1,Hi again!\n" + "12,2,Hi!\n" + "22,2,Hi again!\n" + "13,2," +
          "Hi!\n" + "23,2,Hi again!\n"

      case 10 =>
        /*
         * check correctness of groupReduce on custom type with key extractor and combine
         */
        val env = ExecutionEnvironment.getExecutionEnvironment
        val ds =  CollectionDataSets.getCustomTypeDataSet(env)

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
        val reduceDs =  ds.groupBy(_.myInt).reduceGroup(new CustomTypeGroupReduceWithCombine)

        reduceDs.writeAsText(resultPath)
        env.execute()
        if (onCollection) {
          null
        }
        else {
          "1,0,test1\n" + "2,3,test2\n" + "3,12,test3\n" + "4,30,test4\n" + "5,60," +
            "test5\n" + "6,105,test6\n"
        }

      case 11 =>
        /*
         * check correctness of groupReduce on tuples with combine
         */
        val env = ExecutionEnvironment.getExecutionEnvironment
        // important because it determines how often the combiner is called
        env.setDegreeOfParallelism(2)
        val ds =  CollectionDataSets.get3TupleDataSet(env)
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
        val reduceDs =  ds.groupBy(1).reduceGroup(new Tuple3GroupReduceWithCombine)
        reduceDs.writeAsCsv(resultPath)
        env.execute()
        if (onCollection) {
          null
        }
        else {
          "1,test1\n" + "5,test2\n" + "15,test3\n" + "34,test4\n" + "65,test5\n" + "111," +
            "test6\n"
        }


      // all-groupreduce with combine


      case 12 =>
        /*
         * check correctness of all-groupreduce for tuples with combine
         */
        val env = ExecutionEnvironment.getExecutionEnvironment
        val ds =  CollectionDataSets.get3TupleDataSet(env).map(t => t).setParallelism(4)

        val cfg: Configuration = new Configuration
        cfg.setString(PactCompiler.HINT_SHIP_STRATEGY, PactCompiler.HINT_SHIP_STRATEGY_REPARTITION)

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
        val reduceDs =  ds.reduceGroup(new Tuple3AllGroupReduceWithCombine).withParameters(cfg)

        reduceDs.writeAsCsv(resultPath)
        env.execute()
        if (onCollection) {
          null
        }
        else {
          "322," +
          "testtesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttest\n"
        }

      case 13 =>
        /*
         * check correctness of groupReduce with descending group sort
         */
        val env = ExecutionEnvironment.getExecutionEnvironment
        env.setDegreeOfParallelism(1)
        val ds =  CollectionDataSets.get3TupleDataSet(env)
        val reduceDs =  ds.groupBy(1).sortGroup(2, Order.DESCENDING).reduceGroup {
          in =>
            in.reduce((l, r) => (l._1 + r._1, l._2, l._3 + "-" + r._3))
        }

        reduceDs.writeAsCsv(resultPath)
        env.execute()
        "1,1,Hi\n" + "5,2,Hello world-Hello\n" + "15,3,Luke Skywalker-I am fine.-Hello " +
          "world, how are you?\n" + "34,4,Comment#4-Comment#3-Comment#2-Comment#1\n" + "65,5," +
          "Comment#9-Comment#8-Comment#7-Comment#6-Comment#5\n" + "111,6," +
          "Comment#15-Comment#14-Comment#13-Comment#12-Comment#11-Comment#10\n"

      case 14 =>
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

        reduceDs.writeAsCsv(resultPath)
        env.execute()
        "1,1,0,P-),1\n" + "2,3,0,P-),1\n" + "2,2,0,P-),2\n" + "3,9,0,P-),2\n" + "3,6,0," +
          "P-),3\n" + "4,17,0,P-),1\n" + "4,17,0,P-),2\n" + "5,11,0,P-),1\n" + "5,29,0,P-)," +
          "2\n" + "5,25,0,P-),3\n"

      case 15 =>
        /*
         * check that input of combiner is also sorted for combinable groupReduce with group 
         * sorting
         */
        val env = ExecutionEnvironment.getExecutionEnvironment
        env.setDegreeOfParallelism(1)
        val ds =  CollectionDataSets.get3TupleDataSet(env).map { t =>
          MutableTuple3(t._1, t._2, t._3)
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
        
        val reduceDs =  ds.groupBy(1)
          .sortGroup(0, Order.ASCENDING).reduceGroup(new OrderCheckingCombinableReduce)
        reduceDs.writeAsCsv(resultPath)
        env.execute()
        "1,1,Hi\n" + "2,2,Hello\n" + "4,3,Hello world, how are you?\n" + "7,4," +
          "Comment#1\n" + "11,5,Comment#5\n" + "16,6,Comment#10\n"
      
      case 16 =>
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
        reduceDs.writeAsCsv(resultPath)
        env.execute()
        "aa,1\nbb,2\ncc,3\n"


      case 17 =>
        // We don't have that test but keep numbering compatible to Java GroupReduceITCase
        val env = ExecutionEnvironment.getExecutionEnvironment
        env.fromElements("Hello world").writeAsText(resultPath)
        env.execute()
        "Hello world"

      case 18 =>
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
        reduceDs.writeAsText(resultPath)
        env.execute()
        "1\n5\n"

      case 19 =>
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
        reduceDs.writeAsText(resultPath)
        env.execute()
        "3\n1\n"

      case 20 =>
        /*
         * Test string-based definition on group sort, based on test:
         * check correctness of groupReduce with descending group sort
         */
        val env = ExecutionEnvironment.getExecutionEnvironment
        env.setDegreeOfParallelism(1)
        val ds =  CollectionDataSets.get3TupleDataSet(env)
        val reduceDs =  ds.groupBy(1)
          .sortGroup("_3", Order.DESCENDING)
          .reduceGroup {
          in =>
            in.reduce((l, r) => (l._1 + r._1, l._2, l._3 + "-" + r._3))
        }
        reduceDs.writeAsCsv(resultPath)
        env.execute()
        "1,1,Hi\n" + "5,2,Hello world-Hello\n" + "15,3,Luke Skywalker-I am fine.-Hello " +
          "world, how are you?\n" + "34,4,Comment#4-Comment#3-Comment#2-Comment#1\n" + "65,5," +
          "Comment#9-Comment#8-Comment#7-Comment#6-Comment#5\n" + "111,6," +
          "Comment#15-Comment#14-Comment#13-Comment#12-Comment#11-Comment#10\n"

      case 21 =>
        /*
         * Test int-based definition on group sort, for (full) nested Tuple
         */
        val env = ExecutionEnvironment.getExecutionEnvironment
        env.setDegreeOfParallelism(1)
        val ds =  CollectionDataSets.getGroupSortedNestedTupleDataSet(env)
        val reduceDs =  ds.groupBy("_2").sortGroup(0, Order.DESCENDING)
          .reduceGroup(new NestedTupleReducer)
        reduceDs.writeAsText(resultPath)
        env.execute()
        "a--(2,1)-(1,3)-(1,2)-\n" + "b--(2,2)-\n" + "c--(4,9)-(3,6)-(3,3)-\n"


      case 22 =>
        /*
         * Test int-based definition on group sort, for (partial) nested Tuple ASC
         */
        val env = ExecutionEnvironment.getExecutionEnvironment
        env.setDegreeOfParallelism(1)
        val ds =  CollectionDataSets.getGroupSortedNestedTupleDataSet(env)
        val reduceDs =  ds.groupBy("_2")
          .sortGroup("_1._1", Order.ASCENDING)
          .sortGroup("_1._2", Order.ASCENDING)
          .reduceGroup(new NestedTupleReducer)
        reduceDs.writeAsText(resultPath)
        env.execute()
        "a--(1,2)-(1,3)-(2,1)-\n" + "b--(2,2)-\n" + "c--(3,3)-(3,6)-(4,9)-\n"

      case 23 =>
        /*
         * Test string-based definition on group sort, for (partial) nested Tuple DESC
         */
        val env = ExecutionEnvironment.getExecutionEnvironment
        env.setDegreeOfParallelism(1)
        val ds =  CollectionDataSets.getGroupSortedNestedTupleDataSet(env)
        val reduceDs =  ds.groupBy("_2")
          .sortGroup("_1._1", Order.DESCENDING)
          .sortGroup("_1._2", Order.ASCENDING)
          .reduceGroup(new NestedTupleReducer)
        reduceDs.writeAsText(resultPath)
        env.execute()
        "a--(2,1)-(1,2)-(1,3)-\n" + "b--(2,2)-\n" + "c--(4,9)-(3,3)-(3,6)-\n"

      case 24 =>
        /*
         * Test string-based definition on group sort, for two grouping keys
         */
        val env = ExecutionEnvironment.getExecutionEnvironment
        env.setDegreeOfParallelism(1)
        val ds =  CollectionDataSets.getGroupSortedNestedTupleDataSet(env)
        val reduceDs =  ds.groupBy("_2")
          .sortGroup("_1._1", Order.DESCENDING)
          .sortGroup("_1._2", Order.DESCENDING)
          .reduceGroup(new NestedTupleReducer)
        reduceDs.writeAsText(resultPath)
        env.execute()
        "a--(2,1)-(1,3)-(1,2)-\n" + "b--(2,2)-\n" + "c--(4,9)-(3,6)-(3,3)-\n"

      case 25 =>
        /*
         * Test string-based definition on group sort, for two grouping keys with Pojos
         */
        val env = ExecutionEnvironment.getExecutionEnvironment
        env.setDegreeOfParallelism(1)
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
        reduceDs.writeAsText(resultPath)
        env.execute()
        "1---(10,100)-\n" + "2---(30,600)-(30,400)-(30,200)-(20,201)-(20,200)-\n"

      case 26 =>
        /*
         * Test grouping with pojo containing multiple pojos (was a bug)
         */
        val env = ExecutionEnvironment.getExecutionEnvironment
        env.setDegreeOfParallelism(1)
        val ds =  CollectionDataSets.getPojoWithMultiplePojos(env)
        val reduceDs =  ds.groupBy("p2.a2")
          .reduceGroup(
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
            })
        reduceDs.writeAsText(resultPath)
        env.execute()
        "b\nccc\nee\n"

      case _ =>
        throw new IllegalArgumentException("Invalid program id")
    }
  }
}


@RunWith(classOf[Parameterized])
class GroupReduceITCase(config: Configuration) extends JavaProgramTestBase(config) {

  private val curProgId: Int = config.getInteger("ProgramId", -1)
  private var resultPath: String = null
  private var expectedResult: String = null

  protected override def preSubmit(): Unit = {
    resultPath = getTempDirPath("result")
  }

  protected def testProgram(): Unit = {
    expectedResult = GroupReduceProgs.runProgram(curProgId, resultPath, isCollectionExecution)
  }

  protected override def postSubmit(): Unit = {
    if (expectedResult != null) compareResultsByLinesInMemory(expectedResult, resultPath)
  }
}

object GroupReduceITCase {
  @Parameters
  def getConfigurations: java.util.Collection[Array[AnyRef]] = {
    val configs = mutable.MutableList[Array[AnyRef]]()
    for (i <- 1 to GroupReduceProgs.NUM_PROGRAMS) {
      val config = new Configuration()
      config.setInteger("ProgramId", i)
      configs += Array(config)
    }

    configs.asJavaCollection
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


