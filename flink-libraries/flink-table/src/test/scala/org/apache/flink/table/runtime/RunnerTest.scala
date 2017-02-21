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
package org.apache.flink.table.runtime

import org.apache.flink.api.scala._

import org.apache.flink.api.common.functions._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.runtime.RunnerTest.StringCollector
import org.apache.flink.table.codegen.CodeGenerator
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector
import org.junit.Test
import org.junit.Assert.{assertEquals, _}


import org.mockito.Mockito.{mock, when}

import scala.collection.mutable

class RunnerTest {

  @Test
  def testMapRunnerWithConstructorParameter(): Unit = {
    val runtimeContext = mock(classOf[RuntimeContext])
    when(runtimeContext.getUserCodeClassLoader).thenReturn(this.getClass.getClassLoader)

    RunnerTest.clear

    val generator =
      new CodeGenerator(
        TableConfig.DEFAULT,
        false,
        BasicTypeInfo.LONG_TYPE_INFO.asInstanceOf[TypeInformation[Long]])

    val params = generator.addReusableConstructor(classOf[Long])
    val cardinal = params(0)

    val body =
      s"""
         |return java.lang.Long.valueOf(in1) + java.lang.Long.valueOf($cardinal);
         """.stripMargin

    val genFunction = generator.generateFunction(
      "myMapFunction",
      classOf[MapFunction[Long, Long]],
      body,
      BasicTypeInfo.LONG_TYPE_INFO.asInstanceOf[TypeInformation[Long]])

    val data = Array(java.lang.Long.valueOf(10))
    val mapRunner =
      new MapRunner[Long, Long](
        genFunction.name,
        genFunction.code,
        BasicTypeInfo.LONG_TYPE_INFO.asInstanceOf[TypeInformation[Long]],
        data,
        Array[Class[_]](classOf[Long])
      ).asInstanceOf[RichMapFunction[Long, Long]]
    mapRunner.setRuntimeContext(runtimeContext)
    mapRunner.open(new Configuration())

    val expected = mutable.MutableList("10", "11", "12", "13", "14", "15")
    for (i <- 0 to 5)
      RunnerTest.testResults += mapRunner.map(i).toString

    assertEquals(expected.sorted, RunnerTest.testResults.sorted)
  }

  @Test
  def testFlatMapRunnerWithConstructorParameter(): Unit = {
    val runtimeContext = mock(classOf[RuntimeContext])
    when(runtimeContext.getUserCodeClassLoader).thenReturn(this.getClass.getClassLoader)

    RunnerTest.clear

    val generator =
      new CodeGenerator(
        TableConfig.DEFAULT,
        false,
        BasicTypeInfo.LONG_TYPE_INFO.asInstanceOf[TypeInformation[Long]])

    val params = generator.addReusableConstructor(classOf[Long])
    val cardinal = params(0)
    val body =
      s"""
         | c.collect(java.lang.Long.valueOf(in1));
         | c.collect(java.lang.Long.valueOf($cardinal));
         """.stripMargin

    val genFunction = generator.generateFunction(
      "myFlatMapFunction",
      classOf[FlatMapFunction[Long, Long]],
      body,
      BasicTypeInfo.LONG_TYPE_INFO.asInstanceOf[TypeInformation[Long]])

    val data = Array(java.lang.Long.valueOf(10))
    val flatMapRunner =
      new FlatMapRunner[Long, Long](
        genFunction.name,
        genFunction.code,
        BasicTypeInfo.LONG_TYPE_INFO.asInstanceOf[TypeInformation[Long]],
        data,
        Array[Class[_]](classOf[Long])
      ).asInstanceOf[RichFlatMapFunction[Long, Long]]

    flatMapRunner.setRuntimeContext(runtimeContext)
    flatMapRunner.open(new Configuration())

    var testResults = mutable.MutableList.empty[String]
    for (i <- 0 to 5)
      testResults += flatMapRunner.flatMap(i, new StringCollector).toString

    val expected =
      mutable.MutableList("0", "1", "2", "3", "4", "5", "10", "10", "10", "10", "10", "10")

    assertEquals(expected.sorted, RunnerTest.testResults.sorted)
  }

}

object RunnerTest {
  var testResults = mutable.MutableList.empty[String]

  def clear = {
    RunnerTest.testResults.clear()
  }

  final class StringCollector[T] extends Collector[T]() {
    def collect(record: T) {
      testResults.synchronized {
        testResults += record.toString
      }
    }

    def close(): Unit = ???
  }

}
