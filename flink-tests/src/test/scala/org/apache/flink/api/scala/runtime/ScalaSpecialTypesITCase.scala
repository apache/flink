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
package org.apache.flink.api.scala.runtime

import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala.util.CollectionDataSets
import org.apache.flink.configuration.Configuration
import org.apache.flink.test.util.JavaProgramTestBase
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.runners.Parameterized.Parameters

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.flink.api.scala._


@RunWith(classOf[Parameterized])
class ScalaSpecialTypesITCase(config: Configuration) extends JavaProgramTestBase(config) {

  private var curProgId: Int = config.getInteger("ProgramId", -1)
  private var resultPath: String = null
  private var expectedResult: String = null

  protected override def preSubmit(): Unit = {
    resultPath = getTempDirPath("result")
  }

  protected def testProgram(): Unit = {
    expectedResult = curProgId match {
      case 1 =>
        val env = ExecutionEnvironment.getExecutionEnvironment
        val nums = env.fromElements(1, 2, 1, 2)

        val eithers = nums.map(_ match {
          case 1 => Left(10)
          case 2 => Right(20)
        })

        val result = eithers.map(_ match {
          case Left(i) => i
          case Right(i) => i
        }).reduce(_ + _).writeAsText(resultPath)

        env.execute()

        "60"

      case 2 =>
        val env = ExecutionEnvironment.getExecutionEnvironment
        val nums = env.fromElements(1, 2, 1, 2)

        val eithers = nums.map(_ match {
          case 1 => Left(10)
          case 2 => Left(20)
        })

        val result = eithers.map(_ match {
          case Left(i) => i
        }).reduce(_ + _).writeAsText(resultPath)

        env.execute()

        "60"

      case 3 =>
        val env = ExecutionEnvironment.getExecutionEnvironment
        val nums = env.fromElements(1, 2, 1, 2)

        val eithers = nums.map(_ match {
          case 1 => Right(10)
          case 2 => Right(20)
        })

        val result = eithers.map(_ match {
          case Right(i) => i
        }).reduce(_ + _).writeAsText(resultPath)

        env.execute()

        "60"

      case 4 =>
        val env = ExecutionEnvironment.getExecutionEnvironment
        val nums = env.fromElements(1, 2, 1, 2)

        val eithers = nums.map(_ match {
          case 1 => Some(10)
          case 2 => None
        })

        val result = eithers.map(_ match {
          case Some(i) => i
          case None => 20
        }).reduce(_ + _).writeAsText(resultPath)

        env.execute()

        "60"

      case 5 =>
        val env = ExecutionEnvironment.getExecutionEnvironment
        val nums = env.fromElements(1, 2, 1, 2)

        val eithers = nums.map(_ match {
          case 1 => Some(10)
          case 2 => Some(20)
        })

        val result = eithers.map(_ match {
          case Some(i) => i
        }).reduce(_ + _).writeAsText(resultPath)

        env.execute()

        "60"

      case 6 =>
        val env = ExecutionEnvironment.getExecutionEnvironment
        val nums = env.fromElements(1, 2, 1, 2)

        val eithers = nums.map(_ match {
          case 1 => None
          case 2 => None
        })

        val result = eithers.map(_ match {
          case None => 20
        }).reduce(_ + _).writeAsText(resultPath)

        env.execute()

        "80"

      case _ =>
        throw new IllegalArgumentException("Invalid program id")
    }
  }

  protected override def postSubmit(): Unit = {
    compareResultsByLinesInMemory(expectedResult, resultPath)
  }
}

object ScalaSpecialTypesITCase {
  var NUM_PROGRAMS: Int = 6

  @Parameters
  def getConfigurations: java.util.Collection[Array[AnyRef]] = {
    val configs = mutable.MutableList[Array[AnyRef]]()
    for (i <- 1 to ScalaSpecialTypesITCase.NUM_PROGRAMS) {
      val config = new Configuration()
      config.setInteger("ProgramId", i)
      configs += Array(config)
    }

    configs.asJavaCollection
  }
}

