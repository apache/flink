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

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.test.util.MultipleProgramsTestBase.TestExecutionMode
import org.apache.flink.test.util.{TestBaseUtils, MultipleProgramsTestBase}
import org.junit._
import org.junit.rules.TemporaryFolder

import org.apache.flink.api.scala._
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.util.{Failure, Success}

@RunWith(classOf[Parameterized])
class ScalaSpecialTypesITCase(mode: TestExecutionMode) extends MultipleProgramsTestBase(mode) {

  val _tempFolder = new TemporaryFolder()

  @Rule def tempFolder = _tempFolder

  @Test
  def testEither1(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val nums = env.fromElements(1, 2, 1, 2)

    val eithers = nums.map(_ match {
      case 1 => Left(10)
      case 2 => Right(20)
    })

    val resultPath = tempFolder.newFile().toURI.toString

    val result = eithers.map{
      _ match {
      case Left(i) => i
      case Right(i) => i
    }}.reduce(_ + _).writeAsText(resultPath, WriteMode.OVERWRITE)

    env.execute()

    TestBaseUtils.compareResultsByLinesInMemory("60", resultPath)
  }

  @Test
  def testEither2(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val nums = env.fromElements(1, 2, 1, 2)

    val eithers = nums.map(_ match {
      case 1 => Left(10)
      case 2 => Left(20)
    })

    val resultPath = tempFolder.newFile().toURI.toString

    val result = eithers.map(_ match {
      case Left(i) => i
    }).reduce(_ + _).writeAsText(resultPath, WriteMode.OVERWRITE)

    env.execute()

    TestBaseUtils.compareResultsByLinesInMemory("60", resultPath)
  }

  @Test
  def testEither3(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val nums = env.fromElements(1, 2, 1, 2)

    val eithers = nums.map(_ match {
      case 1 => Right(10)
      case 2 => Right(20)
    })

    val resultPath = tempFolder.newFile().toURI.toString

    val result = eithers.map(_ match {
      case Right(i) => i
    }).reduce(_ + _).writeAsText(resultPath, WriteMode.OVERWRITE)

    env.execute()

    TestBaseUtils.compareResultsByLinesInMemory("60", resultPath)
  }

  @Test
  def testTry1(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val nums = env.fromElements(1, 2, 1, 2)

    val trys = nums.map(_ match {
      case 1 => Success(10)
      case 2 => Failure(new RuntimeException("20"))
    })

    val resultPath = tempFolder.newFile().toURI.toString

    val result = trys.map{
      _ match {
        case Success(i) => i
        case Failure(t) => t.getMessage.toInt
      }}.reduce(_ + _).writeAsText(resultPath, WriteMode.OVERWRITE)

    env.execute()

    TestBaseUtils.compareResultsByLinesInMemory("60", resultPath)
  }

  @Test
  def testTry2(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val nums = env.fromElements(1, 2, 1, 2)

    val trys = nums.map(_ match {
      case 1 => Success(10)
      case 2 => Success(20)
    })

    val resultPath = tempFolder.newFile().toURI.toString

    val result = trys.map(_ match {
      case Success(i) => i
    }).reduce(_ + _).writeAsText(resultPath, WriteMode.OVERWRITE)

    env.execute()

    TestBaseUtils.compareResultsByLinesInMemory("60", resultPath)
  }

  @Test
  def testTry3(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val nums = env.fromElements(1, 2, 1, 2)

    val trys = nums.map(_ match {
      case 1 => Failure(new RuntimeException("10"))
      case 2 => Failure(new IllegalAccessError("20"))
    })

    val resultPath = tempFolder.newFile().toURI.toString

    val result = trys.map(_ match {
      case Failure(t) => t.getMessage.toInt
    }).reduce(_ + _).writeAsText(resultPath, WriteMode.OVERWRITE)

    env.execute()

    TestBaseUtils.compareResultsByLinesInMemory("60", resultPath)
  }

  @Test
  def testOption1(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val nums = env.fromElements(1, 2, 1, 2)

    val eithers = nums.map(_ match {
      case 1 => Some(10)
      case 2 => None
    })

    val resultPath = tempFolder.newFile().toURI.toString


    val result = eithers.map(_ match {
      case Some(i) => i
      case None => 20
    }).reduce(_ + _).writeAsText(resultPath, WriteMode.OVERWRITE)

    env.execute()

    TestBaseUtils.compareResultsByLinesInMemory("60", resultPath)
  }

  @Test
  def testOption2(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val nums = env.fromElements(1, 2, 1, 2)

    val eithers = nums.map(_ match {
      case 1 => Some(10)
      case 2 => Some(20)
    })

    val resultPath = tempFolder.newFile().toURI.toString

    val result = eithers.map(_ match {
      case Some(i) => i
    }).reduce(_ + _).writeAsText(resultPath, WriteMode.OVERWRITE)

    env.execute()

    TestBaseUtils.compareResultsByLinesInMemory("60", resultPath)
  }

  @Test
  def testOption3(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val nums = env.fromElements(1, 2, 1, 2)

    val eithers = nums.map(_ match {
      case 1 => None
      case 2 => None
    })

    val resultPath = tempFolder.newFile().toURI.toString

    val result = eithers.map(_ match {
      case None => 20
    }).reduce(_ + _).writeAsText(resultPath, WriteMode.OVERWRITE)

    env.execute()

    TestBaseUtils.compareResultsByLinesInMemory("80", resultPath)
  }
}

