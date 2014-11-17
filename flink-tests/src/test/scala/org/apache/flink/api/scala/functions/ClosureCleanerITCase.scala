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
package org.apache.flink.api.scala.functions

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration
import org.apache.flink.test.util.JavaProgramTestBase
import org.junit.Assert.fail
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.runners.Parameterized.Parameters

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.flink.api.scala._
import org.apache.flink.api.common.InvalidProgramException

/* The test cases are originally from the Apache Spark project. Like the ClosureCleaner itself. */

@RunWith(classOf[Parameterized])
class ClosureCleanerITCase(config: Configuration) extends JavaProgramTestBase(config) {

  private var curProgId: Int = config.getInteger("ProgramId", -1)
  private var resultPath: String = null
  private var expectedResult: String = null

  protected override def preSubmit(): Unit = {
    resultPath = getTempDirPath("result")
  }

  protected def testProgram(): Unit = {
    expectedResult = curProgId match {
      case 1 =>
        TestObject.run(resultPath)
        "30" // 6 + 7 + 8 + 9

      case 2 =>
        val obj = new TestClass
        obj.run(resultPath)
        "30" // 6 + 7 + 8 + 9

      case 3 =>
        val obj = new TestClassWithoutDefaultConstructor(5)
        obj.run(resultPath)
        "30" // 6 + 7 + 8 + 9


      case 4 =>
        val obj = new TestClassWithoutFieldAccess
        obj.run(resultPath)
        "30" // 6 + 7 + 8 + 9

      case 5 =>
        TestObjectWithNesting.run(resultPath)
        "27"

      case 6 =>
        val obj = new TestClassWithNesting(1)
        obj.run(resultPath)
        "27"

      case 7 =>
        TestObjectWithBogusReturns.run(resultPath)
        "1"

      case 8 =>
        TestObjectWithNestedReturns.run(resultPath)
        "1"

      case _ =>
        throw new IllegalArgumentException("Invalid program id")
    }
  }

  protected override def postSubmit(): Unit = {
    compareResultsByLinesInMemory(expectedResult, resultPath)
  }
}

object ClosureCleanerITCase {

  val NUM_PROGRAMS = 6

  @Parameters
  def getConfigurations: java.util.Collection[Array[AnyRef]] = {
    val configs = mutable.MutableList[Array[AnyRef]]()
    for (i <- 1 to NUM_PROGRAMS) {
      val config = new Configuration()
      config.setInteger("ProgramId", i)
      configs += Array(config)
    }

    configs.asJavaCollection
  }
}

// A non-serializable class we create in closures to make sure that we aren't
// keeping references to unneeded variables from our outer closures.
class NonSerializable {}

object TestObject {
  def run(resultPath: String): Unit = {
    var nonSer = new NonSerializable
    val x = 5
    val env = ExecutionEnvironment.getExecutionEnvironment
    val nums = env.fromElements(1, 2, 3, 4)

    nums.map(_ + x).reduce(_ + _).writeAsText(resultPath)

    env.execute()
  }
}

class TestClass extends Serializable {
  var x = 5

  def getX = x

  def run(resultPath: String): Unit = {
    var nonSer = new NonSerializable

    val env = ExecutionEnvironment.getExecutionEnvironment
    val nums = env.fromElements(1, 2, 3, 4)

    nums.map(_ + getX).reduce(_ + _).writeAsText(resultPath)

    env.execute()
  }
}

class TestClassWithoutDefaultConstructor(x: Int) extends Serializable {
  def getX = x

  def run(resultPath: String): Unit = {
    var nonSer = new NonSerializable

    val env = ExecutionEnvironment.getExecutionEnvironment
    val nums = env.fromElements(1, 2, 3, 4)

    nums.map(_ + getX).reduce(_ + _).writeAsText(resultPath)

    env.execute()
  }
}

// This class is not serializable, but we aren't using any of its fields in our
// closures, so they won't have a $outer pointing to it and should still work.
class TestClassWithoutFieldAccess {
  var nonSer = new NonSerializable

  def run(resultPath: String): Unit = {
    var nonSer2 = new NonSerializable

    val x = 5
    val env = ExecutionEnvironment.getExecutionEnvironment
    val nums = env.fromElements(1, 2, 3, 4)

    nums.map(_ + x).reduce(_ + _).writeAsText(resultPath)

    env.execute()
  }
}

object TestObjectWithBogusReturns {
  def run(resultPath: String): Unit = {
    var nonSer2 = new NonSerializable

    val x = 5
    val env = ExecutionEnvironment.getExecutionEnvironment
    val nums = env.fromElements(1)

    // this return is invalid since it will transfer control outside the closure
    try {
      nums.map { x => return 1; x * 2}.print()
    } catch {
      case inv: InvalidProgramException => // all good
      case _ => fail("Bogus return statement not detected.")
    }

    nums.writeAsText(resultPath)

    env.execute()
  }
}

object TestObjectWithNestedReturns {
  def run(resultPath: String): Unit = {
    var nonSer2 = new NonSerializable

    val x = 5
    val env = ExecutionEnvironment.getExecutionEnvironment
    val nums = env.fromElements(1)

    nums.map { x =>
      // this return is fine since it will not transfer control outside the closure
      def foo(): Int = { return 5; 1 }
        foo()
    }

    nums.writeAsText(resultPath)

    env.execute()
  }
}

object TestObjectWithNesting {
  def run(resultPath: String): Unit = {
    var nonSer = new NonSerializable
    var answer = 0

    val env = ExecutionEnvironment.getExecutionEnvironment
    val nums = env.fromElements(1, 1)
    var y = 1

    val result = nums.iterate(4) {
      in =>
        var nonSer2 = new NonSerializable
        var x = y + 3
        in.map(_ + x + y).reduce(_ + _).withBroadcastSet(nums, "nums")
    }

    result.writeAsText(resultPath)

    env.execute()
  }
}

class TestClassWithNesting(val y: Int) extends Serializable {

  def getY = y

  def run(resultPath: String): Unit = {
    var nonSer = new NonSerializable
    var answer = 0

    val env = ExecutionEnvironment.getExecutionEnvironment
    val nums = env.fromElements(1, 1)

    val result = nums.iterate(4) {
      in =>
        var nonSer2 = new NonSerializable
        var x = y + 3
        in.map(_ + x + getY).reduce(_ + _).withBroadcastSet(nums, "nums")
    }

    result.writeAsText(resultPath)

    env.execute()
  }
}
