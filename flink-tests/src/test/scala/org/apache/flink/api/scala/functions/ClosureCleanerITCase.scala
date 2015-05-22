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
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.test.util.MultipleProgramsTestBase.TestExecutionMode
import org.apache.flink.test.util.{TestBaseUtils, MultipleProgramsTestBase}
import org.junit.Assert.fail
import org.junit.{After, Before, Test, Rule}
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import org.apache.flink.api.scala._
import org.apache.flink.api.common.InvalidProgramException

/* The test cases are originally from the Apache Spark project. Like the ClosureCleaner itself. */

@RunWith(classOf[Parameterized])
class ClosureCleanerITCase(mode: TestExecutionMode) extends MultipleProgramsTestBase(mode) {

  val _tempFolder = new TemporaryFolder()
  var resultPath: String = _
  var result: String = _

  @Rule
  def tempFolder = _tempFolder

  @Before
  def before(): Unit = {
    resultPath = tempFolder.newFile().toURI.toString
  }

  @After
  def after(): Unit = {
    TestBaseUtils.compareResultsByLinesInMemory(result, resultPath)
  }

  @Test
  def testObject(): Unit = {
    TestObject.run(resultPath)
    result = "30"
  }

  @Test
  def testClass(): Unit = {
    val obj = new TestClass
    obj.run(resultPath)
    result = "30"
  }

  @Test
  def testClassWithoutDefaulConstructor(): Unit = {
    val obj = new TestClassWithoutDefaultConstructor(5)
    obj.run(resultPath)
    result = "30"
  }

  @Test
  def testClassWithoutFieldAccess(): Unit = {
    val obj = new TestClassWithoutFieldAccess
    obj.run(resultPath)
    result = "30" // 6 + 7 + 8 + 9
  }

  @Test
  def testObjectWithNesting(): Unit = {
    TestObjectWithNesting.run(resultPath)
    result = "27"
  }

  @Test
  def testClassWithNesting(): Unit = {
    val obj = new TestClassWithNesting(1)
    obj.run(resultPath)
    result = "27"
  }

  @Test
  def testObjectWithBogusReturns(): Unit = {
    TestObjectWithBogusReturns.run(resultPath)
    result = "1"
  }

  @Test
  def testObjectWithNestedReturns(): Unit = {
    TestObjectWithNestedReturns.run(resultPath)
    result = "1"
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

    nums.map(_ + x).reduce(_ + _).writeAsText(resultPath, WriteMode.OVERWRITE)

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

    nums.map(_ + getX).reduce(_ + _).writeAsText(resultPath, WriteMode.OVERWRITE)

    env.execute()
  }
}

class TestClassWithoutDefaultConstructor(x: Int) extends Serializable {
  def getX = x

  def run(resultPath: String): Unit = {
    var nonSer = new NonSerializable

    val env = ExecutionEnvironment.getExecutionEnvironment
    val nums = env.fromElements(1, 2, 3, 4)

    nums.map(_ + getX).reduce(_ + _).writeAsText(resultPath, WriteMode.OVERWRITE)

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

    nums.map(_ + x).reduce(_ + _).writeAsText(resultPath, WriteMode.OVERWRITE)

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
      case _: Throwable => fail("Bogus return statement not detected.")
    }

    nums.writeAsText(resultPath, WriteMode.OVERWRITE)

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

    nums.writeAsText(resultPath, WriteMode.OVERWRITE)

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

    result.writeAsText(resultPath, WriteMode.OVERWRITE)

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

    result.writeAsText(resultPath, WriteMode.OVERWRITE)

    env.execute()
  }
}
