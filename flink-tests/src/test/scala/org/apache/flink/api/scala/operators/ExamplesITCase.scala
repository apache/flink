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

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.test.util.MultipleProgramsTestBase.TestExecutionMode
import org.apache.flink.test.util.{TestBaseUtils, MultipleProgramsTestBase}

import org.junit.{Test, After, Before, Rule}
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.Parameterized


// TODO case class Tuple2[T1, T2](_1: T1, _2: T2)
// TODO case class Foo(a: Int, b: String

case class Nested(myLong: Long)

class Pojo(var myString: String, var myInt: Int, var nested: Nested) {
  def this() = {
    this("", 0, new Nested(1))
  }

  def this(myString: String, myInt: Int, myLong: Long) { this(myString, myInt, new Nested(myLong)) }

  override def toString = s"myString=$myString myInt=$myInt nested.myLong=${nested.myLong}"
}

class NestedPojo(var myLong: Long) {
  def this() { this(0) }
}

class PojoWithPojo(var myString: String, var myInt: Int, var nested: Nested) {
  def this() = {
    this("", 0, new Nested(1))
  }

  def this(myString: String, myInt: Int, myLong: Long) { this(myString, myInt, new Nested(myLong)) }

  override def toString = s"myString=$myString myInt=$myInt nested.myLong=${nested.myLong}"
}

@RunWith(classOf[Parameterized])
class ExamplesITCase(mode: TestExecutionMode) extends MultipleProgramsTestBase(mode) {
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
    TestBaseUtils.compareResultsByLinesInMemory(expected, resultPath)
  }

  @Test
  def testNestesdTuplesWithIntOffset(): Unit = {
    /*
     * Test nested tuples with int offset
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = env.fromElements( (("this","is"), 1), (("this", "is"),2), (("this","hello"),3) )

    val grouped = ds.groupBy(0).reduce( { (e1, e2) => ((e1._1._1, e1._1._2), e1._2 + e2._2)})
    grouped.writeAsText(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expected = "((this,hello),3)\n((this,is),3)\n"
  }

  @Test
  def testNestedTuplesWithExpressionKeys(): Unit = {
    /*
     * Test nested tuples with expression keys
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = env.fromElements( (("this","is"), 1), (("this", "is"),2), (("this","hello"),3) )

    val grouped = ds.groupBy("_1._1").reduce{
      (e1, e2) => ((e1._1._1, e1._1._2), e1._2 + e2._2)
    }
    grouped.writeAsText(resultPath, WriteMode.OVERWRITE)
    env.execute()
    expected = "((this,is),6)\n"
  }

  @Test
  def testNestedPojos(): Unit = {
    /*
     * Test nested pojos
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = env.fromElements(
      new PojoWithPojo("one", 1, 1L),
      new PojoWithPojo("one", 1, 1L),
      new PojoWithPojo("two", 666, 2L) )

    val grouped = ds.groupBy("nested.myLong").reduce {
      (p1, p2) =>
        p1.myInt += p2.myInt
        p1
    }
    grouped.writeAsText(resultPath, WriteMode.OVERWRITE)
    env.execute()
    expected = "myString=two myInt=666 nested.myLong=2\nmyString=one myInt=2 nested.myLong=1\n"
  }

  @Test
  def testPojoWithNestedCaseClass(): Unit = {
    /*
     * Test pojo with nested case class
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = env.fromElements(
      new Pojo("one", 1, 1L),
      new Pojo("one", 1, 1L),
      new Pojo("two", 666, 2L) )

    val grouped = ds.groupBy("nested.myLong").reduce {
      (p1, p2) =>
        p1.myInt += p2.myInt
        p1
    }
    grouped.writeAsText(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expected = "myString=two myInt=666 nested.myLong=2\nmyString=one myInt=2 nested.myLong=1\n"
  }
}
