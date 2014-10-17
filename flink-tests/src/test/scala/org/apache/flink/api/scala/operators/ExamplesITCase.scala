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

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration
import org.apache.flink.test.util.JavaProgramTestBase
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.apache.flink.api.scala._
import org.junit.runners.Parameterized.Parameters
import scala.collection.JavaConverters._

import scala.collection.mutable

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

object ExampleProgs {
  var NUM_PROGRAMS: Int = 4

  def runProgram(progId: Int, resultPath: String, onCollection: Boolean): String = {
    progId match {
      case 1 =>
        /*
          Test nested tuples with int offset
         */
        val env = ExecutionEnvironment.getExecutionEnvironment
        val ds = env.fromElements( (("this","is"), 1), (("this", "is"),2), (("this","hello"),3) )

        val grouped = ds.groupBy(0).reduce( { (e1, e2) => ((e1._1._1, e1._1._2), e1._2 + e2._2)})
        grouped.writeAsText(resultPath)
        env.execute()
        "((this,hello),3)\n((this,is),3)\n"

      case 2 =>
        /*
          Test nested tuples with int offset
         */
        val env = ExecutionEnvironment.getExecutionEnvironment
        val ds = env.fromElements( (("this","is"), 1), (("this", "is"),2), (("this","hello"),3) )

        val grouped = ds.groupBy("_1._1").reduce{
          (e1, e2) => ((e1._1._1, e1._1._2), e1._2 + e2._2)
        }
        grouped.writeAsText(resultPath)
        env.execute()
        "((this,is),6)\n"

      case 3 =>
        /*
          Test nested pojos
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
        grouped.writeAsText(resultPath)
        env.execute()
        "myString=two myInt=666 nested.myLong=2\nmyString=one myInt=2 nested.myLong=1\n"

      case 4 =>
        /*
          Test pojo with nested case class
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
        grouped.writeAsText(resultPath)
        env.execute()
        "myString=two myInt=666 nested.myLong=2\nmyString=one myInt=2 nested.myLong=1\n"
    }
  }
}

@RunWith(classOf[Parameterized])
class ExamplesITCase(config: Configuration) extends JavaProgramTestBase(config) {

  private var curProgId: Int = config.getInteger("ProgramId", -1)
  private var resultPath: String = null
  private var expectedResult: String = null

  protected override def preSubmit(): Unit = {
    resultPath = getTempDirPath("result")
  }

  protected def testProgram(): Unit = {
    expectedResult = ExampleProgs.runProgram(curProgId, resultPath, isCollectionExecution)
  }

  protected override def postSubmit(): Unit = {
    compareResultsByLinesInMemory(expectedResult, resultPath)
  }
}

object ExamplesITCase {
  @Parameters
  def getConfigurations: java.util.Collection[Array[AnyRef]] = {
    val configs = mutable.MutableList[Array[AnyRef]]()
    for (i <- 1 to ExampleProgs.NUM_PROGRAMS) {
      val config = new Configuration()
      config.setInteger("ProgramId", i)
      configs += Array(config)
    }

    configs.asJavaCollection
  }
}
