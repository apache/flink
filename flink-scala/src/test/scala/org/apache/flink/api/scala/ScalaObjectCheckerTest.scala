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

package org.apache.flink.api.scala

import org.apache.flink.api.common.InvalidProgramException
import org.apache.flink.api.common.functions.RichMapFunction
import org.junit.Test


class ScalaObjectCheckerTest {

  @Test(expected = classOf[InvalidProgramException])
  def testAssertScalaForbidScalaObjectFunction(): Unit = {
    object AScalaObject
    ScalaObjectChecker.assertScalaForbidScalaObjectFunction(AScalaObject)
  }

  @Test
  def testAssertScalaForbidScalaObjectFunction2(): Unit = {
    class AScalaClass
    ScalaObjectChecker.assertScalaForbidScalaObjectFunction(new AScalaClass)
  }

  @Test(expected = classOf[InvalidProgramException])
  def testEnvForObject(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val src = env.fromCollection(Seq(1, 2, 3))
    object RichMapObject extends RichMapFunction[Int, Int] {
      override def map(value: Int): Int = value * 2
    }
    src.map(RichMapObject)
  }

  @Test
  def testEnvForClass(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val src = env.fromCollection(Seq(1, 2, 3))
    class RichMapClass extends RichMapFunction[Int, Int] {
      override def map(value: Int): Int = value * 2
    }
    src.map(new RichMapClass)
  }
}
