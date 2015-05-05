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

package org.apache.flink.api.scala

import org.apache.flink.api.java.io.DiscardingOutputFormat
import org.junit.Test
import org.apache.flink.api.common.InvalidProgramException

// Verify that the sanity checking in delta iterations works. We just
// have a dummy job that is not meant to be executed. Only verify that
// the join/coGroup inside the iteration is checked.
class DeltaIterationSanityCheckTest extends Serializable {

  @Test
  def testCorrectJoinWithSolution1(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val solutionInput = env.fromElements((1, "1"))
    val worksetInput = env.fromElements((2, "2"))

    val iteration = solutionInput.iterateDelta(worksetInput, 10, Array("_1")) { (s, ws) =>
      val result = s.join(ws).where("_1").equalTo("_1") { (l, r) => l }
      (result, ws)
    }

    iteration.output(new DiscardingOutputFormat[(Int, String)])
  }

  @Test
  def testCorrectJoinWithSolution2(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val solutionInput = env.fromElements((1, "1"))
    val worksetInput = env.fromElements((2, "2"))

    val iteration = solutionInput.iterateDelta(worksetInput, 10, Array("_1")) { (s, ws) =>
      val result = ws.join(s).where("_1").equalTo("_1") { (l, r) => l }
      (result, ws)
    }

    iteration.output(new DiscardingOutputFormat[(Int,String)])
  }

  @Test(expected = classOf[InvalidProgramException])
  def testIncorrectJoinWithSolution1(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val solutionInput = env.fromElements((1, "1"))
    val worksetInput = env.fromElements((2, "2"))

    val iteration = solutionInput.iterateDelta(worksetInput, 10, Array("_1")) { (s, ws) =>
      val result = s.join(ws).where("_2").equalTo("_2") { (l, r) => l }
      (result, ws)
    }

    iteration.output(new DiscardingOutputFormat[(Int,String)])
  }

  @Test(expected = classOf[InvalidProgramException])
  def testIncorrectJoinWithSolution2(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val solutionInput = env.fromElements((1, "1"))
    val worksetInput = env.fromElements((2, "2"))

    val iteration = solutionInput.iterateDelta(worksetInput, 10, Array("_1")) { (s, ws) =>
      val result = ws.join(s).where("_2").equalTo("_2") { (l, r) => l }
      (result, ws)
    }

    iteration.output(new DiscardingOutputFormat[(Int,String)])  
  }

  @Test(expected = classOf[InvalidProgramException])
  def testIncorrectJoinWithSolution3(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val solutionInput = env.fromElements((1, "1"))
    val worksetInput = env.fromElements((2, "2"))

    val iteration = solutionInput.iterateDelta(worksetInput, 10, Array("_2")) { (s, ws) =>
      val result = ws.join(s).where("_1").equalTo("_1") { (l, r) => l }
      (result, ws)
    }

    iteration.output(new DiscardingOutputFormat[(Int,String)])
   }

  @Test
  def testCorrectCoGroupWithSolution1(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val solutionInput = env.fromElements((1, "1"))
    val worksetInput = env.fromElements((2, "2"))

    val iteration = solutionInput.iterateDelta(worksetInput, 10, Array("_1")) { (s, ws) =>
      val result = s.coGroup(ws).where("_1").equalTo("_1") { (l, r) => l.min }
      (result, ws)
    }

    iteration.output(new DiscardingOutputFormat[(Int,String)])
  }

  @Test
  def testCorrectCoGroupWithSolution2(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val solutionInput = env.fromElements((1, "1"))
    val worksetInput = env.fromElements((2, "2"))

    val iteration = solutionInput.iterateDelta(worksetInput, 10, Array("_1")) { (s, ws) =>
      val result = ws.coGroup(s).where("_1").equalTo("_1") { (l, r) => l.min }
      (result, ws)
    }

    iteration.output(new DiscardingOutputFormat[(Int,String)])
  }

  @Test(expected = classOf[InvalidProgramException])
  def testIncorrectCoGroupWithSolution1(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val solutionInput = env.fromElements((1, "1"))
    val worksetInput = env.fromElements((2, "2"))

    val iteration = solutionInput.iterateDelta(worksetInput, 10, Array("_1")) { (s, ws) =>
      val result = s.coGroup(ws).where("_2").equalTo("_2") { (l, r) => l.min }
      (result, ws)
    }

    iteration.output(new DiscardingOutputFormat[(Int,String)])
  }

  @Test(expected = classOf[InvalidProgramException])
  def testIncorrectCoGroupWithSolution2(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val solutionInput = env.fromElements((1, "1"))
    val worksetInput = env.fromElements((2, "2"))

    val iteration = solutionInput.iterateDelta(worksetInput, 10, Array("_1")) { (s, ws) =>
      val result = ws.coGroup(s).where("_2").equalTo("_2") { (l, r) => l.min }
      (result, ws)
    }

    iteration.output(new DiscardingOutputFormat[(Int,String)])  
  }

  @Test(expected = classOf[InvalidProgramException])
  def testIncorrectCoGroupWithSolution3(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val solutionInput = env.fromElements((1, "1"))
    val worksetInput = env.fromElements((2, "2"))

    val iteration = solutionInput.iterateDelta(worksetInput, 10, Array("_2")) { (s, ws) =>
      val result = ws.coGroup(s).where("_1").equalTo("_1") { (l, r) => l.min }
      (result, ws)
    }

    iteration.output(new DiscardingOutputFormat[(Int,String)])
  }
}
