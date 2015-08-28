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
package org.apache.flink.api.scala.operators.translation

import org.apache.flink.api.common.operators.base.GroupReduceOperatorBase
import org.apache.flink.api.java.io.DiscardingOutputFormat
import org.junit.Assert
import org.junit.Test

import org.apache.flink.api.scala._

class DistinctTranslationTest {
  @Test
  def testCombinable(): Unit = {
    try {
      val env = ExecutionEnvironment.getExecutionEnvironment
      val input = env.fromElements("1", "2", "1", "3")

      val op = input.distinct { x => x}
      op.output(new DiscardingOutputFormat[String])

      val p = env.createProgramPlan()

      val reduceOp =
        p.getDataSinks.iterator.next.getInput.asInstanceOf[GroupReduceOperatorBase[_, _, _]]

      Assert.assertTrue(reduceOp.isCombinable)
    }
    catch {
      case e: Exception => {
        e.printStackTrace()
        Assert.fail(e.getMessage)
      }
    }
  }
}

