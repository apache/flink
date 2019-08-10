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

package org.apache.flink.table.planner.codegen.agg.batch

import org.apache.flink.runtime.execution.Environment
import org.apache.flink.runtime.jobgraph.OperatorID
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.streaming.runtime.tasks.{OneInputStreamTask, OneInputStreamTaskTestHarness}
import org.apache.flink.table.dataformat.{BaseRow, BinaryString, GenericRow}
import org.apache.flink.table.planner.codegen.agg.AggTestBase
import org.apache.flink.table.planner.utils.BaseRowTestUtil
import org.apache.flink.table.runtime.operators.CodeGenOperatorFactory
import org.apache.flink.table.runtime.typeutils.BaseRowTypeInfo
import org.apache.flink.table.types.logical.{BigIntType, DoubleType, LogicalType, RowType, VarCharType}
import org.junit.Assert
import java.util
import java.util.function

import scala.collection.JavaConverters._

/**
  * Base agg test.
  */
abstract class BatchAggTestBase extends AggTestBase(isBatchMode = true) {

  val globalOutputType = RowType.of(
    Array[LogicalType](
      new VarCharType(VarCharType.MAX_LENGTH), new VarCharType(VarCharType.MAX_LENGTH),
      new BigIntType(),
      new DoubleType(),
      new BigIntType()),
    Array(
      "f0", "f4",
      "agg1Output",
      "agg2Output",
      "agg3Output"))

  def row(args: Any*): GenericRow = {
    GenericRow.of(args.map {
      case str: String => BinaryString.fromString(str)
      case l: Long => Long.box(l)
      case d: Double => Double.box(d)
      case o: AnyRef => o
    }.toArray[AnyRef]: _*)
  }

  def testOperator(
      args: (CodeGenOperatorFactory[BaseRow], RowType, RowType),
      input: Array[BaseRow], expectedOutput: Array[GenericRow]): Unit = {
    val testHarness = new OneInputStreamTaskTestHarness[BaseRow, BaseRow](
      new function.Function[Environment, OneInputStreamTask[BaseRow, BaseRow]] {
        override def apply(t: Environment) = new OneInputStreamTask(t)
      }, 1, 1, BaseRowTypeInfo.of(args._2), BaseRowTypeInfo.of(args._3))
    testHarness.memorySize = 32 * 100 * 1024

    testHarness.setupOutputForSingletonOperatorChain()
    val streamConfig = testHarness.getStreamConfig
    streamConfig.setStreamOperatorFactory(args._1)
    streamConfig.setOperatorID(new OperatorID)

    testHarness.invoke()
    testHarness.waitForTaskRunning()

    for (row <- input) {
      testHarness.processElement(new StreamRecord[BaseRow](row, 0L))
    }

    testHarness.waitForInputProcessing()

    testHarness.endInput()
    testHarness.waitForTaskCompletion()

    val outputs = new util.ArrayList[GenericRow]()
    val outQueue = testHarness.getOutput
    while (!outQueue.isEmpty) {
      outputs.add(BaseRowTestUtil.toGenericRowDeeply(
        outQueue.poll().asInstanceOf[StreamRecord[BaseRow]].getValue, args._3.getChildren))
    }
    Assert.assertArrayEquals(expectedOutput.toArray[AnyRef], outputs.asScala.toArray[AnyRef])
  }
}
