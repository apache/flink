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

package org.apache.flink.table.api.scala.batch.utils

import java.util

import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.api.scala.batch.utils.TableProgramsTestBase.{EFFICIENT, NO_NULL, TableConfigMode}
import org.apache.flink.test.util.MultipleProgramsTestBase
import org.apache.flink.test.util.MultipleProgramsTestBase.TestExecutionMode
import org.junit.runners.Parameterized

import scala.collection.JavaConversions._

class TableProgramsTestBase(
    mode: TestExecutionMode,
    tableConfigMode: TableConfigMode)
  extends MultipleProgramsTestBase(mode) {

  def config: TableConfig = {
    val conf = new TableConfig
    tableConfigMode match {
      case NO_NULL =>
        conf.setNullCheck(false)
      case EFFICIENT =>
        conf.setEfficientTypeUsage(true)
      case _ => // keep default
    }
    conf
  }
}

object TableProgramsTestBase {
  case class TableConfigMode(nullCheck: Boolean, efficientTypes: Boolean)

  val DEFAULT = TableConfigMode(nullCheck = true, efficientTypes = false)
  val NO_NULL = TableConfigMode(nullCheck = false, efficientTypes = false)
  val EFFICIENT = TableConfigMode(nullCheck = false, efficientTypes = true)

  @Parameterized.Parameters(name = "Execution mode = {0}, Table config = {1}")
  def parameters(): util.Collection[Array[java.lang.Object]] = {
    Seq[Array[AnyRef]](Array(TestExecutionMode.COLLECTION, DEFAULT))
  }
}
