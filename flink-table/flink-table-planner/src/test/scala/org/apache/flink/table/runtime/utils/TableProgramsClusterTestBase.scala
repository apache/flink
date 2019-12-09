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

package org.apache.flink.table.runtime.utils

import java.util

import org.apache.flink.table.runtime.batch.table.GroupWindowITCase
import org.apache.flink.table.runtime.utils.TableProgramsTestBase.TableConfigMode
import org.apache.flink.test.util.MultipleProgramsTestBase.TestExecutionMode
import org.junit.runners.Parameterized

import scala.collection.JavaConversions._

/**
  * This test base provides full cluster-like integration tests for batch programs. Only runtime
  * operator tests should use this test base as they are expensive.
  * (e.g. [[GroupWindowITCase]])
  */
class TableProgramsClusterTestBase(
    executionMode: TestExecutionMode,
    tableConfigMode: TableConfigMode)
    extends TableProgramsTestBase(executionMode, tableConfigMode) {
}

object TableProgramsClusterTestBase {

  @Parameterized.Parameters(name = "Execution mode = {0}, Table config = {1}")
  def parameters(): util.Collection[Array[java.lang.Object]] = {
    Seq[Array[AnyRef]](
      Array(TestExecutionMode.CLUSTER, TableProgramsTestBase.DEFAULT),
      Array(TestExecutionMode.CLUSTER_OBJECT_REUSE, TableProgramsTestBase.DEFAULT)
    )
  }
}
