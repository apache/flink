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

import org.apache.flink.table.runtime.utils.TableProgramsTestBase.TableConfigMode
import org.apache.flink.test.util.MultipleProgramsTestBase.TestExecutionMode
import org.junit.runners.Parameterized

import scala.collection.JavaConversions._

/**
  * This test base provides lightweight integration tests for batch programs. However, it does
  * not test everything (e.g. combiners). Runtime operator tests should
  * use [[TableProgramsClusterTestBase]].
  *
  * It does not use the COLLECTION execution mode any longer as the CollectionEnvironment is
  * deprecated. Because of the plans of removing the legacy planner we do not refactor the class
  * too much.
  */
class TableProgramsCollectionTestBase(
    tableConfigMode: TableConfigMode)
    extends TableProgramsTestBase(TestExecutionMode.CLUSTER, tableConfigMode) {
}

object TableProgramsCollectionTestBase {

  @Parameterized.Parameters(name = "Table config = {0}")
  def parameters(): util.Collection[Array[java.lang.Object]] = {
    Seq[Array[AnyRef]](Array(TableProgramsTestBase.DEFAULT))
  }
}
