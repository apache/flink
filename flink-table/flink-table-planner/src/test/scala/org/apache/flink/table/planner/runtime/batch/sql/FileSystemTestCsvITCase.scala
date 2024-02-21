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
package org.apache.flink.table.planner.runtime.batch.sql

import org.apache.flink.core.fs.Path
import org.apache.flink.testutils.TestFileSystem
import org.apache.flink.testutils.junit.extensions.parameterized.NoOpTestExtension

import org.assertj.core.api.Assertions
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.extension.ExtendWith

/** Test for file system table factory with testcsv format. */
@ExtendWith(Array(classOf[NoOpTestExtension]))
class FileSystemTestCsvITCase extends BatchFileSystemITCaseBase {

  override def formatProperties(): Array[String] = {
    super.formatProperties() ++ Seq("'format' = 'testcsv'")
  }

  override def getScheme: String = "test"

  @AfterEach
  def close(): Unit = {
    val path = new Path(resultPath)
    if (TestFileSystem.getNumberOfUnclosedOutputStream(path) != 0) {
      Assertions.fail(s"File $resultPath is not closed");
    }
  }
}
