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
package org.apache.flink.table.api.scala.stream.utils

import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend

import org.junit.rules.TemporaryFolder
import org.junit.Rule

class StreamingWithStateTestBase extends StreamingMultipleProgramsTestBase {

  val _tempFolder = new TemporaryFolder

  @Rule
  def tempFolder: TemporaryFolder = _tempFolder

  def getStateBackend: RocksDBStateBackend = {
    val dbPath = tempFolder.newFolder().getAbsolutePath
    val checkpointPath = tempFolder.newFolder().toURI.toString
    val backend = new RocksDBStateBackend(checkpointPath)
    backend.setDbStoragePath(dbPath)
    backend
  }
}
