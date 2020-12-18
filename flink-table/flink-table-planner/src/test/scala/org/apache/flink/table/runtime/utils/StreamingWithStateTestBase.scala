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

import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.table.utils.LegacyRowResource
import org.apache.flink.test.util.AbstractTestBase

import org.junit.Rule
import org.junit.rules.{ExpectedException, TemporaryFolder}

class StreamingWithStateTestBase extends AbstractTestBase {

  @Rule
  def usesLegacyRows: LegacyRowResource = LegacyRowResource.INSTANCE

  val expectedException = ExpectedException.none()

  @Rule
  def thrown = expectedException

  val _tempFolder = new TemporaryFolder

  @Rule
  def tempFolder: TemporaryFolder = _tempFolder

  def getStateBackend: StateBackend = {
    val dbPath = tempFolder.newFolder().getAbsolutePath
    val checkpointPath = tempFolder.newFolder().toURI.toString
    val backend = new RocksDBStateBackend(checkpointPath)
    backend.setDbStoragePath(dbPath)
    backend
  }
}
