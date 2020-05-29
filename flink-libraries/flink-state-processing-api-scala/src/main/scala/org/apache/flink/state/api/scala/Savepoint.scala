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


package org.apache.flink.state.api.scala

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.state.api.{Savepoint => JSavepoint}

object Savepoint {
  def load(env: ExecutionEnvironment, path: String, stateBackend: StateBackend): ExistingSavepoint = {
    val existingSavepoint = JSavepoint.load(env.getJavaEnv, path, stateBackend)
    asScalaExistingSavepoint(existingSavepoint)
  }

  def create(stateBackend: StateBackend, maxParallelism: Int): NewSavepoint = {
    val savepoint = JSavepoint.create(stateBackend, maxParallelism)
    asScalaNewSavepoint(savepoint)
  }

}
