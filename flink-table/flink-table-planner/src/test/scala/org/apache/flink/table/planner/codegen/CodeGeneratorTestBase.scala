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
package org.apache.flink.table.planner.codegen

import org.apache.flink.configuration.Configuration
import org.apache.flink.table.api.config.TableConfigOptions
import org.apache.flink.testutils.junit.extensions.parameterized.{ParameterizedTestExtension, Parameters}

import org.junit.jupiter.api.extension.ExtendWith

import java.util

@ExtendWith(Array(classOf[ParameterizedTestExtension]))
class CodeGeneratorTestBase {}

object CodeGeneratorTestBase {
  @Parameters(name = "config={0}")
  def parameters(): util.Collection[Configuration] = {
    util.Arrays.asList(
      Configuration.fromMap(
        util.Collections
          .singletonMap(TableConfigOptions.INDEPENDENT_NAME_COUNTER_ENABLED.key(), "true")),
      Configuration.fromMap(
        util.Collections
          .singletonMap(TableConfigOptions.INDEPENDENT_NAME_COUNTER_ENABLED.key(), "false"))
    )
  }
}
