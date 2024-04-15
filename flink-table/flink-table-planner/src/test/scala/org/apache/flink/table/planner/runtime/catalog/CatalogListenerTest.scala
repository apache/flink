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
package org.apache.flink.table.planner.runtime.catalog

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.bridge.scala.internal.StreamTableEnvironmentImpl
import org.apache.flink.table.api.config.TableConfigOptions
import org.apache.flink.table.catalog.listener.{CatalogFactory1, CatalogFactory2}

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

import java.util

/** Catalog listener tests for environment. */
class CatalogListenerTest {
  @Test
  def testFindCatalogListenerFromTableConfig(): Unit = {
    val configuration = new Configuration()
    val tEnv1 = StreamTableEnvironment
      .create(
        StreamExecutionEnvironment.getExecutionEnvironment,
        EnvironmentSettings
          .newInstance()
          .withConfiguration(configuration)
          .build())
      .asInstanceOf[StreamTableEnvironmentImpl]
    assertThat(tEnv1.getCatalogManager.getCatalogModificationListeners.isEmpty).isTrue

    configuration.set(
      TableConfigOptions.TABLE_CATALOG_MODIFICATION_LISTENERS,
      util.Arrays.asList(CatalogFactory1.IDENTIFIER, CatalogFactory2.IDENTIFIER))
    val tEnv2 = StreamTableEnvironment
      .create(
        StreamExecutionEnvironment.getExecutionEnvironment,
        EnvironmentSettings
          .newInstance()
          .withConfiguration(configuration)
          .build())
      .asInstanceOf[StreamTableEnvironmentImpl]
    assertThat(tEnv2.getCatalogManager.getCatalogModificationListeners.size()).isEqualTo(2)
  }
}
