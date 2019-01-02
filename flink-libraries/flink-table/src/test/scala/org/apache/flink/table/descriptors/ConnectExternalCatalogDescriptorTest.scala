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

package org.apache.flink.table.descriptors

import java.util

import org.apache.flink.table.factories.utils.TestExternalCatalogFactory
import org.apache.flink.table.factories.utils.TestExternalCatalogFactory.CATALOG_IS_STREAMING
import org.apache.flink.table.factories.utils.TestExternalCatalogFactory.CATALOG_TYPE_VALUE_TEST
import org.apache.flink.table.utils.TableTestBase
import org.hamcrest.CoreMatchers.{is, notNullValue}
import org.junit.Assert.assertThat
import org.junit.Test

/**
  * Tests for [[ConnectExternalCatalogDescriptor]].
  */
class ConnectExternalCatalogDescriptorTest extends TableTestBase {

  import ConnectExternalCatalogDescriptorTest._

  @Test
  def testStreamConnectExternalCatalogDescriptor(): Unit = {
    testConnectExternalCatalogDescriptor(true)
  }

  @Test
  def testBatchConnectExternalCatalogDescriptor(): Unit = {
    testConnectExternalCatalogDescriptor(false)
  }

  private def testConnectExternalCatalogDescriptor(isStreaming: Boolean): Unit = {

    val tableEnv = if (isStreaming) {
      streamTestUtil().tableEnv
    } else {
      batchTestUtil().tableEnv
    }

    val catalog = testExternalCatalog(isStreaming)
    val descriptor: ConnectExternalCatalogDescriptor = tableEnv.connect(catalog)

    // tests the catalog factory discovery and thus validates the result automatically
    descriptor.registerExternalCatalog("test")
    assertThat(tableEnv.listExternalCatalogs(), is(Array("test")))

    val tb1 = tableEnv.scan("test", "db1", "tb1")
    assertThat(tb1, is(notNullValue()))
  }
}

object ConnectExternalCatalogDescriptorTest {

  /**
    * Gets a descriptor for the external catalog produced by [[TestExternalCatalogFactory]].
    */
  private def testExternalCatalog(isStreaming: Boolean) = {
    new ExternalCatalogDescriptor(CATALOG_TYPE_VALUE_TEST, 1) {
      override protected def toCatalogProperties: util.Map[String, String] =
        java.util.Collections.singletonMap(CATALOG_IS_STREAMING, isStreaming.toString)
    }
  }
}
