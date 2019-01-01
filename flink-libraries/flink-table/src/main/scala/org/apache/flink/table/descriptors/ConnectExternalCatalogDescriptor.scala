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

import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.factories.TableFactoryUtil

/**
  * Common class for external catalogs created
  * with [[TableEnvironment.connect(ExternalCatalogDescriptor)]].
  */
 class ConnectExternalCatalogDescriptor(
    private val tableEnv: TableEnvironment,
    private val catalogDescriptor: ExternalCatalogDescriptor)
  extends DescriptorBase {

  /**
    * Searches for the specified external, configures it accordingly, and registers it as
    * a catalog under the given name.
    *
    * @param name catalog name to be registered in the table environment
    */
  def registerExternalCatalog(name: String): Unit = {
    val externalCatalog = TableFactoryUtil.findAndCreateExternalCatalog(tableEnv, this)
    tableEnv.registerExternalCatalog(name, externalCatalog)
  }

  // ----------------------------------------------------------------------------------------------

  /**
    * Converts this descriptor into a set of properties.
    */
  override def toProperties: util.Map[String, String] = {
    catalogDescriptor.toProperties
  }
}
