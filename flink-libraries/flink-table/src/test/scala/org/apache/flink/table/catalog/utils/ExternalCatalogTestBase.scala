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
package org.apache.flink.table.catalog.utils

import org.apache.flink.table.utils.TableTestBase

class ExternalCatalogTestBase extends TableTestBase {
  protected val table1Path: Array[String] = Array("test", "db1", "tb1")
  protected val table1TopLevelPath: Array[String] = Array("test", "tb1")
  protected val table1ProjectedFields: Array[String] = Array("a", "b", "c")
  protected val table2Path: Array[String] = Array("test", "db2", "tb2")
  protected val table2ProjectedFields: Array[String] = Array("d", "e", "g")

  def sourceBatchTableNode(sourceTablePath: Array[String], fields: Array[String]): String = {
    s"BatchTableSourceScan(table=[[${sourceTablePath.mkString(", ")}]], " +
      s"fields=[${fields.mkString(", ")}])"
  }

  def sourceStreamTableNode(sourceTablePath: Array[String], fields: Array[String]): String = {
    s"StreamTableSourceScan(table=[[${sourceTablePath.mkString(", ")}]], " +
      s"fields=[${fields.mkString(", ")}])"
  }
}
