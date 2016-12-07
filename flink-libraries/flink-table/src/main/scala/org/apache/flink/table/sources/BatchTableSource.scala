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

package org.apache.flink.table.sources

import org.apache.flink.api.java.{ExecutionEnvironment, DataSet}

/** Defines an external batch table and provides access to its data.
  *
  * @tparam T Type of the [[DataSet]] created by this [[TableSource]].
  */
trait BatchTableSource[T] extends TableSource[T] {

  /**
    * Returns the data of the table as a [[DataSet]].
    *
    * NOTE: This method is for internal use only for defining a [[TableSource]].
    *       Do not use it in Table API programs.
    */
  def getDataSet(execEnv: ExecutionEnvironment): DataSet[T]
}
