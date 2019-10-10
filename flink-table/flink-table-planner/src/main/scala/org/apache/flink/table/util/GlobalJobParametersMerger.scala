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

package org.apache.flink.table.util

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.configuration.Configuration
import org.apache.flink.table.api.TableConfig

import _root_.scala.collection.JavaConversions._

/**
  * Utilities for merging the table config parameters and global job parameters.
  */
object GlobalJobParametersMerger {

  /**
    * Merge global job parameters and table config parameters,
    * and set the merged result to GlobalJobParameters.
    */
  def mergeParameters(executionConfig: ExecutionConfig, tableConfig: TableConfig): Unit = {
    if (executionConfig != null) {
      val parameters = new Configuration()
      if (tableConfig != null && tableConfig.getConfiguration != null) {
        parameters.addAll(tableConfig.getConfiguration)
      }

      if (executionConfig.getGlobalJobParameters != null) {
        executionConfig.getGlobalJobParameters.toMap.foreach {
          kv => parameters.setString(kv._1, kv._2)
        }
      }

      executionConfig.setGlobalJobParameters(parameters)
    }
  }
}
