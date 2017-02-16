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

package org.apache.flink.table.functions

import java.io.File

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.metrics.MetricGroup

/**
  * A UDFContext contains information about the context in which user-defined functions are
  * executed. The information include the metric group, the distributed cache files,
  * and the global job parameters.
  *
  * @param context the context in which rich functions are executed
  */
class UDFContext(context: RuntimeContext) {

  /**
    * Returns the metric group for this parallel subtask.
    *
    * @return The metric group for this parallel subtask.
    */
  def getMetricGroup: MetricGroup = context.getMetricGroup

  /**
    * Get the local temporary file copies of distributed cache files
    *
    * @param name distributed cache file name
    * @return The the local temporary file copies of distributed cache files
    */
  def getDistributedCacheFile(name: String): File = context.getDistributedCache.getFile(name)

  /**
    * Get the global job parameter value associated with the given key as a string.
    * which is set by ExecutionEnvironment.getConfig.setGlobalJobParameters()
    *
    * @param key          the key pointing to the associated value
    * @param defaultValue the default value which is returned in case global job parameter is null
    *                     or there is no value associated with the given key
    * @return the (default) value associated with the given key
    */
  def getJobParameter(key: String, defaultValue: String): String = {
    val conf = context.getExecutionConfig.getGlobalJobParameters
    if (conf != null && conf.toMap.containsKey(key)) {
      conf.toMap.get(key)
    } else {
      defaultValue
    }
  }
}
