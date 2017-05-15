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

package org.apache.flink.table.api

import _root_.java.io.Serializable
import org.apache.flink.api.common.time.Time

class QueryConfig private[table] extends Serializable {}

/**
  * The [[BatchQueryConfig]] holds parameters to configure the behavior of batch queries.
  */
class BatchQueryConfig private[table] extends QueryConfig

/**
  * The [[StreamQueryConfig]] holds parameters to configure the behavior of streaming queries.
  *
  * An empty [[StreamQueryConfig]] can be generated using the [[StreamTableEnvironment.queryConfig]]
  * method.
  */
class StreamQueryConfig private[table] extends QueryConfig {

  /**
    * The minimum time until state which was not updated will be retained.
    * State might be cleared and removed if it was not updated for the defined period of time.
    */
  private var minIdleStateRetentionTime: Long = Long.MinValue

  /**
    * The maximum time until state which was not updated will be retained.
    * State will be cleared and removed if it was not updated for the defined period of time.
    */
  private var maxIdleStateRetentionTime: Long = Long.MinValue

  /**
    * Specifies the time interval for how long idle state, i.e., state which was not updated, will
    * be retained. When state was not updated for the specified interval of time, it will be cleared
    * and removed.
    *
    * When new data arrives for previously cleaned-up state, the new data will be handled as if it
    * was the first data. This can result in previous results being overwritten.
    *
    * Note: [[withIdleStateRetentionTime(minTime: Time, maxTime: Time)]] allows to set a minimum and
    * maximum time for state to be retained. This method is more efficient, because the system has
    * to do less bookkeeping to identify the time at which state must be cleared.
    *
    * @param time The time interval for how long idle state is retained. Set to 0 (zero) to never
    *             clean-up the state.
    */
  def withIdleStateRetentionTime(time: Time): StreamQueryConfig = {
    withIdleStateRetentionTime(time, time)
  }

  /**
    * Specifies a minimum and a maximum time interval for how long idle state, i.e., state which
    * was not updated, will be retained.
    * State will never be cleared until it was idle for less than the minimum time and will never
    * be kept if it was idle for more than the maximum time.
    *
    * When new data arrives for previously cleaned-up state, the new data will be handled as if it
    * was the first data. This can result in previous results being overwritten.
    *
    * Set to 0 (zero) to never clean-up the state.
    *
    * @param minTime The minimum time interval for which idle state is retained. Set to 0 (zero) to
    *                never clean-up the state.
    * @param maxTime The maximum time interval for which idle state is retained. May not be smaller
    *                than than minTime. Set to 0 (zero) to never clean-up the state.
    */
  def withIdleStateRetentionTime(minTime: Time, maxTime: Time): StreamQueryConfig = {
    if (maxTime.toMilliseconds < minTime.toMilliseconds) {
      throw new IllegalArgumentException("maxTime may not be smaller than minTime.")
    }
    minIdleStateRetentionTime = minTime.toMilliseconds
    maxIdleStateRetentionTime = maxTime.toMilliseconds
    this
  }

  def getMinIdleStateRetentionTime: Long = {
    minIdleStateRetentionTime
  }

  def getMaxIdleStateRetentionTime: Long = {
    maxIdleStateRetentionTime
  }
}
