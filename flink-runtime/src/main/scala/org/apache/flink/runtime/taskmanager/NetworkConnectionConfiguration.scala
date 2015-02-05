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

package org.apache.flink.runtime.taskmanager

/**
 * Network connection configuration for the [[TaskManager]].
 */
sealed trait NetworkConnectionConfiguration{
  def numBuffers: Int
  def bufferSize: Int
}

case class LocalNetworkConfiguration(numBuffers: Int,
                                     bufferSize: Int) extends NetworkConnectionConfiguration

case class ClusterNetworkConfiguration(numBuffers: Int, bufferSize: Int, numInThreads: Int,
                                       numOutThreads: Int, lowWaterMark: Int,
                                       highWaterMark: Int) extends NetworkConnectionConfiguration
