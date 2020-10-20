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

package org.apache.flink.table.connector;

import org.apache.flink.annotation.PublicEvolving;

import java.util.Optional;

/**
 * Parallelism provider for connector providers.
 * Now only works on {@code SinkFunctionProvider} and {@code OutputFormatProvider}.
 */
@PublicEvolving
public interface ParallelismProvider {

  /**
   * Gets the parallelism for this contract instance. The parallelism denotes how many parallel
   * instances of the user source/sink will be spawned during the execution.
   *
   * @return empty if the connector does not want to provide parallelism, then the planner will
   * decide the number of parallel instances by itself.
   */
  default Optional<Integer> getParallelism() {
     return Optional.empty();
  }
}
