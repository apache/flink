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

package org.apache.flink.table.runtime.batch.sql.agg

import org.apache.flink.configuration.Configuration
import org.apache.flink.table.api.TableConfigOptions

/**
  * AggregateITCase using HashAgg Operator.
  */
class HashAggITCase
    extends AggregateITCaseBase("HashAggregate") {

  override def prepareAggOp(): Unit = {
    // for hash agg fallback test
    val configuration = new Configuration()
    configuration.setInteger(TableConfigOptions.SQL_RESOURCE_HASH_AGG_TABLE_MEM, 4)
    tEnv.getConfig.getConf.addAll(configuration)
    tEnv.getConfig.getConf.setString(
      TableConfigOptions.SQL_EXEC_DISABLED_OPERATORS, "SortAgg")
  }
}
