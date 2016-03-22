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

package org.apache.flink.api.table.plan.schema

import java.lang.Double
import java.util
import java.util.Collections

import org.apache.calcite.rel.{RelCollation, RelDistribution}
import org.apache.calcite.schema.Statistic
import org.apache.calcite.util.ImmutableBitSet
import org.apache.flink.api.java.DataSet

class DataSetTable[T](
    val dataSet: DataSet[T],
    override val fieldIndexes: Array[Int],
    override val fieldNames: Array[String])
  extends FlinkTable[T](dataSet.getType, fieldIndexes, fieldNames) {

  override def getStatistic: Statistic = {
    new DefaultDataSetStatistic
  }

}

class DefaultDataSetStatistic extends Statistic {

  override def getRowCount: Double = 1000d

  override def getCollations: util.List[RelCollation] = Collections.emptyList()

  override def isKey(columns: ImmutableBitSet): Boolean = false

  override def getDistribution: RelDistribution = null
}
