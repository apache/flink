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

package org.apache.flink.table.plan.stats

/**
  * column statistics
  *
  * @param ndv       number of distinct values
  * @param nullCount number of nulls
  * @param avgLen    average length of column values
  * @param maxLen    max length of column values
  * @param max       max value of column values
  * @param min       min value of column values
  */
case class ColumnStats(
    ndv: Long,
    nullCount: Long,
    avgLen: Long,
    maxLen: Long,
    max: Option[Any],
    min: Option[Any]) {

  override def toString: String = {
    val columnStatsStr = Seq(
      s"ndv=$ndv",
      s"nullCount=$nullCount",
      s"avgLen=$avgLen",
      s"maxLen=$maxLen",
      if (max.isDefined) s"max=${max.get}" else "",
      if (min.isDefined) s"min=${min.get}"  else ""
    ).filter(_.nonEmpty).mkString(", ")

    s"ColumnStats(${columnStatsStr})"
  }

}
