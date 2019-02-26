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

import java.lang.{Double, Long}

/**
  * Column statistics.
  * Note we currently assume that, in Flink, the max and min of ColumnStats will be same type as
  * the Flink column type. For example, for SHORT and Long columns, the max and min of their
  * ColumnStats should be of type SHORT and LONG. This assumption might change in the future.
  *
  * @param ndv       number of distinct values, the value is null if not available.
  *                  (when it's not null, it may be an inaccurate value.)
  * @param nullCount number of nulls, the value is null if not available.
  *                  (when it's not null, it may be an inaccurate value.)
  * @param avgLen    average length of column values, the value is null if not available.
  *                  (when it's not null, it may be an inaccurate value.)
  * @param maxLen    max length of column values, the value is null if not available.
  *                  (when it's not null, it may be an inaccurate value.)
  * @param max       max value of column values, the value is null if not available.
  *                  (it must be a correct value, and the accurate max value may be
  *                  less than this value.)
  * @param min       min value of column values, the value is null if not available.
  *                  (it must be a correct value, and the accurate min value may be
  *                  greater than this value.)
  */
case class ColumnStats(
    ndv: Long,
    nullCount: Long,
    avgLen: Double,
    maxLen: Integer,
    max: Any,
    min: Any) {

  validateNumber(ndv, "ndv")
  validateNumber(nullCount, "nullCount")
  validateNumber(avgLen, "avgLen")
  validateNumber(maxLen, "maxLen")
  validateMaxMin(max, min)

  override def toString: String = {
    val columnStatsStr = Seq(
      if (ndv != null) s"ndv=$ndv" else "",
      if (nullCount != null) s"nullCount=$nullCount" else "",
      if (avgLen != null) s"avgLen=$avgLen" else "",
      if (maxLen != null) s"maxLen=$maxLen" else "",
      if (max != null) s"max=$max" else "",
      if (min != null) s"min=$min" else ""
    ).filter(_.nonEmpty).mkString(", ")

    s"ColumnStats($columnStatsStr)"
  }

  private def validateNumber(number: Number, fieldName: String): Unit = {
    require(number == null || number.doubleValue() >= 0,
      s"$fieldName should be null or be greater than or equal to 0!")
  }

  private def validateMaxMin(max: Any, min: Any): Unit = {
    require(max == null || max.isInstanceOf[Comparable[_]],
      "max should be null or be a Comparable instance!")
    require(min == null || min.isInstanceOf[Comparable[_]],
      "min should be null or be a Comparable instance!")
    require(min == null || max == null || max.getClass == min.getClass,
      "the classes of min and max should be same!")
  }

}
