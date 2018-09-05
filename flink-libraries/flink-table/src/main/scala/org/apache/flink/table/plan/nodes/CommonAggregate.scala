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

package org.apache.flink.table.plan.nodes

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall
import org.apache.flink.table.calcite.FlinkRelBuilder
import FlinkRelBuilder.NamedWindowProperty
import org.apache.flink.table.runtime.aggregate.AggregateUtil._

import scala.collection.JavaConverters._

trait CommonAggregate {

  private[flink] def groupingToString(inputType: RelDataType, grouping: Array[Int]): String = {

    val inFields = inputType.getFieldNames.asScala
    grouping.map( inFields(_) ).mkString(", ")
  }

  private[flink] def aggregationToString(
      inputType: RelDataType,
      grouping: Array[Int],
      rowType: RelDataType,
      namedAggregates: Seq[CalcitePair[AggregateCall, String]],
      namedProperties: Seq[NamedWindowProperty])
    : String = {

    val inFields = inputType.getFieldNames.asScala
    val outFields = rowType.getFieldNames.asScala

    val groupStrings = grouping.map( inFields(_) )

    val aggs = namedAggregates.map(_.getKey)
    val aggStrings = aggs.map( a => s"${a.getAggregation}(${
      val prefix = if (a.isDistinct) "DISTINCT " else ""
      prefix + (if (a.getArgList.size() > 0) {
        a.getArgList.asScala.map(inFields(_)).mkString(", ")
      } else {
        "*"
      })
    })")

    val propStrings = namedProperties.map(_.property.toString)

    (groupStrings ++ aggStrings ++ propStrings).zip(outFields).map {
      case (f, o) => if (f == o) {
        f
      } else {
        s"$f AS $o"
      }
    }.mkString(", ")
  }
}
