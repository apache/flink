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

import org.apache.calcite.rel.RelFieldCollation
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFieldImpl}
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.core.Window.Group
import org.apache.flink.table.runtime.aggregate.AggregateUtil._
import org.apache.flink.table.functions.{ProcTimeType, RowTimeType}
import scala.collection.JavaConverters._

trait OverAggregate {

  private[flink] def partitionToString(inputType: RelDataType, partition: Array[Int]): String = {
    val inFields = inputType.getFieldNames.asScala
    partition.map( inFields(_) ).mkString(", ")
  }

  private[flink] def orderingToString(
    inputType: RelDataType,
    orderFields: java.util.List[RelFieldCollation]): String = {

    val inFields = inputType.getFieldList.asScala

    val orderingString = orderFields.asScala.map {
      x => inFields(x.getFieldIndex).getValue
    }.mkString(", ")

    orderingString
  }

  private[flink] def windowRange(overWindow: Group): String = {
    s"BETWEEN ${overWindow.lowerBound} AND ${overWindow.upperBound}"
  }

  private[flink] def aggregationToString(
    inputType: RelDataType,
    rowType: RelDataType,
    namedAggregates: Seq[CalcitePair[AggregateCall, String]]): String = {

    val inFields = inputType.getFieldList.asScala.map {
      x =>
        x.asInstanceOf[RelDataTypeFieldImpl].getType
        match {
          case proceTime: ProcTimeType => "PROCTIME"
          case rowTime: RowTimeType => "ROWTIME"
          case _ => x.asInstanceOf[RelDataTypeFieldImpl].getName
        }
    }
    val outFields = rowType.getFieldList.asScala.map {
      x =>
        x.asInstanceOf[RelDataTypeFieldImpl].getType
        match {
          case proceTime: ProcTimeType => "PROCTIME"
          case rowTime: RowTimeType => "ROWTIME"
          case _ => x.asInstanceOf[RelDataTypeFieldImpl].getName
        }
    }

    val aggStrings = namedAggregates.map(_.getKey).map(
      a => s"${a.getAggregation}(${
        if (a.getArgList.size() > 0) {
          inFields(a.getArgList.get(0))
        } else {
          "*"
        }
      })")

    (inFields ++ aggStrings).zip(outFields).map {
      case (f, o) => if (f == o) {
        f
      } else {
        s"$f AS $o"
      }
    }.mkString(", ")
  }

}
