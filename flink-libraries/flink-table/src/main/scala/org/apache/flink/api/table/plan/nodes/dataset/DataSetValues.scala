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

package org.apache.flink.api.table.plan.nodes.dataset

import com.google.common.collect.ImmutableList
import org.apache.calcite.plan.{RelTraitSet, RelOptCluster}
import org.apache.calcite.rel.{RelWriter, RelNode}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.Values
import org.apache.calcite.rex.RexLiteral
import org.apache.flink.api.common.io.{GenericInputFormat, NonParallelInput}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.DataSet
import org.apache.flink.api.scala._
import org.apache.flink.api.table.typeutils.RowTypeInfo
import org.apache.flink.api.table.typeutils.TypeConverter._
import org.apache.flink.api.table.{BatchTableEnvironment, Row, TableConfig}

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

/**
  * DataSet RelNode for a LogicalValues.
  *
  */
class DataSetValues(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    rowType: RelDataType,
    tuples: ImmutableList[ImmutableList[RexLiteral]])
  extends Values(cluster, rowType, tuples, traitSet)
  with DataSetRel {

  override def deriveRowType() = rowType

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new DataSetValues(
      cluster,
      traitSet,
      rowType,
      tuples
    )
  }

  override def toString: String = {
    "Values(values: (${rowType.getFieldNames.asScala.toList.mkString(\", \")}))"
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw).item("values", valuesFieldsToString)
  }

  override def translateToPlan(
      tableEnv: BatchTableEnvironment,
      expectedType: Option[TypeInformation[Any]]): DataSet[Any] = {

    val config = tableEnv.getConfig

    val returnType = determineReturnType(
      getRowType,
      expectedType,
      config.getNullCheck,
      config.getEfficientTypeUsage).asInstanceOf[RowTypeInfo]

    val inputFormat = new ValuesInputFormat(tuples)

    tableEnv.createDataSetSource(inputFormat, returnType).asInstanceOf[DataSet[Any]]
  }

  private def valuesFieldsToString: String = {
    rowType.getFieldNames.asScala.toList.mkString(", ")
  }

}

class ValuesInputFormat(val tuples: ImmutableList[ImmutableList[RexLiteral]])
  extends GenericInputFormat[Row]
  with NonParallelInput {

  var readIdx = 0

  override def reachedEnd(): Boolean = readIdx == tuples.size()

  override def nextRecord(reuse: Row): Row = {

    if (readIdx == tuples.size()) {
      return null
    }

    val t = tuples.get(readIdx)
    readIdx += 1

    var i = 0
    for(f <- t) {
      reuse.setField(i, f.getValue)
      i += 1
    }
    reuse
  }
}


