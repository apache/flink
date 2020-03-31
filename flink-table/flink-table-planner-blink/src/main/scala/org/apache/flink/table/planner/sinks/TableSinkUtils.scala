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

package org.apache.flink.table.planner.sinks

import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.catalog.ObjectIdentifier
import org.apache.flink.table.operations.CatalogSinkModifyOperation
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter.fromDataTypeToLogicalType
import org.apache.flink.table.runtime.types.PlannerTypeUtils
import org.apache.flink.table.sinks.{PartitionableTableSink, TableSink}

import scala.collection.JavaConversions._

object TableSinkUtils {

  /**
    * Checks if the given [[CatalogSinkModifyOperation]]'s query can be written to
    * the given [[TableSink]]. It checks if the names & the field types match. If the table
    * sink is a [[PartitionableTableSink]], also check that the partitions are valid.
    *
    * @param sinkOperation The sink operation with the query that is supposed to be written.
    * @param sinkIdentifier Tha path of the sink. It is needed just for logging. It does not
    *                      participate in the validation.
    * @param sink     The sink that we want to write to.
    * @param partitionKeys The partition keys of this table.
    */
  def validateSink(
      sinkOperation: CatalogSinkModifyOperation,
      sinkIdentifier: ObjectIdentifier,
      sink: TableSink[_],
      partitionKeys: Seq[String]): Unit = {
    val query = sinkOperation.getChild
    // validate schema of source table and table sink
    val srcFieldTypes = query.getTableSchema.getFieldDataTypes
    val sinkFieldTypes = sink.getTableSchema.getFieldDataTypes

    val srcLogicalTypes = srcFieldTypes.map(t => fromDataTypeToLogicalType(t))
    val sinkLogicalTypes = sinkFieldTypes.map(t => fromDataTypeToLogicalType(t))

    if (srcLogicalTypes.length != sinkLogicalTypes.length ||
      srcLogicalTypes.zip(sinkLogicalTypes).exists {
        case (srcType, sinkType) =>
          !PlannerTypeUtils.isInteroperable(srcType, sinkType)
      }) {

      val srcFieldNames = query.getTableSchema.getFieldNames
      val sinkFieldNames = sink.getTableSchema.getFieldNames

      // format table and table sink schema strings
      val srcSchema = srcFieldNames.zip(srcLogicalTypes)
        .map { case (n, t) => s"$n: $t" }
        .mkString("[", ", ", "]")
      val sinkSchema = sinkFieldNames.zip(sinkLogicalTypes)
        .map { case (n, t) => s"$n: $t" }
        .mkString("[", ", ", "]")

      throw new ValidationException(
        s"Field types of query result and registered TableSink " +
          s"$sinkIdentifier do not match.\n" +
          s"Query result schema: $srcSchema\n" +
          s"TableSink schema:    $sinkSchema")
    }

    // check partitions are valid
    if (partitionKeys.nonEmpty) {
      sink match {
        case _: PartitionableTableSink =>
        case _ => throw new ValidationException("We need PartitionableTableSink to write data to" +
            s" partitioned table: $sinkIdentifier")
      }
    }

    val staticPartitions = sinkOperation.getStaticPartitions
    if (staticPartitions != null && !staticPartitions.isEmpty) {
      staticPartitions.map(_._1) foreach { p =>
        if (!partitionKeys.contains(p)) {
          throw new ValidationException(s"Static partition column $p should be in the partition" +
              s" fields list $partitionKeys for Table($sinkIdentifier).")
        }
      }
    }
  }
}
