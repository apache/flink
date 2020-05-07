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

package org.apache.flink.table.sinks

import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.catalog.ObjectIdentifier
import org.apache.flink.table.operations.QueryOperation

import java.util.{Map => JMap}

object TableSinkUtils {

  /**
    * Checks if the given [[QueryOperation]] can be written to the given [[TableSink]].
    * It checks if the names & the field types match. If this sink is a [[PartitionableTableSink]],
    * it will also validate the partitions.
    *
    * @param staticPartitions Static partitions of the sink if there exists any.
    * @param query            The query that is supposed to be written.
    * @param objectIdentifier The path of the sink. It is needed just for logging. It does not
    *                         participate in the validation.
    * @param sink             The sink that we want to write to.
    */
  def validateSink(
      staticPartitions: JMap[String, String],
      query: QueryOperation,
      objectIdentifier: ObjectIdentifier,
      sink: TableSink[_])
    : Unit = {
    // validate schema of source table and table sink
    val srcFieldTypes = query.getTableSchema.getFieldTypes
    val sinkFieldTypes = sink.getTableSchema.getFieldTypes

    if (srcFieldTypes.length != sinkFieldTypes.length ||
      srcFieldTypes.zip(sinkFieldTypes).exists { case (srcF, snkF) => srcF != snkF }) {

      val srcFieldNames = query.getTableSchema.getFieldNames
      val sinkFieldNames = sink.getTableSchema.getFieldNames

      // format table and table sink schema strings
      val srcSchema = srcFieldNames.zip(srcFieldTypes)
        .map { case (n, t) => s"$n: $t" }
        .mkString("[", ", ", "]")
      val sinkSchema = sinkFieldNames.zip(sinkFieldTypes)
        .map { case (n, t) => s"$n: $t" }
        .mkString("[", ", ", "]")

      throw new ValidationException(
        s"Field types of query result and registered TableSink " +
          s"$objectIdentifier do not match.\n" +
          s"Query result schema: $srcSchema\n" +
          s"TableSink schema:    $sinkSchema")
    }
    // check partitions are valid
    if (staticPartitions != null && !staticPartitions.isEmpty) {
      val invalidMsg = "Can't insert static partitions into a non-partitioned table sink. " +
        "A partitioned sink should implement 'PartitionableTableSink'."
      sink match {
        case _: PartitionableTableSink =>
        case _ => throw new ValidationException(invalidMsg)
      }
    }
  }
}
