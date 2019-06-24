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
import org.apache.flink.table.operations.{CatalogSinkModifyOperation, QueryOperation}
import java.util.{List => JList}

object TableSinkUtils {

  /**
    * Checks if the given [[QueryOperation]] can be written to the given [[TableSink]].
    * It checks if the names & the field types match.
    *
    * @param query    The query that is supposed to be written.
    * @param sinkPath Tha path of the sink. It is needed just for logging. It does not
    *                 participate in the validation.
    * @param sink     The sink that we want to write to.
    */
  def validateSink(
      query: QueryOperation,
      sinkPath: JList[String],
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
        .map { case (n, t) => s"$n: ${t.getTypeClass.getSimpleName}" }
        .mkString("[", ", ", "]")
      val sinkSchema = sinkFieldNames.zip(sinkFieldTypes)
        .map { case (n, t) => s"$n: ${t.getTypeClass.getSimpleName}" }
        .mkString("[", ", ", "]")

      throw new ValidationException(
        s"Field types of query result and registered TableSink " +
          s"${sinkPath} do not match.\n" +
          s"Query result schema: $srcSchema\n" +
          s"TableSink schema:    $sinkSchema")
    }
  }
}
