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

package org.apache.flink.table.plan.util

import org.apache.flink.streaming.api.transformations.{PartitionTransformation, StreamTransformation}
import org.apache.flink.table.api.TableException
import org.apache.flink.table.connector.DefinedDistribution
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.runtime.BinaryHashPartitioner
import org.apache.flink.table.sinks.TableSink
import org.apache.flink.table.typeutils.BaseRowTypeInfo

object SinkUtil {

  def keyPartition(
      input: StreamTransformation[BaseRow],
      typeInfo: BaseRowTypeInfo,
      keys: Array[Int]): StreamTransformation[BaseRow] = {

    val baseRowTypeInfo = typeInfo.asInstanceOf[BaseRowTypeInfo]
    val partitioner = new BinaryHashPartitioner(baseRowTypeInfo, keys)
    val transformation = new PartitionTransformation(input, partitioner)
    transformation.setOutputType(baseRowTypeInfo)
    transformation
  }

  def createPartitionTransformation(
      sink: TableSink[_],
      input: StreamTransformation[BaseRow]): StreamTransformation[BaseRow] = {
    sink match {
      case par: DefinedDistribution =>
        val pk = par.getPartitionField()
        if (pk != null) {
          val pkIndex = sink.getFieldNames.indexOf(pk)
          if (pkIndex < 0) {
            throw new TableException("partitionBy field must be in the schema")
          } else {
            keyPartition(
              input, input.getOutputType.asInstanceOf[BaseRowTypeInfo], Array(pkIndex))
          }
        } else {
          input
        }
      case _ => input
    }
  }
}
