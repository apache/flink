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

import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory}
import org.apache.flink.streaming.api.datastream.DataStream

class DataStreamTable[T](
    val dataStream: DataStream[T],
    override val fieldIndexes: Array[Int],
    override val fieldNames: Array[String])
  extends FlinkTable[T](dataStream.getType, fieldIndexes, fieldNames) {

  override def getRowType(typeFactory: RelDataTypeFactory): RelDataType = {
    val builder = typeFactory.builder
    fieldNames.zip(fieldTypes)
      .foreach( f => builder.add(f._1, f._2).nullable(true) )
    builder.build
  }

}
