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

package org.apache.flink.table.plan.cost

import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.metadata.{ReflectiveRelMetadataProvider, RelMdSize, RelMetadataProvider, RelMetadataQuery}
import org.apache.calcite.util.BuiltInMethod

import java.lang.Double
import java.util

import scala.collection.JavaConversions._

class FlinkRelMdSize extends RelMdSize {

  override def averageColumnSizes(rel: RelNode, mq: RelMetadataQuery): util.List[Double] = {
    rel.getRowType.getFieldList.map(f => averageTypeValueSize(f.getType)).toList
  }
}

object FlinkRelMdSize {
  private val INSTANCE = new FlinkRelMdSize

  val SOURCE: RelMetadataProvider = ReflectiveRelMetadataProvider.reflectiveSource(
    INSTANCE,
    BuiltInMethod.AVERAGE_COLUMN_SIZES.method,
    BuiltInMethod.AVERAGE_ROW_SIZE.method)
}
