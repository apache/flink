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

package org.apache.flink.table.sources.tsextractors

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.expressions.{Expression, ResolvedFieldReference}

/**
  * Extracts the timestamp of a StreamRecord into a rowtime attribute.
  *
  * Note: This extractor only works for StreamTableSources.
  */
final class StreamRecordTimestamp extends TimestampExtractor {

  /** No argument fields required. */
  override def getArgumentFields: Array[String] = Array()

  /** No validation required. */
  @throws[ValidationException]
  override def validateArgumentFields(physicalFieldTypes: Array[TypeInformation[_]]): Unit = { }

  /**
    * Returns an [[Expression]] that extracts the timestamp of a StreamRecord.
    */
  override def getExpression(fieldAccesses: Array[ResolvedFieldReference]): Expression = {
    org.apache.flink.table.expressions.StreamRecordTimestamp()
  }

  override def equals(obj: Any): Boolean = obj match {
    case _: StreamRecordTimestamp => true
    case _ => false
  }

  override def hashCode(): Int = {
    classOf[StreamRecordTimestamp].hashCode()
  }
}

object StreamRecordTimestamp {
  val INSTANCE: StreamRecordTimestamp = new StreamRecordTimestamp
}
