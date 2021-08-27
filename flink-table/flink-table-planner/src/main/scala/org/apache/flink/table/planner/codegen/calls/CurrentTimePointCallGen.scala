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

package org.apache.flink.table.planner.codegen.calls

import org.apache.flink.table.planner.codegen.GenerateUtils.generateNonNullField
import org.apache.flink.table.planner.codegen.{CodeGeneratorContext, GeneratedExpression}
import org.apache.flink.table.types.logical.LogicalType
import org.apache.flink.table.types.logical.LogicalTypeRoot.{DATE, TIMESTAMP_WITHOUT_TIME_ZONE,TIMESTAMP_WITH_LOCAL_TIME_ZONE, TIME_WITHOUT_TIME_ZONE}

/**
  * Generates function call to determine current time point (as date/time/timestamp) in
  * local timezone or not.
  */
class CurrentTimePointCallGen(local: Boolean, isStreaming: Boolean) extends CallGenerator {

  override def generate(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      returnType: LogicalType): GeneratedExpression = returnType.getTypeRoot match {
    // LOCALTIME in Streaming mode
    case TIME_WITHOUT_TIME_ZONE if local && isStreaming =>
      val time = ctx.addReusableRecordLevelLocalTime()
      generateNonNullField(returnType, time)

    // LOCALTIME in Batch mode
    case TIME_WITHOUT_TIME_ZONE if local && !isStreaming =>
      val time = ctx.addReusableQueryLevelLocalTime()
      generateNonNullField(returnType, time)

    // LOCALTIMESTAMP in Streaming mode
    case TIMESTAMP_WITHOUT_TIME_ZONE if local && isStreaming =>
      val timestamp = ctx.addReusableRecordLevelLocalDateTime()
      generateNonNullField(returnType, timestamp)

    // LOCALTIMESTAMP in Batch mode
    case TIMESTAMP_WITHOUT_TIME_ZONE if local && !isStreaming =>
      val timestamp = ctx.addReusableQueryLevelLocalDateTime()
      generateNonNullField(returnType, timestamp)

    // CURRENT_DATE in Streaming mode
    case DATE if isStreaming =>
      val date = ctx.addReusableRecordLevelCurrentDate()
      generateNonNullField(returnType, date)

    // CURRENT_DATE in Batch mode
    case DATE if !isStreaming =>
      val date = ctx.addReusableQueryLevelCurrentDate()
      generateNonNullField(returnType, date)

    // CURRENT_TIME in Streaming mode
    case TIME_WITHOUT_TIME_ZONE if isStreaming =>
      val time = ctx.addReusableRecordLevelLocalTime()
      generateNonNullField(returnType, time)

    // CURRENT_TIME in Batch mode
    case TIME_WITHOUT_TIME_ZONE if !isStreaming =>
      val time = ctx.addReusableQueryLevelLocalTime()
      generateNonNullField(returnType, time)

    // CURRENT_TIMESTAMP in Streaming mode
    case TIMESTAMP_WITH_LOCAL_TIME_ZONE if isStreaming =>
      val timestamp = ctx.addReusableRecordLevelCurrentTimestamp()
      generateNonNullField(returnType, timestamp)

    // CURRENT_TIMESTAMP in Batch mode
    case TIMESTAMP_WITH_LOCAL_TIME_ZONE if !isStreaming =>
      val timestamp = ctx.addReusableQueryLevelCurrentTimestamp()
      generateNonNullField(returnType, timestamp)
  }

}
