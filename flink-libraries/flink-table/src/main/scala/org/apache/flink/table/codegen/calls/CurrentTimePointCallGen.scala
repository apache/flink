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

package org.apache.flink.table.codegen.calls

import org.apache.flink.table.api.types.{DataTypes, InternalType}
import org.apache.flink.table.codegen.CodeGenUtils._
import org.apache.flink.table.codegen.{CodeGeneratorContext, GeneratedExpression}

/**
  * Generates function call to determine current time point (as date/time/timestamp) in
  * local timezone or not.
  */
class CurrentTimePointCallGen(local: Boolean) extends CallGenerator {

  override def generate(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      returnType: InternalType,
      nullCheck: Boolean): GeneratedExpression = returnType match {
    case DataTypes.TIME if local =>
      val time = ctx.addReusableLocalTime()
      generateNonNullField(returnType, time, nullCheck)

    case DataTypes.TIMESTAMP if local =>
      val timestamp = ctx.addReusableLocalTimestamp()
      generateNonNullField(returnType, timestamp, nullCheck)

    case DataTypes.DATE =>
      val date = ctx.addReusableDate()
      generateNonNullField(returnType, date, nullCheck)

    case DataTypes.TIME =>
      val time = ctx.addReusableTime()
      generateNonNullField(returnType, time, nullCheck)

    case DataTypes.TIMESTAMP =>
      val timestamp = ctx.addReusableTimestamp()
      generateNonNullField(returnType, timestamp, nullCheck)
  }

}
