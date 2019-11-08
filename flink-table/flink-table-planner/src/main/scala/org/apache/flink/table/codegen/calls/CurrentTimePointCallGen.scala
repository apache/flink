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

import org.apache.flink.api.common.typeinfo.{SqlTimeTypeInfo, TypeInformation}
import org.apache.flink.table.codegen.{CodeGenerator, GeneratedExpression}

/**
  * Generates function call to determine current time point (as date/time/timestamp) in
  * local timezone or not.
  */
class CurrentTimePointCallGen(
    targetType: TypeInformation[_],
    local: Boolean)
  extends CallGenerator {

  override def generate(
      codeGenerator: CodeGenerator,
      operands: Seq[GeneratedExpression])
    : GeneratedExpression = targetType match {
    case SqlTimeTypeInfo.TIME if local =>
      val time = codeGenerator.addReusableLocalTime()
      codeGenerator.generateTerm(targetType, time)

    case SqlTimeTypeInfo.TIMESTAMP if local =>
      val timestamp = codeGenerator.addReusableLocalTimestamp()
      codeGenerator.generateTerm(targetType, timestamp)

    case SqlTimeTypeInfo.DATE =>
      val date = codeGenerator.addReusableDate()
      codeGenerator.generateTerm(targetType, date)

    case SqlTimeTypeInfo.TIME =>
      val time = codeGenerator.addReusableTime()
      codeGenerator.generateTerm(targetType, time)

    case SqlTimeTypeInfo.TIMESTAMP =>
      val timestamp = codeGenerator.addReusableTimestamp()
      codeGenerator.generateTerm(targetType, timestamp)
  }

}
