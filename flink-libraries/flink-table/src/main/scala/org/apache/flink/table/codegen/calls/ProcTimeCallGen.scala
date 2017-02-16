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
class ProcTimeCallGen()
  extends CallGenerator {

  override def generate(
      codeGenerator: CodeGenerator,
      operands: Seq[GeneratedExpression])
    : GeneratedExpression = {
<<<<<<< Upstream, based on upstream/master
<<<<<<< Upstream, based on upstream/master
      val time = codeGenerator.addReusableEpochTimestamp()
=======
      val time = codeGenerator.addReusableTimestamp()
>>>>>>> 31060e4 Changed ProcTime from time to timestamp
=======
      val time = codeGenerator.addReusableEpochTimestamp()
>>>>>>> 1b90ac9 Refactor part of the code, moved test from IT to TemporalType test, changed ProcTimeCallGen (to be verified)
      codeGenerator.generateNonNullLiteral(SqlTimeTypeInfo.TIMESTAMP, time)
  }
  

}
