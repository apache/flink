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

package org.apache.flink.table.utils

object LogicalPlanFormatUtils {
  private val tempPattern = """TMP_\d+""".r

  def formatTempTableId(preStr: String): String = {
    val str = preStr.replaceAll("ArrayBuffer\\(", "List\\(")
    val minId = getMinTempTableId(str)
    tempPattern.replaceAllIn(str, s => "TMP_" + (s.matched.substring(4).toInt - minId) )
  }

  private def getMinTempTableId(logicalStr: String): Long = {
    val found = tempPattern.findAllIn(logicalStr).map(s => {
      s.substring(4).toInt
    })
    if (found.isEmpty) {
      0L
    } else {
      found.min
    }
  }
}
