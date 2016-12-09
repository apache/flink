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

package org.apache.flink.api.scala.batch.utils


object LogicalPlanFormatUtils {
  private val tempPartern = """TMP_\d+""".r

  def formatTempTableId(preStr: String): String = {
    val minId = getMinTempTableId(preStr)
    tempPartern.replaceAllIn(preStr, s => "TMP_" + (s.matched.substring(4).toInt - minId) )
  }

  private def getMinTempTableId(logicalStr: String): Long = {
    tempPartern.findAllIn(logicalStr).map(s => {
      s.substring(4).toInt
    }).min
  }

  def main(args: Array[String]): Unit = {
    val str = "Project(ListBuffer(('TMP_4 as 'TMP_6 + 2) as '_c0, " +
        "('TMP_5 as 'TMP_7 + 5) as '_c1),Aggregate(List(),List(avg(('_1 + 2)) as 'TMP_4, count('_2) as 'TMP_5)," +
        "CatalogNode(_DataSetTable_0,RecordType(FLOAT _1, VARCHAR(2147483647) _2))))"
    println(str)
    print(formatTempTableId(str))
  }
}
