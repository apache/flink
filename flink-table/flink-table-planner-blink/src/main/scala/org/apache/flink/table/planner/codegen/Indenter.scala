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
package org.apache.flink.table.planner.codegen

class IndentStringContext(sc: StringContext) {
  def j(args: Any*): String = {
    val sb = new StringBuilder()
    for ((s, a) <- sc.parts zip args) {
      sb append s

      val ind = getindent(s)
      if (ind.nonEmpty) {
        sb append a.toString.replaceAll("\n", "\n" + ind)
      } else {
        sb append a.toString
      }
    }
    if (sc.parts.size > args.size) {
      sb append sc.parts.last
    }

    sb.toString()
  }

  // get white indent after the last new line, if any
  def getindent(str: String): String = {
    val lastnl = str.lastIndexOf("\n")
    if (lastnl == -1) ""
    else {
      val ind = str.substring(lastnl + 1)
      val trimmed = ind.trim
      if (trimmed.isEmpty || trimmed == "|") {
        ind // ind is all whitespace or pipe for use with stripMargin. Use this
      } else {
        ""
      }
    }
  }
}

object Indenter {
  implicit def toISC(sc: StringContext): IndentStringContext = new IndentStringContext(sc)
}
