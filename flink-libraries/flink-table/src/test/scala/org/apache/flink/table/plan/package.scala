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

package org.apache.flink.table

package object plan {

  implicit class StringImprovements(s: String) {

    /** For every line(split by line-separators) in this string:
      *
      * If a line has `|`, strip a leading prefix consisting of blanks
      * or control characters followed by `|` from the line. This is same as
      * [[scala.collection.immutable.StringLike.stripMargin]].
      *
      * If a line does not have `|`, strip a leading prefix consisting of blanks
      * and remove the line-separators, the last line and this line are connected as a whole line.
      * This is different from [[scala.collection.immutable.StringLike.stripMargin]].
      */
    def stripMarginSkipLineBreak: String = stripMarginSkipLineBreak('|', " ")

    /** For every line(split by line-separators) in this string:
      *
      * If a line has `marginChar`, strip a leading prefix consisting of blanks
      * or control characters followed by `marginChar` from the line. This is same as
      * [[scala.collection.immutable.StringLike.stripMargin]].
      *
      * If a line does not have `marginChar`, strip a leading prefix consisting of blanks
      * and remove the line-separators, the last line and this line are connected as a whole line.
      * This is different from [[scala.collection.immutable.StringLike.stripMargin]].
      */
    def stripMarginSkipLineBreak(marginChar: Char, middleOfBreakLine: String): String = {
      val buf = new StringBuilder
      for (line <- s.linesWithSeparators) {
        val len = line.length
        var index = 0
        while (index < len && line.charAt(index) <= ' ') index += 1
        if (buf.nonEmpty && index < len && line.charAt(index) != marginChar) {
          // delete last char(\n or \f). refers to StringLike#linesWithSeparators to know more.
          require(buf.last == 0x0A || buf.last == 0x0C)
          buf.deleteCharAt(buf.length - 1)
          buf append middleOfBreakLine
        }

        buf append (
          if (index < len && line.charAt(index) == marginChar) {
            line.substring(index + 1)
          } else {
            line.substring(index)
          })
      }
      buf.toString
    }
  }

}
