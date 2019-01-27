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
package org.apache.flink.table.plan.util

sealed trait RankRange extends Serializable {
  def toString(inputFieldNames: Seq[String]): String
}

/** [[ConstantRankRangeWithoutEnd]] is a RankRange which not specify RankEnd. */
case class ConstantRankRangeWithoutEnd(rankStart: Long) extends RankRange {
  override def toString(inputFieldNames: Seq[String]): String = this.toString

  override def toString: String = s"rankStart=$rankStart"
}

/** rankStart and rankEnd are inclusive, rankStart always start from one. */
case class ConstantRankRange(rankStart: Long, rankEnd: Long) extends RankRange {

  override def toString(inputFieldNames: Seq[String]): String = this.toString

  override def toString: String = s"rankStart=$rankStart, rankEnd=$rankEnd"
}

/** changing rank limit depends on input */
case class VariableRankRange(rankEndIndex: Int) extends RankRange {
  override def toString(inputFieldNames: Seq[String]): String = {
    s"rankEnd=${inputFieldNames(rankEndIndex)}"
  }

  override def toString: String = {
    s"rankEnd=$$$rankEndIndex"
  }
}
