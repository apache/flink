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

package org.apache.flink.table.planner.utils

import java.lang.{Integer => JInt}
import java.sql.Timestamp
import java.util

import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.table.functions.TableAggregateFunction
import org.apache.flink.util.Collector

class Top3Accum {
  var data: util.HashMap[JInt, JInt] = _
  var size: JInt = _
  var smallest: JInt = _
}

/**
  * Test function for plan test.
  */
class EmptyTableAggFuncWithoutEmit extends TableAggregateFunction[JTuple2[JInt, JInt], Top3Accum] {

  override def createAccumulator(): Top3Accum = new Top3Accum

  def accumulate(acc: Top3Accum, category: Long, value: Timestamp): Unit = {}

  def accumulate(acc: Top3Accum, category: Long, value: Int): Unit = {}

  def accumulate(acc: Top3Accum, value: Int): Unit = {}
}

class EmptyTableAggFunc extends EmptyTableAggFuncWithoutEmit {

  def emitValue(acc: Top3Accum, out: Collector[JTuple2[JInt, JInt]]): Unit = {}
}

class EmptyTableAggFuncWithIntResultType extends TableAggregateFunction[JInt, Top3Accum] {

  override def createAccumulator(): Top3Accum = new Top3Accum

  def accumulate(acc: Top3Accum, value: Int): Unit = {}

  def emitValue(acc: Top3Accum, out: Collector[JInt]): Unit = {}
}
