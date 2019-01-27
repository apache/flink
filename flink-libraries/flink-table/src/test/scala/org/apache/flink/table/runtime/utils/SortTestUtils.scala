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

package org.apache.flink.table.runtime.utils

import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.accumulators.SerializedListAccumulator
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.table.api.{BatchTableEnvironment, TableEnvironment}
import org.apache.flink.types.Row

import scala.collection.JavaConverters._

object SortTestUtils {

  val tupleDataSetStrings = List((1, 1L, "Hi")
    ,(2, 2L, "Hello")
    ,(3, 2L, "Hello world")
    ,(4, 3L, "Hello world, how are you?")
    ,(5, 3L, "I am fine.")
    ,(6, 3L, "Luke Skywalker")
    ,(7, 4L, "Comment#1")
    ,(8, 4L, "Comment#2")
    ,(9, 4L, "Comment#3")
    ,(10, 4L, "Comment#4")
    ,(11, 5L, "Comment#5")
    ,(12, 5L, "Comment#6")
    ,(13, 5L, "Comment#7")
    ,(14, 5L, "Comment#8")
    ,(15, 5L, "Comment#9")
    ,(16, 6L, "Comment#10")
    ,(17, 6L, "Comment#11")
    ,(18, 6L, "Comment#12")
    ,(19, 6L, "Comment#13")
    ,(20, 6L, "Comment#14")
    ,(21, 6L, "Comment#15"))

  def sortExpectedly(dataSet: List[Product])
                    (implicit ordering: Ordering[Product]): String = 
    sortExpectedly(dataSet, 0, dataSet.length)

  def sortExpectedly(dataSet: List[Product], start: Int, end: Int)
                    (implicit ordering: Ordering[Product]): String = {
    dataSet
      .sorted(ordering)
      .slice(start, end)
      .mkString("\n")
      .replaceAll("[\\(\\)]", "")
  }

  def getOrderedRows(tEnv: BatchTableEnvironment,
      typeSerializer: TypeSerializer[Seq[Row]], id: String): Seq[Row] = {
    val res: JobExecutionResult = tEnv.execute()
    val accResult: _root_.java.util.ArrayList[Array[Byte]] = res.getAccumulatorResult(id)
    val results: Seq[Seq[Row]] =
      SerializedListAccumulator.deserializeList(accResult, typeSerializer).asScala
    var totalSize = 0
    results.foreach((rows : Seq[Row]) => totalSize += rows.length)
    val avgSize = totalSize / results.length
    results.foreach((rows : Seq[Row]) => rows.length >= avgSize && rows.length <= avgSize + 1)

    results
        .filterNot(_.isEmpty)
        // sort all partitions by their head element to verify the order across partitions
        .reduceLeft(_ ++ _)
  }
}
