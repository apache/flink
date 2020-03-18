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

package org.apache.flink.table.planner.runtime.utils

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
}
