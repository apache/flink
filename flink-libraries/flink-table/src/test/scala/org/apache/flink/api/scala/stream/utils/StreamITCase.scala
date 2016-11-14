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

package org.apache.flink.api.scala.stream.utils

import java.util.Collections

import org.apache.flink.api.table.Row
import org.junit.Assert._
import scala.collection.mutable
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import scala.collection.JavaConverters._

object StreamITCase {

  var testResults = mutable.MutableList.empty[String]

  def clear = {
    StreamITCase.testResults.clear()
  }

  def compareWithList(expected: java.util.List[String]): Unit = {
    Collections.sort(expected)
    assertEquals(expected.asScala, StreamITCase.testResults.sorted)
  }

  final class StringSink extends RichSinkFunction[Row]() {
    def invoke(value: Row) {
      testResults.synchronized {
        testResults += value.toString 
      }
    }
  }
}
