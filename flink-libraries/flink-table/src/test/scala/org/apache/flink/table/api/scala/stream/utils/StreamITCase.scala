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

package org.apache.flink.table.api.scala.stream.utils

import java.util.Collections

import org.apache.flink.types.Row
import org.junit.Assert._

import scala.collection.mutable
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

object StreamITCase {

  var testResults: mutable.MutableList[String] = mutable.MutableList.empty[String]
  var retractedResults: ArrayBuffer[String] = mutable.ArrayBuffer.empty[String]

  def clear = {
    StreamITCase.testResults.clear()
    StreamITCase.retractedResults.clear()
  }

  def compareWithList(expected: java.util.List[String]): Unit = {
    Collections.sort(expected)
    assertEquals(expected.asScala, StreamITCase.testResults.sorted)
  }

  final class StringSink[T] extends RichSinkFunction[T]() {
    def invoke(value: T) {
      testResults.synchronized {
        testResults += value.toString
      }
    }
  }

  final class RetractMessagesSink extends RichSinkFunction[(Boolean, Row)]() {
    def invoke(v: (Boolean, Row)) {
      testResults.synchronized {
        testResults += (if (v._1) "+" else "-") + v._2
      }
    }
  }

  final class RetractingSink() extends RichSinkFunction[(Boolean, Row)] {
    def invoke(v: (Boolean, Row)) {
      retractedResults.synchronized {
        val value = v._2.toString
        if (v._1) {
          retractedResults += value
        } else {
          val idx = retractedResults.indexOf(value)
          if (idx >= 0) {
            retractedResults.remove(idx)
          } else {
            throw new RuntimeException("Tried to retract a value that wasn't added first. " +
              "This is probably an incorrectly implemented test. " +
              "Try to set the parallelism of the sink to 1.")
          }
        }
      }
    }
  }

}
