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
package org.apache.flink.cep.scala

import org.apache.flink.cep.{CEP => JCEP, EventComparator}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala.DataStream

/**
 * Utility method to transform a [[DataStream]] into a [[PatternStream]] to do CEP.
 *
 * @deprecated
 *   All Flink Scala APIs are deprecated and will be removed in a future Flink major version. You
 *   can still build your application in Scala, but you should move to the Java version of either
 *   the DataStream and/or Table API.
 * @see
 *   <a href="https://s.apache.org/flip-265">FLIP-265 Deprecate and remove Scala API support</a>
 */
@deprecated(org.apache.flink.api.scala.FLIP_265_WARNING, since = "1.18.0")
object CEP {

  /**
   * Transforms a [[DataStream]] into a [[PatternStream]] in the Scala API. See
   * [[org.apache.flink.cep.CEP}]]for a more detailed description how the underlying Java API works.
   *
   * @param input
   *   DataStream containing the input events
   * @param pattern
   *   Pattern specification which shall be detected
   * @tparam T
   *   Type of the input events
   * @return
   *   Resulting pattern stream
   */
  def pattern[T](input: DataStream[T], pattern: Pattern[T, _ <: T]): PatternStream[T] = {
    wrapPatternStream(JCEP.pattern(input.javaStream, pattern.wrappedPattern))
  }

  /**
   * Transforms a [[DataStream]] into a [[PatternStream]] in the Scala API. See
   * [[org.apache.flink.cep.CEP}]]for a more detailed description how the underlying Java API works.
   *
   * @param input
   *   DataStream containing the input events
   * @param pattern
   *   Pattern specification which shall be detected
   * @param comparator
   *   Comparator to sort events with equal timestamps
   * @tparam T
   *   Type of the input events
   * @return
   *   Resulting pattern stream
   */
  def pattern[T](
      input: DataStream[T],
      pattern: Pattern[T, _ <: T],
      comparator: EventComparator[T]): PatternStream[T] = {
    wrapPatternStream(JCEP.pattern(input.javaStream, pattern.wrappedPattern, comparator))
  }
}
