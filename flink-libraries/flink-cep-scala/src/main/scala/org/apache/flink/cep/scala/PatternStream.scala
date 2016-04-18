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

import java.util.{Map => JMap}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.cep.{PatternFlatSelectFunction, PatternSelectFunction,
PatternStream => JPatternStream}
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala.asScalaStream
import org.apache.flink.util.Collector
import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * Stream abstraction for CEP pattern detection. A pattern stream is a stream which emits detected
  * pattern sequences as a map of events associated with their names. The pattern is detected using
  * a [[org.apache.flink.cep.nfa.NFA]]. In order to process the detected sequences, the user has to
  * specify a [[PatternSelectFunction]] or a [[PatternFlatSelectFunction]].
  *
  * @param jPatternStream Underlying pattern stream from Java API
  * @tparam T Type of the events
  */
class PatternStream[T](jPatternStream: JPatternStream[T]) {

  private[flink] def wrappedPatternStream = jPatternStream

  /**
    * Applies a select function to the detected pattern sequence. For each pattern sequence the
    * provided [[PatternSelectFunction]] is called. The pattern select function can produce
    * exactly one resulting element.
    *
    * @param patternSelectFunction The pattern select function which is called for each detected
    *                              pattern sequence.
    * @tparam R Type of the resulting elements
    * @return [[DataStream]] which contains the resulting elements from the pattern select function.
    */
  def select[R: TypeInformation](patternSelectFunction: PatternSelectFunction[T, R])
  : DataStream[R] = {
    asScalaStream(jPatternStream.select(patternSelectFunction, implicitly[TypeInformation[R]]))
  }

  /**
    * Applies a flat select function to the detected pattern sequence. For each pattern sequence
    * the provided [[PatternFlatSelectFunction]] is called. The pattern flat select function can
    * produce an arbitrary number of resulting elements.
    *
    * @param patternFlatSelectFunction The pattern flat select function which is called for each
    *                                  detected pattern sequence.
    * @tparam R Type of the resulting elements
    * @return [[DataStream]] which contains the resulting elements from the pattern flat select
    *         function.
    */
  def flatSelect[R: TypeInformation](patternFlatSelectFunction: PatternFlatSelectFunction[T, R])
  : DataStream[R] = {
    asScalaStream(jPatternStream
      .flatSelect(patternFlatSelectFunction, implicitly[TypeInformation[R]]))
  }

  /**
    * Applies a select function to the detected pattern sequence. For each pattern sequence the
    * provided [[PatternSelectFunction]] is called. The pattern select function can produce exactly
    * one resulting element.
    *
    * @param patternSelectFun The pattern select function which is called for each detected
    *                         pattern sequence.
    * @tparam R Type of the resulting elements
    * @return [[DataStream]] which contains the resulting elements from the pattern select function.
    */
  def select[R: TypeInformation](patternSelectFun: mutable.Map[String, T] => R): DataStream[R] = {
    val patternSelectFunction: PatternSelectFunction[T, R] = new PatternSelectFunction[T, R] {
      val cleanFun = cleanClosure(patternSelectFun)

      def select(in: JMap[String, T]): R = cleanFun(in.asScala)
    }
    select(patternSelectFunction)
  }

  /**
    * Applies a flat select function to the detected pattern sequence. For each pattern sequence
    * the provided [[PatternFlatSelectFunction]] is called. The pattern flat select function
    * can produce an arbitrary number of resulting elements.
    *
    * @param patternFlatSelectFun The pattern flat select function which is called for each
    *                             detected pattern sequence.
    * @tparam R Type of the resulting elements
    * @return [[DataStream]] which contains the resulting elements from the pattern flat select
    *         function.
    */
  def flatSelect[R: TypeInformation](patternFlatSelectFun: (mutable.Map[String, T],
    Collector[R]) => Unit): DataStream[R] = {
    val patternFlatSelectFunction: PatternFlatSelectFunction[T, R] =
      new PatternFlatSelectFunction[T, R] {
        val cleanFun = cleanClosure(patternFlatSelectFun)

        def flatSelect(pattern: JMap[String, T], out: Collector[R]): Unit =
          cleanFun(pattern.asScala, out)
      }
    flatSelect(patternFlatSelectFunction)
  }

}

object PatternStream {
  /**
    *
    * @param jPatternStream Underlying pattern stream from Java API
    * @tparam T Type of the events
    * @return A new pattern stream wrapping the pattern stream from Java APU
    */
  def apply[T](jPatternStream: JPatternStream[T]) = {
    new PatternStream[T](jPatternStream)
  }
}
