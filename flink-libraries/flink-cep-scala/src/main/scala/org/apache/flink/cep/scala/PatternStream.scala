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
import org.apache.flink.cep.{PatternFlatSelectFunction, PatternFlatTimeoutFunction, PatternSelectFunction, PatternTimeoutFunction, PatternStream => JPatternStream}
import org.apache.flink.cep.pattern.{Pattern => JPattern}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.apache.flink.types.{Either => FEither}
import org.apache.flink.api.java.tuple.{Tuple2 => FTuple2}
import java.lang.{Long => JLong}

import org.apache.flink.cep.operator.CEPOperatorUtils
import org.apache.flink.cep.scala.pattern.Pattern

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

  def getPattern: Pattern[T, T] = Pattern(jPatternStream.getPattern.asInstanceOf[JPattern[T, T]])

  def getInputStream: DataStream[T] = asScalaStream(jPatternStream.getInputStream())

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
    * Applies a select function to the detected pattern sequence. For each pattern sequence the
    * provided [[PatternSelectFunction]] is called. The pattern select function can produce
    * exactly one resulting element.
    *
    * Additionally a timeout function is applied to partial event patterns which have timed out. For
    * each partial pattern sequence the provided [[PatternTimeoutFunction]] is called. The pattern
    * timeout function has to produce exactly one resulting timeout event.
    *
    * The resulting event and the resulting timeout event are wrapped in an [[Either]] instance.
    *
    * @param patternTimeoutFunction The pattern timeout function which is called for each partial
    *                               pattern sequence which has timed out.
    * @param patternSelectFunction  The pattern select function which is called for each detected
    *                               pattern sequence.
    * @tparam L Type of the resulting timeout event
    * @tparam R Type of the resulting event
    * @return Data stream of either type which contains the resulting events and resulting timeout
    *         events.
    */
  def select[L: TypeInformation, R: TypeInformation](
    patternTimeoutFunction: PatternTimeoutFunction[T, L],
    patternSelectFunction: PatternSelectFunction[T, R])
  : DataStream[Either[L, R]] = {

    val patternStream = CEPOperatorUtils.createTimeoutPatternStream(
      jPatternStream.getInputStream(),
      jPatternStream.getPattern())

    val cleanedSelect = cleanClosure(patternSelectFunction)
    val cleanedTimeout = cleanClosure(patternTimeoutFunction)

    implicit val eitherTypeInfo = createTypeInformation[Either[L, R]]

    asScalaStream(patternStream).map[Either[L, R]] {
     input: FEither[FTuple2[JMap[String, T], JLong], JMap[String, T]] =>
       if (input.isLeft) {
         val timeout = input.left()
         val timeoutEvent = cleanedTimeout.timeout(timeout.f0, timeout.f1)
         val t = Left[L, R](timeoutEvent)
         t
       } else {
         val event = cleanedSelect.select(input.right())
         val t = Right[L, R](event)
         t
       }
    }
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
    * Applies a flat select function to the detected pattern sequence. For each pattern sequence
    * the provided [[PatternFlatSelectFunction]] is called. The pattern flat select function can
    * produce an arbitrary number of resulting elements.
    *
    * Additionally a timeout function is applied to partial event patterns which have timed out. For
    * each partial pattern sequence the provided [[PatternFlatTimeoutFunction]] is called. The
    * pattern timeout function can produce an arbitrary number of resulting timeout events.
    *
    * The resulting event and the resulting timeout event are wrapped in an [[Either]] instance.
    *
    * @param patternFlatTimeoutFunction The pattern flat timeout function which is called for each
    *                                   partially matched pattern sequence which has timed out.
    * @param patternFlatSelectFunction  The pattern flat select function which is called for each
    *                                   detected pattern sequence.
    * @tparam L Type of the resulting timeout event
    * @tparam R Type of the resulting event
    * @return Data stream of either type which contains the resulting events and the resulting
    *         timeout events wrapped in a [[Either]] type.
    */
  def flatSelect[L: TypeInformation, R: TypeInformation](
    patternFlatTimeoutFunction: PatternFlatTimeoutFunction[T, L],
    patternFlatSelectFunction: PatternFlatSelectFunction[T, R])
  : DataStream[Either[L, R]] = {
    val patternStream = CEPOperatorUtils.createTimeoutPatternStream(
      jPatternStream.getInputStream(),
      jPatternStream.getPattern()
    )

    val cleanedSelect = cleanClosure(patternFlatSelectFunction)
    val cleanedTimeout = cleanClosure(patternFlatTimeoutFunction)

    implicit val eitherTypeInfo = createTypeInformation[Either[L, R]]

    asScalaStream(patternStream).flatMap[Either[L, R]] {
      (input: FEither[FTuple2[JMap[String, T], JLong], JMap[String, T]],
        collector: Collector[Either[L, R]]) =>

        if (input.isLeft()) {
          val timeout = input.left()

          cleanedTimeout.timeout(timeout.f0, timeout.f1, new Collector[L]() {
            override def collect(record: L): Unit = collector.collect(Left(record))

            override def close(): Unit = collector.close()
          })
        } else {
          cleanedSelect.flatSelect(input.right, new Collector[R]() {
            override def collect(record: R): Unit = collector.collect(Right(record))

            override def close(): Unit = collector.close()
          })
        }
    }
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
    val cleanFun = cleanClosure(patternSelectFun)

    val patternSelectFunction: PatternSelectFunction[T, R] = new PatternSelectFunction[T, R] {

      def select(in: JMap[String, T]): R = cleanFun(in.asScala)
    }
    select(patternSelectFunction)
  }

  /**
    * Applies a select function to the detected pattern sequence. For each pattern sequence the
    * provided [[PatternSelectFunction]] is called. The pattern select function can produce
    * exactly one resulting element.
    *
    * Additionally a timeout function is applied to partial event patterns which have timed out. For
    * each partial pattern sequence the provided [[PatternTimeoutFunction]] is called. The pattern
    * timeout function has to produce exactly one resulting element.
    *
    * The resulting event and the resulting timeout event are wrapped in an [[Either]] instance.
    *
    * @param patternTimeoutFunction The pattern timeout function which is called for each partial
    *                               pattern sequence which has timed out.
    * @param patternSelectFunction  The pattern select function which is called for each detected
    *                               pattern sequence.
    * @tparam L Type of the resulting timeout event
    * @tparam R Type of the resulting event
    * @return Data stream of either type which contain the resulting events and resulting timeout
    *         events.
    */
  def select[L: TypeInformation, R: TypeInformation](
      patternTimeoutFunction: (mutable.Map[String, T], Long) => L) (
      patternSelectFunction: mutable.Map[String, T] => R)
    : DataStream[Either[L, R]] = {

    val cleanSelectFun = cleanClosure(patternSelectFunction)
    val cleanTimeoutFun = cleanClosure(patternTimeoutFunction)

    val patternSelectFun = new PatternSelectFunction[T, R] {
      override def select(pattern: JMap[String, T]): R = cleanSelectFun(pattern.asScala)
    }
    val patternTimeoutFun = new PatternTimeoutFunction[T, L] {
      override def timeout(pattern: JMap[String, T], timeoutTimestamp: Long): L = {
        cleanTimeoutFun(pattern.asScala, timeoutTimestamp)
      }
    }

    select(patternTimeoutFun, patternSelectFun)
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
    val cleanFun = cleanClosure(patternFlatSelectFun)

    val patternFlatSelectFunction: PatternFlatSelectFunction[T, R] =
      new PatternFlatSelectFunction[T, R] {

        def flatSelect(pattern: JMap[String, T], out: Collector[R]): Unit =
          cleanFun(pattern.asScala, out)
      }
    flatSelect(patternFlatSelectFunction)
  }

  /**
    * Applies a flat select function to the detected pattern sequence. For each pattern sequence
    * the provided [[PatternFlatSelectFunction]] is called. The pattern flat select function can
    * produce an arbitrary number of resulting elements.
    *
    * Additionally a timeout function is applied to partial event patterns which have timed out. For
    * each partial pattern sequence the provided [[PatternFlatTimeoutFunction]] is called. The
    * pattern timeout function can produce an arbitrary number of resulting timeout events.
    *
    * The resulting event and the resulting timeout event are wrapped in an [[Either]] instance.
    *
    * @param patternFlatTimeoutFunction The pattern flat timeout function which is called for each
    *                                   partially matched pattern sequence which has timed out.
    * @param patternFlatSelectFunction  The pattern flat select function which is called for each
    *                                   detected pattern sequence.
    * @tparam L Type of the resulting timeout event
    * @tparam R Type of the resulting event
    * @return Data stream of either type which contains the resulting events and the resulting
    *         timeout events wrapped in a [[Either]] type.
    */
  def flatSelect[L: TypeInformation, R: TypeInformation](
      patternFlatTimeoutFunction: (mutable.Map[String, T], Long, Collector[L]) => Unit) (
      patternFlatSelectFunction: (mutable.Map[String, T], Collector[R]) => Unit)
    : DataStream[Either[L, R]] = {

    val cleanSelectFun = cleanClosure(patternFlatSelectFunction)
    val cleanTimeoutFun = cleanClosure(patternFlatTimeoutFunction)

    val patternFlatSelectFun = new PatternFlatSelectFunction[T, R] {
      override def flatSelect(pattern: JMap[String, T], out: Collector[R]): Unit = {
        cleanSelectFun(pattern.asScala, out)
      }
    }

    val patternFlatTimeoutFun = new PatternFlatTimeoutFunction[T, L] {
      override def timeout(
        pattern: JMap[String, T],
        timeoutTimestamp: Long, out: Collector[L])
      : Unit = {
        cleanTimeoutFun(pattern.asScala, timeoutTimestamp, out)
      }
    }

    flatSelect(patternFlatTimeoutFun, patternFlatSelectFun)
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
