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

import java.util.{UUID, List => JList, Map => JMap}

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.cep.functions.PatternProcessFunction
import org.apache.flink.cep.{PatternFlatSelectFunction, PatternFlatTimeoutFunction, PatternSelectFunction, PatternTimeoutFunction, PatternStream => JPatternStream}
import org.apache.flink.streaming.api.scala.{asScalaStream, _}
import org.apache.flink.util.Collector

import scala.collection.Map

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
    * Applies a process function to the detected pattern sequence. For each pattern sequence the
    * provided [[PatternProcessFunction]] is called.
    *
    * @param patternProcessFunction The pattern process function which is called for each detected
    *                              pattern sequence.
    * @tparam R Type of the resulting elements
    * @return [[DataStream]] which contains the resulting elements from the pattern select function.
    */
  def process[R: TypeInformation](patternProcessFunction: PatternProcessFunction[T, R])
  : DataStream[R] = {
    asScalaStream(jPatternStream.process(patternProcessFunction, implicitly[TypeInformation[R]]))
  }

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
    * @deprecated Use the version that returns timeouted events as a side-output
    * @return Data stream of either type which contains the resulting events and resulting timeout
    *         events.
    */
  @deprecated
  def select[L: TypeInformation, R: TypeInformation](
    patternTimeoutFunction: PatternTimeoutFunction[T, L],
    patternSelectFunction: PatternSelectFunction[T, R])
  : DataStream[Either[L, R]] = {
    val outputTag = OutputTag[L](UUID.randomUUID().toString)
    val mainStream = select(outputTag, patternTimeoutFunction, patternSelectFunction)
    mainStream.connect(mainStream.getSideOutput[L](outputTag)).map(r => Right(r), l => Left(l))
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
    * You can get the stream of timeouted matches using [[DataStream.getSideOutput()]] on the
    * [[DataStream]] resulting from the windowed operation with the same [[OutputTag]].
    *
    * @param outputTag [[OutputTag]] that identifies side output with timeouted patterns
    * @param patternTimeoutFunction The pattern timeout function which is called for each partial
    *                               pattern sequence which has timed out.
    * @param patternSelectFunction  The pattern select function which is called for each detected
    *                               pattern sequence.
    * @tparam L Type of the resulting timeout event
    * @tparam R Type of the resulting event
    * @return Data stream which contains the resulting elements with the resulting timeout elements
    *         in a side output.
    */
  def select[L: TypeInformation, R: TypeInformation](
    outputTag: OutputTag[L],
    patternTimeoutFunction: PatternTimeoutFunction[T, L],
    patternSelectFunction: PatternSelectFunction[T, R])
  : DataStream[R] = {
    val cleanedSelect = cleanClosure(patternSelectFunction)
    val cleanedTimeout = cleanClosure(patternTimeoutFunction)

    asScalaStream(
      jPatternStream
      .select(outputTag, cleanedTimeout, implicitly[TypeInformation[R]], cleanedSelect)
    )
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
    * @deprecated Use the version that returns timeouted events as a side-output
    * @return Data stream of either type which contains the resulting events and the resulting
    *         timeout events wrapped in a [[Either]] type.
    */
  @deprecated
  def flatSelect[L: TypeInformation, R: TypeInformation](
    patternFlatTimeoutFunction: PatternFlatTimeoutFunction[T, L],
    patternFlatSelectFunction: PatternFlatSelectFunction[T, R])
  : DataStream[Either[L, R]] = {

    val outputTag = OutputTag[L]("dummy-timeouted")
    val mainStream = flatSelect(outputTag, patternFlatTimeoutFunction, patternFlatSelectFunction)
    mainStream.connect(mainStream.getSideOutput[L](outputTag)).map(r => Right(r), l => Left(l))
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
    * You can get the stream of timeouted matches using [[DataStream.getSideOutput()]] on the
    * [[DataStream]] resulting from the windowed operation with the same [[OutputTag]].
    *
    * @param outputTag [[OutputTag]] that identifies side output with timeouted patterns
    * @param patternFlatTimeoutFunction The pattern flat timeout function which is called for each
    *                                   partially matched pattern sequence which has timed out.
    * @param patternFlatSelectFunction  The pattern flat select function which is called for each
    *                                   detected pattern sequence.
    * @tparam L Type of the resulting timeout event
    * @tparam R Type of the resulting event
    * @return Data stream which contains the resulting elements with the resulting timeout elements
    *         in a side output.
    */
  def flatSelect[L: TypeInformation, R: TypeInformation](
    outputTag: OutputTag[L],
    patternFlatTimeoutFunction: PatternFlatTimeoutFunction[T, L],
    patternFlatSelectFunction: PatternFlatSelectFunction[T, R])
  : DataStream[R] = {

    val cleanedSelect = cleanClosure(patternFlatSelectFunction)
    val cleanedTimeout = cleanClosure(patternFlatTimeoutFunction)

    asScalaStream(
      jPatternStream.flatSelect(
        outputTag,
        cleanedTimeout,
        implicitly[TypeInformation[R]],
        cleanedSelect))
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
  def select[R: TypeInformation](patternSelectFun: Map[String, Iterable[T]] => R): DataStream[R] = {
    val cleanFun = cleanClosure(patternSelectFun)

    val patternSelectFunction: PatternSelectFunction[T, R] = new PatternSelectFunction[T, R] {

      def select(in: JMap[String, JList[T]]): R = cleanFun(mapToScala(in))
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
    * @deprecated Use the version that returns timeouted events as a side-output
    * @return Data stream of either type which contain the resulting events and resulting timeout
    *         events.
    */
  @deprecated
  def select[L: TypeInformation, R: TypeInformation](
      patternTimeoutFunction: (Map[String, Iterable[T]], Long) => L) (
      patternSelectFunction: Map[String, Iterable[T]] => R)
    : DataStream[Either[L, R]] = {

    val cleanSelectFun = cleanClosure(patternSelectFunction)
    val cleanTimeoutFun = cleanClosure(patternTimeoutFunction)

    val patternSelectFun = new PatternSelectFunction[T, R] {
      override def select(pattern: JMap[String, JList[T]]): R =
        cleanSelectFun(mapToScala(pattern))
    }
    val patternTimeoutFun = new PatternTimeoutFunction[T, L] {
      override def timeout(pattern: JMap[String, JList[T]], timeoutTimestamp: Long): L =
        cleanTimeoutFun(mapToScala(pattern), timeoutTimestamp)
    }

    select(patternTimeoutFun, patternSelectFun)
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
    * You can get the stream of timeouted matches using [[DataStream.getSideOutput()]] on the
    * [[DataStream]] resulting from the windowed operation with the same [[OutputTag]].
    *
    * @param outputTag [[OutputTag]] that identifies side output with timeouted patterns
    * @param patternTimeoutFunction The pattern timeout function which is called for each partial
    *                               pattern sequence which has timed out.
    * @param patternSelectFunction  The pattern select function which is called for each detected
    *                               pattern sequence.
    * @tparam L Type of the resulting timeout event
    * @tparam R Type of the resulting event
    * @return Data stream of either type which contain the resulting events and resulting timeout
    *         events.
    */
  def select[L: TypeInformation, R: TypeInformation](outputTag: OutputTag[L])(
    patternTimeoutFunction: (Map[String, Iterable[T]], Long) => L) (
    patternSelectFunction: Map[String, Iterable[T]] => R)
  : DataStream[R] = {

    val cleanSelectFun = cleanClosure(patternSelectFunction)
    val cleanTimeoutFun = cleanClosure(patternTimeoutFunction)

    val patternSelectFun = new PatternSelectFunction[T, R] {
      override def select(pattern: JMap[String, JList[T]]): R =
        cleanSelectFun(mapToScala(pattern))
    }
    val patternTimeoutFun = new PatternTimeoutFunction[T, L] {
      override def timeout(pattern: JMap[String, JList[T]], timeoutTimestamp: Long): L =
        cleanTimeoutFun(mapToScala(pattern), timeoutTimestamp)
    }

    select(outputTag, patternTimeoutFun, patternSelectFun)
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
  def flatSelect[R: TypeInformation](patternFlatSelectFun: (Map[String, Iterable[T]],
    Collector[R]) => Unit): DataStream[R] = {
    val cleanFun = cleanClosure(patternFlatSelectFun)

    val patternFlatSelectFunction: PatternFlatSelectFunction[T, R] =
      new PatternFlatSelectFunction[T, R] {

        def flatSelect(pattern: JMap[String, JList[T]], out: Collector[R]): Unit =
          cleanFun(mapToScala(pattern), out)
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
    * @deprecated Use the version that returns timeouted events as a side-output
    * @return Data stream of either type which contains the resulting events and the resulting
    *         timeout events wrapped in a [[Either]] type.
    */
  @deprecated
  def flatSelect[L: TypeInformation, R: TypeInformation](
      patternFlatTimeoutFunction: (Map[String, Iterable[T]], Long, Collector[L]) => Unit) (
      patternFlatSelectFunction: (Map[String, Iterable[T]], Collector[R]) => Unit)
    : DataStream[Either[L, R]] = {

    val cleanSelectFun = cleanClosure(patternFlatSelectFunction)
    val cleanTimeoutFun = cleanClosure(patternFlatTimeoutFunction)

    val patternFlatSelectFun = new PatternFlatSelectFunction[T, R] {
      override def flatSelect(pattern: JMap[String, JList[T]], out: Collector[R]): Unit =
        cleanSelectFun(mapToScala(pattern), out)
    }

    val patternFlatTimeoutFun = new PatternFlatTimeoutFunction[T, L] {
      override def timeout(
        pattern: JMap[String, JList[T]],
        timeoutTimestamp: Long, out: Collector[L])
      : Unit = {
        cleanTimeoutFun(mapToScala(pattern), timeoutTimestamp, out)
      }
    }

    flatSelect(patternFlatTimeoutFun, patternFlatSelectFun)
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
    * You can get the stream of timeouted matches using [[DataStream.getSideOutput()]] on the
    * [[DataStream]] resulting from the windowed operation with the same [[OutputTag]].
    *
    * @param outputTag [[OutputTag]] that identifies side output with timeouted patterns
    * @param patternFlatTimeoutFunction The pattern flat timeout function which is called for each
    *                                   partially matched pattern sequence which has timed out.
    * @param patternFlatSelectFunction  The pattern flat select function which is called for each
    *                                   detected pattern sequence.
    * @tparam L Type of the resulting timeout event
    * @tparam R Type of the resulting event
    * @return Data stream of either type which contains the resulting events and the resulting
    *         timeout events wrapped in a [[Either]] type.
    */
  def flatSelect[L: TypeInformation, R: TypeInformation](outputTag: OutputTag[L])(
    patternFlatTimeoutFunction: (Map[String, Iterable[T]], Long, Collector[L]) => Unit) (
    patternFlatSelectFunction: (Map[String, Iterable[T]], Collector[R]) => Unit)
  : DataStream[R] = {

    val cleanSelectFun = cleanClosure(patternFlatSelectFunction)
    val cleanTimeoutFun = cleanClosure(patternFlatTimeoutFunction)

    val patternFlatSelectFun = new PatternFlatSelectFunction[T, R] {
      override def flatSelect(pattern: JMap[String, JList[T]], out: Collector[R]): Unit =
        cleanSelectFun(mapToScala(pattern), out)
    }

    val patternFlatTimeoutFun = new PatternFlatTimeoutFunction[T, L] {
      override def timeout(
        pattern: JMap[String, JList[T]],
        timeoutTimestamp: Long, out: Collector[L])
      : Unit = {
        cleanTimeoutFun(mapToScala(pattern), timeoutTimestamp, out)
      }
    }

    flatSelect(outputTag, patternFlatTimeoutFun, patternFlatSelectFun)
  }

 def sideOutputLateData(lateDataOutputTag: OutputTag[T]): PatternStream[T] = {
   jPatternStream.sideOutputLateData(lateDataOutputTag)
   this
 }
}

object PatternStream {
  /**
    *
    * @param jPatternStream Underlying pattern stream from Java API
    * @tparam T Type of the events
    * @return A new pattern stream wrapping the pattern stream from Java APU
    */
  def apply[T](jPatternStream: JPatternStream[T]): PatternStream[T] = {
    new PatternStream[T](jPatternStream)
  }
}
