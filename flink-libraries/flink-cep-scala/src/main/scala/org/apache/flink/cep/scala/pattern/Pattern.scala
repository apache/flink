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
package org.apache.flink.cep.scala.pattern

import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.cep
import org.apache.flink.cep.pattern.{Pattern => JPattern}
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Base class for a pattern definition.
  *
  * A pattern definition is used by [[org.apache.flink.cep.nfa.compiler.NFACompiler]] to create
  * a [[org.apache.flink.cep.nfa.NFA]].
  *
  * {{{
  * Pattern<T, F> pattern = Pattern.<T>begin("start")
  * .next("middle").subtype(F.class)
  * .followedBy("end").where(new MyFilterFunction());
  * }
  * }}}
  *
  * @param jPattern Underlying Java API Pattern
  * @tparam T Base type of the elements appearing in the pattern
  * @tparam F Subtype of T to which the current pattern operator is constrained
  */
class Pattern[T , F <: T](jPattern: JPattern[T, F]) {

  private[flink] def wrappedPattern = jPattern

  /**
    *
    * @return Name of the pattern operator
    */
  def getName(): String = jPattern.getName()

  /**
    *
    * @return Window length in which the pattern match has to occur
    */
  def getWindowTime(): Option[Time] = {
    Option(jPattern.getWindowTime())
  }

  /**
    *
    * @return Filter condition for an event to be matched
    */
  def getFilterFunction(): Option[FilterFunction[F]] = {
    Option(jPattern.getFilterFunction())
  }

  /**
    * Applies a subtype constraint on the current pattern operator. This means that an event has
    * to be of the given subtype in order to be matched.
    *
    * @param clazz Class of the subtype
    * @tparam S Type of the subtype
    * @return The same pattern operator with the new subtype constraint
    */
  def subtype[S <: F](clazz: Class[S]): Pattern[T, S] = {
    jPattern.subtype(clazz)
    this.asInstanceOf[Pattern[T, S]]
  }

  /**
    * Defines the maximum time interval for a matching pattern. This means that the time gap
    * between first and the last event must not be longer than the window time.
    *
    * @param windowTime Time of the matching window
    * @return The same pattern operator with the new window length
    */
  def within(windowTime: Time): Pattern[T, F] = {
    jPattern.within(windowTime)
    this
  }

  /**
    * Appends a new pattern operator to the existing one. The new pattern operator enforces strict
    * temporal contiguity. This means that the whole pattern only matches if an event which matches
    * this operator directly follows the preceding matching event. Thus, there cannot be any
    * events in between two matching events.
    *
    * @param name Name of the new pattern operator
    * @return A new pattern operator which is appended to this pattern operator
    */
  def next(name: String): Pattern[T, T] = {
    Pattern[T, T](jPattern.next(name))
  }

  /**
    * Appends a new pattern operator to the existing one. The new pattern operator enforces
    * non-strict temporal contiguity. This means that a matching event of this operator and the
    * preceding matching event might be interleaved with other events which are ignored.
    *
    * @param name Name of the new pattern operator
    * @return A new pattern operator which is appended to this pattern operator
    */
  def followedBy(name: String): FollowedByPattern[T, T] = {
    FollowedByPattern(jPattern.followedBy(name))
  }

  /**
    * Specifies a filter condition which has to be fulfilled by an event in order to be matched.
    *
    * @param filter Filter condition
    * @return The same pattern operator where the new filter condition is set
    */
  def where(filter: FilterFunction[F]): Pattern[T, F] = {
    jPattern.where(filter)
    this
  }

  /**
    * Specifies a filter condition which has to be fulfilled by an event in order to be matched.
    *
    * @param filterFun Filter condition
    * @return The same pattern operator where the new filter condition is set
    */
  def where(filterFun: F => Boolean): Pattern[T, F] = {
    val filter = new FilterFunction[F] {
      val cleanFilter = cep.scala.cleanClosure(filterFun)

      override def filter(value: F): Boolean = cleanFilter(value)
    }
    where(filter)
  }

  /**
    *
    * @return The previous pattern operator
    */
  def getPrevious(): Option[Pattern[T, _ <: T]] = {
    wrapPattern(jPattern.getPrevious())
  }

}

object Pattern {

  /**
    * Constructs a new Pattern by wrapping a given Java API Pattern
    *
    * @param jPattern Underlying Java API Pattern.
    * @tparam T Base type of the elements appearing in the pattern
    * @tparam F Subtype of T to which the current pattern operator is constrained
    * @return New wrapping Pattern object
    */
  def apply[T, F <: T](jPattern: JPattern[T, F]) = new Pattern[T, F](jPattern)

  /**
    * Starts a new pattern with the initial pattern operator whose name is provided. Furthermore,
    * the base type of the event sequence is set.
    *
    * @param name Name of the new pattern operator
    * @tparam X Base type of the event pattern
    * @return The first pattern operator of a pattern
    */
  def begin[X](name: String): Pattern[X, X] = Pattern(JPattern.begin(name))

}
