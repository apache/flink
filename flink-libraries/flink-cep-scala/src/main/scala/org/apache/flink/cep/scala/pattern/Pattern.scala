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

import org.apache.flink.cep
import org.apache.flink.cep.pattern.conditions.IterativeCondition
import org.apache.flink.cep.pattern.conditions.IterativeCondition.Context
import org.apache.flink.cep.pattern.{MalformedPatternException, Quantifier, Pattern => JPattern}
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Base class for a pattern definition.
  *
  * A pattern definition is used by [[org.apache.flink.cep.nfa.compiler.NFACompiler]] to create
  * a [[org.apache.flink.cep.nfa.NFA]].
  *
  * {{{
  * Pattern<T, F> pattern = Pattern.<T>begin("start")
  *   .next("middle").subtype(F.class)
  *   .followedBy("end").where(new MyCondition());
  * }}}
  *
  * @param jPattern Underlying Java API Pattern
  * @tparam T Base type of the elements appearing in the pattern
  * @tparam F Subtype of T to which the current pattern operator is constrained
  */
class Pattern[T , F <: T](jPattern: JPattern[T, F]) {

  private[flink] def wrappedPattern = jPattern

  /**
    * @return The previous pattern
    */
  def getPrevious: Option[Pattern[T, _ <: T]] = {
    wrapPattern(jPattern.getPrevious)
  }

  /**
    *
    * @return Name of the pattern operator
    */
  def getName: String = jPattern.getName

  /**
    *
    * @return Window length in which the pattern match has to occur
    */
  def getWindowTime: Option[Time] = {
    Option(jPattern.getWindowTime)
  }

  /**
    *
    * @return currently applied quantifier to this pattern
    */
  def getQuantifier: Quantifier = jPattern.getQuantifier

  def getCondition: Option[IterativeCondition[F]] = {
    Option(jPattern.getCondition)
  }

  /**
    * Adds a condition that has to be satisfied by an event
    * in order to be considered a match. If another condition has already been
    * set, the new one is going to be combined with the previous with a
    * logical {{{AND}}}. In other case, this is going to be the only
    * condition.
    *
    * @param condition The condition as an [[IterativeCondition]].
    * @return The pattern with the new condition is set.
    */
  def where(condition: IterativeCondition[F]): Pattern[T, F] = {
    jPattern.where(condition)
    this
  }

  /**
    * Adds a condition that has to be satisfied by an event
    * in order to be considered a match. If another condition has already been
    * set, the new one is going to be combined with the previous with a
    * logical {{{AND}}}. In other case, this is going to be the only
    * condition.
    *
    * @param condition The condition to be set.
    * @return The pattern with the new condition is set.
    */
  def where(condition: (F, Context[F]) => Boolean): Pattern[T, F] = {
    val condFun = new IterativeCondition[F] {
      val cleanCond = cep.scala.cleanClosure(condition)

      override def filter(value: F, ctx: Context[F]): Boolean = cleanCond(value, ctx)
    }
    where(condFun)
  }

  /**
    * Adds a condition that has to be satisfied by an event
    * in order to be considered a match. If another condition has already been
    * set, the new one is going to be combined with the previous with a
    * logical {{{AND}}}. In other case, this is going to be the only
    * condition.
    *
    * @param condition The condition to be set.
    * @return The pattern with the new condition is set.
    */
  def where(condition: F => Boolean): Pattern[T, F] = {
    val condFun = new IterativeCondition[F] {
      val cleanCond = cep.scala.cleanClosure(condition)

      override def filter(value: F, ctx: Context[F]): Boolean = cleanCond(value)
    }
    where(condFun)
  }

  /**
    * Adds a condition that has to be satisfied by an event
    * in order to be considered a match. If another condition has already been
    * set, the new one is going to be combined with the previous with a
    * logical {{{OR}}}. In other case, this is going to be the only
    * condition.
    *
    * @param condition The condition as an [[IterativeCondition]].
    * @return The pattern with the new condition is set.
    */
  def or(condition: IterativeCondition[F]): Pattern[T, F] = {
    jPattern.or(condition)
    this
  }

  /**
    * Adds a condition that has to be satisfied by an event
    * in order to be considered a match. If another condition has already been
    * set, the new one is going to be combined with the previous with a
    * logical {{{OR}}}. In other case, this is going to be the only
    * condition.
    *
    * @param condition The {{{OR}}} condition.
    * @return The pattern with the new condition is set.
    */
  def or(condition: (F, Context[F]) => Boolean): Pattern[T, F] = {
    val condFun = new IterativeCondition[F] {
      val cleanCond = cep.scala.cleanClosure(condition)

      override def filter(value: F, ctx: Context[F]): Boolean = cleanCond(value, ctx)
    }
    or(condFun)
  }

  /**
    * Applies a subtype constraint on the current pattern. This means that an event has
    * to be of the given subtype in order to be matched.
    *
    * @param clazz Class of the subtype
    * @tparam S Type of the subtype
    * @return The same pattern with the new subtype constraint
    */
  def subtype[S <: F](clazz: Class[S]): Pattern[T, S] = {
    jPattern.subtype(clazz)
    this.asInstanceOf[Pattern[T, S]]
  }

  /**
    * Defines the maximum time interval in which a matching pattern has to be completed in
    * order to be considered valid. This interval corresponds to the maximum time gap between first
    * and the last event.
    *
    * @param windowTime Time of the matching window
    * @return The same pattern operator with the new window length
    */
  def within(windowTime: Time): Pattern[T, F] = {
    jPattern.within(windowTime)
    this
  }

  /**
    * Appends a new pattern to the existing one. The new pattern enforces strict
    * temporal contiguity. This means that the whole pattern sequence matches only
    * if an event which matches this pattern directly follows the preceding matching
    * event. Thus, there cannot be any events in between two matching events.
    *
    * @param name Name of the new pattern
    * @return A new pattern which is appended to this one
    */
  def next(name: String): Pattern[T, T] = {
    Pattern[T, T](jPattern.next(name))
  }

  /**
    * Appends a new pattern to the existing one. The new pattern enforces non-strict
    * temporal contiguity. This means that a matching event of this pattern and the
    * preceding matching event might be interleaved with other events which are ignored.
    *
    * @param name Name of the new pattern
    * @return A new pattern which is appended to this one
    */
  def followedBy(name: String): Pattern[T, T] = {
    Pattern[T, T](jPattern.followedBy(name))
  }

  /**
    * Appends a new pattern to the existing one. The new pattern enforces non-strict
    * temporal contiguity. This means that a matching event of this pattern and the
    * preceding matching event might be interleaved with other events which are ignored.
    *
    * @param name Name of the new pattern
    * @return A new pattern which is appended to this one
    */
  def followedByAny(name: String): Pattern[T, T] = {
    Pattern[T, T](jPattern.followedByAny(name))
  }


  /**
    * Specifies that this pattern is optional for a final match of the pattern
    * sequence to happen.
    *
    * @return The same pattern as optional.
    * @throws MalformedPatternException if the quantifier is not applicable to this pattern.
    */
  def optional: Pattern[T, F] = {
    jPattern.optional()
    this
  }

  /**
    * Specifies that this pattern can occur {{{one or more}}} times.
    * This means at least one and at most infinite number of events can
    * be matched to this pattern.
    *
    * If this quantifier is enabled for a
    * pattern {{{A.oneOrMore().followedBy(B)}}} and a sequence of events
    * {{{A1 A2 B}}} appears, this will generate patterns:
    * {{{A1 B}}} and {{{A1 A2 B}}}. See also {{{allowCombinations()}}}.
    *
    * @return The same pattern with a [[Quantifier.ONE_OR_MORE()]] quantifier applied.
    * @throws MalformedPatternException if the quantifier is not applicable to this pattern.
    */
  def oneOrMore: Pattern[T, F] = {
    jPattern.oneOrMore()
    this
  }

  /**
    * Specifies exact number of times that this pattern should be matched.
    *
    * @param times number of times matching event must appear
    * @return The same pattern with number of times applied
    * @throws MalformedPatternException if the quantifier is not applicable to this pattern.
    */
  def times(times: Int): Pattern[T, F] = {
    jPattern.times(times)
    this
  }

  /**
    * Applicable only to [[Quantifier.ONE_OR_MORE()]] and [[Quantifier.TIMES()]] patterns,
    * this option allows more flexibility to the matching events.
    *
    * If {{{allowCombinations()}}} is not applied for a
    * pattern {{{A.oneOrMore().followedBy(B)}}} and a sequence of events
    * {{{A1 A2 B}}} appears, this will generate patterns:
    * {{{A1 B}}} and {{{A1 A2 B}}}. If this method is applied, we
    * will have {{{A1 B}}}, {{{A2 B}}} and {{{A1 A2 B}}}.
    *
    * @return The same pattern with the updated quantifier.
    * @throws MalformedPatternException if the quantifier is not applicable to this pattern.
    */
  def allowCombinations(): Pattern[T, F] = {
    jPattern.allowCombinations()
    this
  }

  /**
    * Works in conjunction with [[Pattern#oneOrMore()]] or [[Pattern#times(int)]].
    * Specifies that any not matching element breaks the loop.
    *
    * E.g. a pattern like:
    * {{{
    * Pattern.begin("start").where(_.getName().equals("c"))
    *        .followedBy("middle").where(_.getName().equals("a")).oneOrMore().consecutive()
    *        .followedBy("end1").where(_.getName().equals("b"));
    * }}}
    *
    * For a sequence: C D A1 A2 A3 D A4 B
    *
    * will generate matches: {C A1 B}, {C A1 A2 B}, {C A1 A2 A3 B}
    *
    * By default a relaxed continuity is applied.
    * @return pattern with continuity changed to strict
    */
  def consecutive(): Pattern[T, F] = {
    jPattern.consecutive()
    this
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
    * Starts a new pattern sequence. The provided name is the one of the initial pattern
    * of the new sequence. Furthermore, the base type of the event sequence is set.
    *
    * @param name The name of starting pattern of the new pattern sequence
    * @tparam X Base type of the event pattern
    * @return The first pattern of a pattern sequence
    */
  def begin[X](name: String): Pattern[X, X] = Pattern(JPattern.begin(name))

}
