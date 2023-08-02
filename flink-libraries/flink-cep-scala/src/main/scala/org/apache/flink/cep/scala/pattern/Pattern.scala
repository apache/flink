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
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy
import org.apache.flink.cep.pattern.{MalformedPatternException, Pattern => JPattern, Quantifier}
import org.apache.flink.cep.pattern.conditions.{IterativeCondition, SimpleCondition}
import org.apache.flink.cep.pattern.conditions.IterativeCondition.{Context => JContext}
import org.apache.flink.cep.scala.conditions.Context
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * Base class for a pattern definition.
 *
 * A pattern definition is used by [[org.apache.flink.cep.nfa.compiler.NFACompiler]] to create a
 * [[org.apache.flink.cep.nfa.NFA]].
 *
 * {{{
 * Pattern<T, F> pattern = Pattern.<T>begin("start")
 *   .next("middle").subtype(F.class)
 *   .followedBy("end").where(new MyCondition());
 * }}}
 *
 * @param jPattern
 *   Underlying Java API Pattern
 * @tparam T
 *   Base type of the elements appearing in the pattern
 * @tparam F
 *   Subtype of T to which the current pattern operator is constrained
 * @deprecated
 *   All Flink Scala APIs are deprecated and will be removed in a future Flink major version. You
 *   can still build your application in Scala, but you should move to the Java version of either
 *   the DataStream and/or Table API.
 * @see
 *   <a href="https://s.apache.org/flip-265">FLIP-265 Deprecate and remove Scala API support</a>
 */
@deprecated(org.apache.flink.api.scala.FLIP_265_WARNING, since = "1.18.0")
class Pattern[T, F <: T](jPattern: JPattern[T, F]) {

  private[flink] def wrappedPattern = jPattern

  /** @return The previous pattern */
  def getPrevious: Option[Pattern[T, _ <: T]] = {
    wrapPattern(jPattern.getPrevious)
  }

  /** @return Name of the pattern operator */
  def getName: String = jPattern.getName

  /** @return Window length in which the pattern match has to occur */
  def getWindowTime: Option[Time] = {
    Option(jPattern.getWindowTime)
  }

  /** @return currently applied quantifier to this pattern */
  def getQuantifier: Quantifier = jPattern.getQuantifier

  def getCondition: Option[IterativeCondition[F]] = {
    Option(jPattern.getCondition)
  }

  def getUntilCondition: Option[IterativeCondition[F]] = {
    Option(jPattern.getUntilCondition)
  }

  /**
   * Adds a condition that has to be satisfied by an event in order to be considered a match. If
   * another condition has already been set, the new one is going to be combined with the previous
   * with a logical {{{AND}}}. In other case, this is going to be the only condition.
   *
   * @param condition
   *   The condition as an [[IterativeCondition]].
   * @return
   *   The pattern with the new condition is set.
   */
  def where(condition: IterativeCondition[F]): Pattern[T, F] = {
    jPattern.where(condition)
    this
  }

  /**
   * Adds a condition that has to be satisfied by an event in order to be considered a match. If
   * another condition has already been set, the new one is going to be combined with the previous
   * with a logical {{{AND}}}. In other case, this is going to be the only condition.
   *
   * @param condition
   *   The condition to be set.
   * @return
   *   The pattern with the new condition is set.
   */
  def where(condition: (F, Context[F]) => Boolean): Pattern[T, F] = {
    val condFun = new IterativeCondition[F] {
      val cleanCond = cep.scala.cleanClosure(condition)

      override def filter(value: F, ctx: JContext[F]): Boolean = {
        cleanCond(value, new JContextWrapper(ctx))
      }
    }
    where(condFun)
  }

  /**
   * Adds a condition that has to be satisfied by an event in order to be considered a match. If
   * another condition has already been set, the new one is going to be combined with the previous
   * with a logical {{{AND}}}. In other case, this is going to be the only condition.
   *
   * @param condition
   *   The condition to be set.
   * @return
   *   The pattern with the new condition is set.
   */
  def where(condition: F => Boolean): Pattern[T, F] = {
    val condFun = new IterativeCondition[F] {
      val cleanCond = cep.scala.cleanClosure(condition)

      override def filter(value: F, ctx: JContext[F]): Boolean = cleanCond(value)
    }
    where(condFun)
  }

  /**
   * Adds a condition that has to be satisfied by an event in order to be considered a match. If
   * another condition has already been set, the new one is going to be combined with the previous
   * with a logical {{{OR}}}. In other case, this is going to be the only condition.
   *
   * @param condition
   *   The condition as an [[IterativeCondition]].
   * @return
   *   The pattern with the new condition is set.
   */
  def or(condition: IterativeCondition[F]): Pattern[T, F] = {
    jPattern.or(condition)
    this
  }

  /**
   * Adds a condition that has to be satisfied by an event in order to be considered a match. If
   * another condition has already been set, the new one is going to be combined with the previous
   * with a logical {{{OR}}}. In other case, this is going to be the only condition.
   *
   * @param condition
   *   The {{{OR}}} condition.
   * @return
   *   The pattern with the new condition is set.
   */
  def or(condition: F => Boolean): Pattern[T, F] = {
    val condFun = new SimpleCondition[F] {
      val cleanCond = cep.scala.cleanClosure(condition)

      override def filter(value: F): Boolean =
        cleanCond(value)
    }
    or(condFun)
  }

  /**
   * Adds a condition that has to be satisfied by an event in order to be considered a match. If
   * another condition has already been set, the new one is going to be combined with the previous
   * with a logical {{{OR}}}. In other case, this is going to be the only condition.
   *
   * @param condition
   *   The {{{OR}}} condition.
   * @return
   *   The pattern with the new condition is set.
   */
  def or(condition: (F, Context[F]) => Boolean): Pattern[T, F] = {
    val condFun = new IterativeCondition[F] {
      val cleanCond = cep.scala.cleanClosure(condition)

      override def filter(value: F, ctx: JContext[F]): Boolean =
        cleanCond(value, new JContextWrapper(ctx))
    }
    or(condFun)
  }

  /**
   * Applies a subtype constraint on the current pattern. This means that an event has to be of the
   * given subtype in order to be matched.
   *
   * @param clazz
   *   Class of the subtype
   * @tparam S
   *   Type of the subtype
   * @return
   *   The same pattern with the new subtype constraint
   */
  def subtype[S <: F](clazz: Class[S]): Pattern[T, S] = {
    jPattern.subtype(clazz)
    this.asInstanceOf[Pattern[T, S]]
  }

  /**
   * Applies a stop condition for a looping state. It allows cleaning the underlying state.
   *
   * @param untilCondition
   *   a condition an event has to satisfy to stop collecting events into looping state
   * @return
   *   The same pattern with applied untilCondition
   */
  def until(untilCondition: IterativeCondition[F]): Pattern[T, F] = {
    jPattern.until(untilCondition)
    this
  }

  /**
   * Applies a stop condition for a looping state. It allows cleaning the underlying state.
   *
   * @param untilCondition
   *   a condition an event has to satisfy to stop collecting events into looping state
   * @return
   *   The same pattern with applied untilCondition
   */
  def until(untilCondition: (F, Context[F]) => Boolean): Pattern[T, F] = {
    val condFun = new IterativeCondition[F] {
      val cleanCond = cep.scala.cleanClosure(untilCondition)

      override def filter(value: F, ctx: JContext[F]): Boolean =
        cleanCond(value, new JContextWrapper(ctx))
    }
    until(condFun)
  }

  /**
   * Applies a stop condition for a looping state. It allows cleaning the underlying state.
   *
   * @param untilCondition
   *   a condition an event has to satisfy to stop collecting events into looping state
   * @return
   *   The same pattern with applied untilCondition
   */
  def until(untilCondition: F => Boolean): Pattern[T, F] = {
    val condFun = new IterativeCondition[F] {
      val cleanCond = cep.scala.cleanClosure(untilCondition)

      override def filter(value: F, ctx: JContext[F]): Boolean = cleanCond(value)
    }
    until(condFun)
  }

  /**
   * Defines the maximum time interval in which a matching pattern has to be completed in order to
   * be considered valid. This interval corresponds to the maximum time gap between first and the
   * last event.
   *
   * @param windowTime
   *   Time of the matching window
   * @return
   *   The same pattern operator with the new window length
   */
  def within(windowTime: Time): Pattern[T, F] = {
    jPattern.within(windowTime)
    this
  }

  /**
   * Appends a new pattern to the existing one. The new pattern enforces strict temporal contiguity.
   * This means that the whole pattern sequence matches only if an event which matches this pattern
   * directly follows the preceding matching event. Thus, there cannot be any events in between two
   * matching events.
   *
   * @param name
   *   Name of the new pattern
   * @return
   *   A new pattern which is appended to this one
   */
  def next(name: String): Pattern[T, T] = {
    Pattern[T, T](jPattern.next(name))
  }

  /**
   * Appends a new pattern to the existing one. The new pattern enforces that there is no event
   * matching this pattern right after the preceding matched event.
   *
   * @param name
   *   Name of the new pattern
   * @return
   *   A new pattern which is appended to this one
   */
  def notNext(name: String): Pattern[T, T] = {
    Pattern[T, T](jPattern.notNext(name))
  }

  /**
   * Appends a new pattern to the existing one. The new pattern enforces non-strict temporal
   * contiguity. This means that a matching event of this pattern and the preceding matching event
   * might be interleaved with other events which are ignored.
   *
   * @param name
   *   Name of the new pattern
   * @return
   *   A new pattern which is appended to this one
   */
  def followedBy(name: String): Pattern[T, T] = {
    Pattern[T, T](jPattern.followedBy(name))
  }

  /**
   * Appends a new pattern to the existing one. The new pattern enforces that there is no event
   * matching this pattern between the preceding pattern and succeeding this one.
   *
   * NOTE: There has to be other pattern after this one.
   *
   * @param name
   *   Name of the new pattern
   * @return
   *   A new pattern which is appended to this one
   */
  def notFollowedBy(name: String): Pattern[T, T] = {
    Pattern[T, T](jPattern.notFollowedBy(name))
  }

  /**
   * Appends a new pattern to the existing one. The new pattern enforces non-strict temporal
   * contiguity. This means that a matching event of this pattern and the preceding matching event
   * might be interleaved with other events which are ignored.
   *
   * @param name
   *   Name of the new pattern
   * @return
   *   A new pattern which is appended to this one
   */
  def followedByAny(name: String): Pattern[T, T] = {
    Pattern[T, T](jPattern.followedByAny(name))
  }

  /**
   * Specifies that this pattern is optional for a final match of the pattern sequence to happen.
   *
   * @return
   *   The same pattern as optional.
   * @throws MalformedPatternException
   *   if the quantifier is not applicable to this pattern.
   */
  def optional: Pattern[T, F] = {
    jPattern.optional()
    this
  }

  /**
   * Specifies that this pattern can occur {{{one or more}}} times. This means at least one and at
   * most infinite number of events can be matched to this pattern.
   *
   * If this quantifier is enabled for a pattern {{{A.oneOrMore().followedBy(B)}}} and a sequence of
   * events {{{A1 A2 B}}} appears, this will generate patterns: {{{A1 B}}} and {{{A1 A2 B}}}. See
   * also {{{allowCombinations()}}}.
   *
   * @return
   *   The same pattern with a [[Quantifier.looping()]] quantifier applied.
   * @throws MalformedPatternException
   *   if the quantifier is not applicable to this pattern.
   */
  def oneOrMore: Pattern[T, F] = {
    jPattern.oneOrMore()
    this
  }

  /**
   * Specifies that this pattern is greedy. This means as many events as possible will be matched to
   * this pattern.
   *
   * @return
   *   The same pattern with { @link Quantifier#greedy} set to true.
   * @throws MalformedPatternException
   *   if the quantifier is not applicable to this pattern.
   */
  def greedy: Pattern[T, F] = {
    jPattern.greedy()
    this
  }

  /**
   * Specifies exact number of times that this pattern should be matched.
   *
   * @param times
   *   number of times matching event must appear
   * @return
   *   The same pattern with number of times applied
   * @throws MalformedPatternException
   *   if the quantifier is not applicable to this pattern.
   */
  def times(times: Int): Pattern[T, F] = {
    jPattern.times(times)
    this
  }

  /**
   * Specifies that the pattern can occur between from and to times.
   *
   * @param from
   *   number of times matching event must appear at least
   * @param to
   *   number of times matching event must appear at most
   * @return
   *   The same pattern with the number of times range applied
   * @throws MalformedPatternException
   *   if the quantifier is not applicable to this pattern.
   */
  def times(from: Int, to: Int): Pattern[T, F] = {
    jPattern.times(from, to)
    this
  }

  /**
   * Specifies that this pattern can occur the specified times at least. This means at least the
   * specified times and at most infinite number of events can be matched to this pattern.
   *
   * @return
   *   The same pattern with a { @link Quantifier#looping(ConsumingStrategy)} quantifier applied.
   * @throws MalformedPatternException
   *   if the quantifier is not applicable to this pattern.
   */
  def timesOrMore(times: Int): Pattern[T, F] = {
    jPattern.timesOrMore(times)
    this
  }

  /**
   * Applicable only to [[Quantifier.looping()]] and [[Quantifier.times()]] patterns, this option
   * allows more flexibility to the matching events.
   *
   * If {{{allowCombinations()}}} is not applied for a pattern {{{A.oneOrMore().followedBy(B)}}} and
   * a sequence of events {{{A1 A2 B}}} appears, this will generate patterns: {{{A1 B}}} and
   * {{{A1 A2 B}}}. If this method is applied, we will have {{{A1 B}}}, {{{A2 B}}} and
   * {{{A1 A2 B}}}.
   *
   * @return
   *   The same pattern with the updated quantifier.
   * @throws MalformedPatternException
   *   if the quantifier is not applicable to this pattern.
   */
  def allowCombinations(): Pattern[T, F] = {
    jPattern.allowCombinations()
    this
  }

  /**
   * Works in conjunction with [[Pattern#oneOrMore()]] or [[Pattern#times(int)]]. Specifies that any
   * not matching element breaks the loop.
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
   * @return
   *   pattern with continuity changed to strict
   */
  def consecutive(): Pattern[T, F] = {
    jPattern.consecutive()
    this
  }

  /**
   * Appends a new group pattern to the existing one. The new pattern enforces non-strict temporal
   * contiguity. This means that a matching event of this pattern and the preceding matching event
   * might be interleaved with other events which are ignored.
   *
   * @param pattern
   *   the pattern to append
   * @return
   *   A new pattern which is appended to this one
   */
  def followedBy(pattern: Pattern[T, F]): GroupPattern[T, F] =
    GroupPattern[T, F](jPattern.followedBy(pattern.wrappedPattern))

  /**
   * Appends a new group pattern to the existing one. The new pattern enforces non-strict temporal
   * contiguity. This means that a matching event of this pattern and the preceding matching event
   * might be interleaved with other events which are ignored.
   *
   * @param pattern
   *   the pattern to append
   * @return
   *   A new pattern which is appended to this one
   */
  def followedByAny(pattern: Pattern[T, F]): GroupPattern[T, F] =
    GroupPattern[T, F](jPattern.followedByAny(pattern.wrappedPattern))

  /**
   * Appends a new group pattern to the existing one. The new pattern enforces strict temporal
   * contiguity. This means that the whole pattern sequence matches only if an event which matches
   * this pattern directly follows the preceding matching event. Thus, there cannot be any events in
   * between two matching events.
   *
   * @param pattern
   *   the pattern to append
   * @return
   *   A new pattern which is appended to this one
   */
  def next(pattern: Pattern[T, F]): GroupPattern[T, F] =
    GroupPattern[T, F](jPattern.next(pattern.wrappedPattern))

  /**
   * Get after match skip strategy.
   * @return
   *   current after match skip strategy
   */
  def getAfterMatchSkipStrategy: AfterMatchSkipStrategy =
    jPattern.getAfterMatchSkipStrategy
}

object Pattern {

  /**
   * Constructs a new Pattern by wrapping a given Java API Pattern
   *
   * @param jPattern
   *   Underlying Java API Pattern.
   * @tparam T
   *   Base type of the elements appearing in the pattern
   * @tparam F
   *   Subtype of T to which the current pattern operator is constrained
   * @return
   *   New wrapping Pattern object
   */
  def apply[T, F <: T](jPattern: JPattern[T, F]) = new Pattern[T, F](jPattern)

  /**
   * Starts a new pattern sequence. The provided name is the one of the initial pattern of the new
   * sequence. Furthermore, the base type of the event sequence is set.
   *
   * @param name
   *   The name of starting pattern of the new pattern sequence
   * @tparam X
   *   Base type of the event pattern
   * @return
   *   The first pattern of a pattern sequence
   */
  def begin[X](name: String): Pattern[X, X] = Pattern(JPattern.begin(name))

  /**
   * Starts a new pattern sequence. The provided name is the one of the initial pattern of the new
   * sequence. Furthermore, the base type of the event sequence is set.
   *
   * @param name
   *   The name of starting pattern of the new pattern sequence
   * @param afterMatchSkipStrategy
   *   The skip strategy to use after each match
   * @tparam X
   *   Base type of the event pattern
   * @return
   *   The first pattern of a pattern sequence
   */
  def begin[X](name: String, afterMatchSkipStrategy: AfterMatchSkipStrategy): Pattern[X, X] =
    Pattern(JPattern.begin(name, afterMatchSkipStrategy))

  /**
   * Starts a new pattern sequence. The provided pattern is the initial pattern of the new sequence.
   *
   * @param pattern
   *   the pattern to begin with
   * @return
   *   the first pattern of a pattern sequence
   */
  def begin[T, F <: T](pattern: Pattern[T, F]): GroupPattern[T, F] =
    GroupPattern[T, F](JPattern.begin(pattern.wrappedPattern))

  /**
   * Starts a new pattern sequence. The provided pattern is the initial pattern of the new sequence.
   *
   * @param pattern
   *   the pattern to begin with
   * @param afterMatchSkipStrategy
   *   The skip strategy to use after each match
   * @return
   *   The first pattern of a pattern sequence
   */
  def begin[T, F <: T](
      pattern: Pattern[T, F],
      afterMatchSkipStrategy: AfterMatchSkipStrategy): GroupPattern[T, F] =
    GroupPattern(JPattern.begin(pattern.wrappedPattern, afterMatchSkipStrategy))

}
