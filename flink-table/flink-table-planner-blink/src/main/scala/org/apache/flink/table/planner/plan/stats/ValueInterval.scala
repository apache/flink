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

package org.apache.flink.table.planner.plan.stats

import org.apache.flink.table.planner.plan.utils.ColumnIntervalUtil.convertNumberToString
import org.apache.flink.util.Preconditions

/** Value range. */
trait ValueInterval

/**
  * This is empty intervals.
  */
case object EmptyValueInterval extends ValueInterval

/**
  * This is infinite interval, which is (negative infinity, positive infinity)
  */
case object InfiniteValueInterval extends ValueInterval

/**
  *
  * This is intervals which has lower.
  */
trait WithLower extends ValueInterval {
  val lower: Comparable[_]
  val includeLower: Boolean
}

/**
  * This is intervals which has upper.
  */
trait WithUpper extends ValueInterval {
  val upper: Comparable[_]
  val includeUpper: Boolean
}

/**
  * This is for finite interval which has lower and upper both.
  *
  * @param lower        lower limit of interval
  * @param upper        upper limit of interval
  * @param includeLower whether include lower
  * @param includeUpper whether include upper
  */
case class FiniteValueInterval private(
    lower: Comparable[_],
    upper: Comparable[_],
    includeLower: Boolean = true,
    includeUpper: Boolean = true
) extends WithLower with WithUpper {
  require(lower != null && upper != null && upper.getClass == lower.getClass)
}

/**
  * This is for left semi infinite interval, which only has upper limit .
  *
  * @param upper        upper limit of interval
  * @param includeUpper whether include upper
  */
case class LeftSemiInfiniteValueInterval private(
    upper: Comparable[_],
    includeUpper: Boolean = true
) extends WithUpper {
  require(upper != null)
}

/**
  * This is for right semi infinite interval, which only has lower limit .
  *
  * @param lower        lower limit of interval
  * @param includeLower whether include lower
  */
case class RightSemiInfiniteValueInterval private(
    lower: Comparable[_],
    includeLower: Boolean = true
) extends WithLower {
  require(lower != null)
}

object ValueInterval {

  def apply(
      lower: Any,
      upper: Any,
      includeLower: Boolean = true,
      includeUpper: Boolean = true): ValueInterval = {
    (lower, upper) match {
      case (lowerV: Comparable[_], upperV: Comparable[_]) =>
        if (lower.getClass != upper.getClass) {
          throw new IllegalArgumentException(
            s"lower: ${lower.getClass} must be same class as upper: ${upper.getClass}")
        }
        val compareResult = compare(lowerV, upperV)
        Preconditions.checkArgument(compareResult <= 0)
        if (compareResult == 0 && (!includeLower || !includeUpper)) {
          EmptyValueInterval
        } else {
          FiniteValueInterval(lowerV, upperV, includeLower, includeUpper)
        }
      case (null, null) =>
        throw new IllegalArgumentException("upper and lower can not be null at the same time!")
      case (lowerV: Comparable[_], null) => RightSemiInfiniteValueInterval(lowerV, includeLower)
      case (_, null) =>
        throw new IllegalArgumentException(
          s"Class of lower ${lower.getClass} is not a sub-class of Comparable!")
      case (null, upperV: Comparable[_]) =>
        LeftSemiInfiniteValueInterval(upperV, includeUpper)
      case (null, _) =>
        throw new IllegalArgumentException(
          s"Class of upper ${lower.getClass} is not a sub-class of Comparable!")
      case (_, _) =>
        throw new IllegalArgumentException(
          s"Lower and upper must be Comparable, but lower is instance of" +
            s" ${lower.getClass} and upper is instance of ${upper.getClass}!")
    }
  }

  val empty: ValueInterval = EmptyValueInterval

  val infinite: ValueInterval = InfiniteValueInterval

  def contains(interval: ValueInterval, value: Any): Boolean = {
    value match {
      case _: Comparable[_] =>
        val singlePoint = ValueInterval(value, value, includeLower = true, includeUpper = true)
        isIntersected(interval, singlePoint)
      case null => false
      case _ =>
        throw new IllegalArgumentException(
          s"Class of value ${value.getClass} is not a sub-class of Comparable!")
    }
  }

  /**
    * Get union result of two intervals
    *
    * @param first  first interval
    * @param second second interval
    * @return union result of two input intervals.
    */
  def union(first: ValueInterval, second: ValueInterval): ValueInterval = (first, second) match {
    case (EmptyValueInterval, _) => second
    case (_, EmptyValueInterval) => first
    case (InfiniteValueInterval, _) | (_, InfiniteValueInterval) => InfiniteValueInterval
    case (_: LeftSemiInfiniteValueInterval, _: RightSemiInfiniteValueInterval) |
         (_: RightSemiInfiniteValueInterval, _: LeftSemiInfiniteValueInterval) =>
      InfiniteValueInterval
    case (n1: FiniteValueInterval, n2: FiniteValueInterval) =>
      val (newLower, includeLower) = unionLower(n1, n2)
      val (newUpper, includeUpper) = unionUpper(n1, n2)
      FiniteValueInterval(newLower, newUpper, includeLower, includeUpper)
    case (n1: WithUpper, n2: WithUpper) =>
      val (upper, includeUpper) = unionUpper(n1, n2)
      LeftSemiInfiniteValueInterval(upper, includeUpper)
    case (n1: WithLower, n2: WithLower) =>
      val (lower, includeLower) = unionLower(n1, n2)
      RightSemiInfiniteValueInterval(lower, includeLower)
    case (_, null) => null
    case (null, _) => null

  }

  /**
    * Get intersected results of two intervals.
    *
    * @param first  first interval
    * @param second second interval
    * @return NullValueInterval if two intervals are not intersected,
    *         or intersected results of two intervals.
    */
  def intersect(first: ValueInterval, second: ValueInterval): ValueInterval = {
    if (!isIntersected(first, second)) {
      EmptyValueInterval
    } else {
      (first, second) match {
        case (_, InfiniteValueInterval) => first
        case (InfiniteValueInterval, _) => second
        case (n1: LeftSemiInfiniteValueInterval, n2: LeftSemiInfiniteValueInterval) =>
          val (newUpper, includeUpper) = intersectUpper(n1, n2)
          LeftSemiInfiniteValueInterval(newUpper, includeUpper)
        case (n1: RightSemiInfiniteValueInterval, n2: RightSemiInfiniteValueInterval) =>
          val (newLower, includeLower) = intersectLower(n1, n2)
          RightSemiInfiniteValueInterval(newLower, includeLower)
        case (n1: FiniteValueInterval, n2: FiniteValueInterval) =>
          val (newLower, includeLower) = intersectLower(n1, n2)
          val (newUpper, includeUpper) = intersectUpper(n1, n2)
          FiniteValueInterval(newLower, newUpper, includeLower, includeUpper)
        case (n1: LeftSemiInfiniteValueInterval, n2: FiniteValueInterval) =>
          val (newUpper, includeUpper) = intersectUpper(n1, n2)
          FiniteValueInterval(n2.lower, newUpper, n2.includeLower, includeUpper)
        case (n1: FiniteValueInterval, n2: LeftSemiInfiniteValueInterval) =>
          val (newUpper, includeUpper) = intersectUpper(n1, n2)
          FiniteValueInterval(n1.lower, newUpper, n1.includeLower, includeUpper)
        case (n1: RightSemiInfiniteValueInterval, n2: FiniteValueInterval) =>
          val (newLower, includeLower) = intersectLower(n1, n2)
          FiniteValueInterval(newLower, n2.upper, includeLower, n2.includeUpper)
        case (n1: FiniteValueInterval, n2: RightSemiInfiniteValueInterval) =>
          val (newLower, includeLower) = intersectLower(n1, n2)
          FiniteValueInterval(newLower, n1.upper, includeLower, n1.includeUpper)
        case (n1: LeftSemiInfiniteValueInterval, n2: RightSemiInfiniteValueInterval) =>
          FiniteValueInterval(n2.lower, n1.upper, n2.includeLower, n1.includeUpper)
        case (n1: RightSemiInfiniteValueInterval, n2: LeftSemiInfiniteValueInterval) =>
          FiniteValueInterval(n1.lower, n2.upper, n1.includeLower, n2.includeUpper)
      }
    }
  }


  /**
    * Get whether two input intervals is intersected
    *
    * @param first  first interval
    * @param second second interval
    * @return true if two input intervals is intersected, or false else.
    */
  def isIntersected(first: ValueInterval, second: ValueInterval): Boolean = (first, second) match {
    case (_, EmptyValueInterval) | (EmptyValueInterval, _) => false
    case (_, InfiniteValueInterval) | (InfiniteValueInterval, _) => true
    case (_: LeftSemiInfiniteValueInterval, _: LeftSemiInfiniteValueInterval) |
         (_: RightSemiInfiniteValueInterval, _: RightSemiInfiniteValueInterval) => true
    case (n1: LeftSemiInfiniteValueInterval, n2: RightSemiInfiniteValueInterval) =>
      compareAndHandle[Boolean](n1.upper, n2.lower, false, n1.includeUpper && n2.includeLower, true)
    case (n1: RightSemiInfiniteValueInterval, n2: LeftSemiInfiniteValueInterval) =>
      compareAndHandle[Boolean](n2.upper, n1.lower, false, n1.includeLower && n2.includeUpper, true)
    case (n1: LeftSemiInfiniteValueInterval, n2: FiniteValueInterval) =>
      compareAndHandle[Boolean](n1.upper, n2.lower, false, n1.includeUpper && n2.includeLower, true)
    case (n1: FiniteValueInterval, n2: LeftSemiInfiniteValueInterval) =>
      compareAndHandle[Boolean](n2.upper, n1.lower, false, n1.includeLower && n2.includeUpper, true)
    case (n1: RightSemiInfiniteValueInterval, n2: FiniteValueInterval) =>
      compareAndHandle[Boolean](n1.lower, n2.upper, true, n1.includeLower && n2.includeUpper, false)
    case (n1: FiniteValueInterval, n2: RightSemiInfiniteValueInterval) =>
      compareAndHandle[Boolean](n2.lower, n1.upper, true, n1.includeUpper && n2.includeLower, false)
    case (n1: FiniteValueInterval, n2: FiniteValueInterval) =>
      val cmp1 = compareAndHandle[Boolean](
        n1.lower, n2.upper, true, n1.includeLower && n2.includeUpper, false)
      val cmp2 = compareAndHandle[Boolean](
        n1.upper, n2.lower, false, n1.includeUpper && n2.includeLower, true)
      cmp1 && cmp2
  }


  private[this] def unionLower(one: WithLower, other: WithLower): (Comparable[_], Boolean) =
    compareAndHandle[(Comparable[_], Boolean)](one.lower, other.lower,
      (one.lower, one.includeLower),
      (one.lower, one.includeLower || other.includeLower),
      (other.lower, other.includeLower))

  private[this] def unionUpper(one: WithUpper, other: WithUpper): (Comparable[_], Boolean) =
    compareAndHandle[(Comparable[_], Boolean)](one.upper, other.upper,
      (other.upper, other.includeUpper),
      (one.upper, one.includeUpper || other.includeUpper),
      (one.upper, one.includeUpper))

  private[this] def intersectLower(one: WithLower, other: WithLower): (Comparable[_], Boolean) =
    compareAndHandle[(Comparable[_], Boolean)](one.lower, other.lower,
      (other.lower, other.includeLower),
      (one.lower, one.includeLower && other.includeLower),
      (one.lower, one.includeLower))


  private[this] def intersectUpper(one: WithUpper, other: WithUpper): (Comparable[_], Boolean) =
    compareAndHandle[(Comparable[_], Boolean)](one.upper, other.upper,
      (one.upper, one.includeUpper),
      (one.upper, one.includeUpper && other.includeUpper),
      (other.upper, other.includeUpper))


  private[this] def compare(one: Comparable[_], other: Comparable[_]): Int = {
    (convertNumberToString(one), convertNumberToString(other)) match {
      case (Some(numStr1), Some(numStr2)) =>
        new java.math.BigDecimal(numStr1).compareTo(new java.math.BigDecimal(numStr2))
      case _ =>
        Preconditions.checkArgument(one.getClass == other.getClass)
        val (convertedOne, convertedOther) = (
          one.asInstanceOf[Comparable[Any]],
          other.asInstanceOf[Comparable[Any]])
        convertedOne.compareTo(convertedOther)
    }
  }

  private[this] def compareAndHandle[T](
      one: Comparable[_],
      other: Comparable[_],
      lessCallback: T,
      equalsCallback: T,
      biggerCallback: T): T = {
    val compareResult = compare(one, other)
    if (compareResult < 0) {
      lessCallback
    } else if (compareResult == 0) {
      equalsCallback
    } else {
      biggerCallback
    }
  }
}
