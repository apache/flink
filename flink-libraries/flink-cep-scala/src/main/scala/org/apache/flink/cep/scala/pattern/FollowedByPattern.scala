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

import org.apache.flink.cep.pattern.{FollowedByPattern => JFollowedByPattern}

object FollowedByPattern {
  /**
    * Constructs a new Pattern by wrapping a given Java API Pattern
    *
    * @param jfbPattern Underlying Java API Pattern.
    * @tparam T Base type of the elements appearing in the pattern
    * @tparam F Subtype of T to which the current pattern operator is constrained
    * @return New wrapping FollowedByPattern object
    */
  def apply[T, F <: T](jfbPattern: JFollowedByPattern[T, F]) =
    new FollowedByPattern[T, F](jfbPattern)
}

/**
  * Pattern operator which signifies that the there is a non-strict temporal contiguity between
  * itself and its preceding pattern operator. This means that there might be events in between
  * two matching events. These events are then simply ignored.
  *
  * @tparam T Base type of the events
  * @tparam F Subtype of T to which the operator is currently constrained
  */
class FollowedByPattern[T, F <: T](jfbPattern: JFollowedByPattern[T, F])
  extends Pattern[T, F](jfbPattern)
