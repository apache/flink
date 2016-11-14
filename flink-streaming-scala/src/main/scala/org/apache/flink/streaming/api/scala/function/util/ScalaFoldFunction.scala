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

package org.apache.flink.streaming.api.scala.function.util

import org.apache.flink.api.common.functions.FoldFunction

/**
 * A wrapper function that exposes a Scala Function2 as a [[FoldFunction]].
 */
final class ScalaFoldFunction[T, R](private[this] val foldFunction: (R, T) => R)
    extends FoldFunction[T, R] {
  
  @throws(classOf[Exception])
  override def fold(accumulator: R, value: T): R = {
    foldFunction(accumulator, value)
  }
}
