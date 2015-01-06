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

package org.apache.flink.streaming.api.scala

import org.apache.flink.streaming.api.datastream.{ SplitDataStream => SplitJavaStream }

/**
 * The SplitDataStream represents an operator that has been split using an
 * {@link OutputSelector}. Named outputs can be selected using the
 * {@link #select} function.
 *
 * @param <OUT>
 *            The type of the output.
 */
class SplitDataStream[T](javaStream: SplitJavaStream[T]) {

  /**
   * Gets the underlying java DataStream object.
   */
  private[flink] def getJavaStream: SplitJavaStream[T] = javaStream

  /**
   *  Sets the output names for which the next operator will receive values.
   */
  def select(outputNames: String*): DataStream[T] = javaStream.select(outputNames: _*)

  /**
   * Selects all output names from a split data stream.
   */
  def selectAll(): DataStream[T] = javaStream.selectAll()

}
