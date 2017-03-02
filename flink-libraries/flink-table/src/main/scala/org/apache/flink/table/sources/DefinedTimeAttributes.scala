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

package org.apache.flink.table.sources

import org.apache.flink.api.java.tuple.Tuple2

/**
  * Defines logical time attributes for a [[TableSource]]. Time attributes can be used for
  * indicating, accessing, and working with Flink's event-time or processing-time. A
  * [[TableSource]] that implements this interface can define names and positions of rowtime
  * and proctime attributes in the rows it produces.
  */
trait DefinedTimeAttributes {

  /**
    * Defines a name and position (starting at 0) of rowtime attribute that represents Flink's
    * event-time. Null if no rowtime should be available. If the position is within the arity of
    * the result row, the logical attribute will overwrite the physical attribute. If the position
    * is higher than the result row, the time attribute will be appended logically.
    */
  def getRowtimeAttribute: Tuple2[Int, String]

  /**
    * Defines a name and position (starting at 0) of proctime attribute that represents Flink's
    * processing-time. Null if no proctime should be available. If the position is within the arity
    * of the result row, the logical attribute will overwrite the physical attribute. If the
    * position is higher than the result row, the time attribute will be appended logically.
    */
  def getProctimeAttribute: Tuple2[Int, String]

}
