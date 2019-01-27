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
package org.apache.flink.table.runtime.aggregate

import org.apache.flink.table.dataformat.BaseRow

/**
  * The InputCounter is used to count input element number under the current key.
  */
object InputCounter {

  def apply(inputCountIndex: Option[Int]): InputCounter = inputCountIndex match {
    case None => new AccumulationInputCounter
    case Some(index) => new RetractionInputCounter(index)
  }

}

abstract class InputCounter extends Serializable {

  /**
    * We store the counter in the accumulator. If the counter is not zero, which means
    * we aggregated at least one record for this key.
    *
    * @return true if input count is zero, false if not.
    */
  def countIsZero(acc: BaseRow): Boolean

}

final class AccumulationInputCounter extends InputCounter {

  // when all the inputs are accumulations, the count will never be zero
  override def countIsZero(acc: BaseRow): Boolean = acc == null

}

final class RetractionInputCounter(countIndex: Int) extends InputCounter {

  // We store the counter in the accumulator and the counter is never be null
  override def countIsZero(acc: BaseRow): Boolean = acc == null || acc.getLong(countIndex) == 0

}

