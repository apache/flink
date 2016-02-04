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
package org.apache.flink.api.table.runtime.aggregate

abstract class MaxAggregate[T: Numeric] extends Aggregate[T] {

  var result: T = _
  val numericResult = implicitly[Numeric[T]]

  override def aggregate(value: Any): Unit = {
    val input: T = value.asInstanceOf[T]

    result = numericResult.max(result, input)
  }

  override def getAggregated(): T = {
    result
  }

}

// Numeric doesn't have min value
class TinyIntMaxAggregate extends MaxAggregate[Byte] {

  override def initiateAggregate: Unit = {
    result = Byte.MinValue
  }

}

class SmallIntMaxAggregate extends MaxAggregate[Short] {

  override def initiateAggregate: Unit = {
    result = Short.MinValue
  }

}

class IntMaxAggregate extends MaxAggregate[Int] {

  override def initiateAggregate: Unit = {
    result = Int.MinValue
  }

}

class LongMaxAggregate extends MaxAggregate[Long] {

  override def initiateAggregate: Unit = {
    result = Long.MinValue
  }

}

class FloatMaxAggregate extends MaxAggregate[Float] {

  override def initiateAggregate: Unit = {
    result = Float.MinValue
  }

}

class DoubleMaxAggregate extends MaxAggregate[Double] {

  override def initiateAggregate: Unit = {
    result = Double.MinValue
  }

}
