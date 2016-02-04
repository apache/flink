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

import java.math.BigInteger
import com.google.common.math.LongMath

// for byte, short, int we return int
abstract class IntegralAvgAggregate[T: Numeric] extends Aggregate[T] {
  
  var sum: Long = 0
  var count: Long = 0

  override def initiateAggregate: Unit = {
    sum = 0
    count = 0
  }

}

class ByteAvgAggregate extends IntegralAvgAggregate[Byte] {

  override def aggregate(value: Any): Unit = {
    count += 1
    sum = LongMath.checkedAdd(sum, value.asInstanceOf[Byte])
  }

  override def getAggregated(): Byte = {
    (sum / count).toByte
  }
}

class ShortAvgAggregate extends IntegralAvgAggregate[Short] {

  override def aggregate(value: Any): Unit = {
    count += 1
    sum = LongMath.checkedAdd(sum, value.asInstanceOf[Short])
  }

  override def getAggregated(): Short = {
    (sum / count).toShort
  }
}

class IntAvgAggregate extends IntegralAvgAggregate[Int] {

  override def aggregate(value: Any): Unit = {
    count += 1
    sum = LongMath.checkedAdd(sum, value.asInstanceOf[Int])
  }

  override def getAggregated(): Int = {
    (sum / count).toInt
  }
}

// Long average aggregate return Long as aggregated value.
class LongAvgAggregate extends Aggregate[Long] {

  var sum: BigInteger = BigInteger.ZERO
  var count: Long = 0

  override def initiateAggregate: Unit = {
    sum = BigInteger.ZERO
    count = 0
  }

  override def aggregate(value: Any): Unit = {
    count += 1
    sum = sum.add(BigInteger.valueOf(value.asInstanceOf[Long]))
  }

  override def getAggregated(): Long = {
    sum.divide(BigInteger.valueOf(count)).longValue
  }
}

// Float average aggregate return Float as aggregated value.
abstract class FloatingPointAvgAggregate[T: Numeric] extends Aggregate[T] {

  var avgValue: Double = 0
  var count: Long = 0

  override def initiateAggregate: Unit = {
    avgValue = 0
    count = 0
  }
}

// Double average aggregate return Double as aggregated value.
class FloatAvgAggregate extends FloatingPointAvgAggregate[Float] {

  override def aggregate(value: Any): Unit = {
    count += 1
    val current = value.asInstanceOf[Float]
    avgValue += (current - avgValue) / count
  }

  override def getAggregated(): Float = {
    avgValue.toFloat
  }
}

// Double average aggregate return Double as aggregated value.
class DoubleAvgAggregate extends FloatingPointAvgAggregate[Double] {

  override def aggregate(value: Any): Unit = {
    count += 1
    val current = value.asInstanceOf[Double]
    avgValue += (current - avgValue) / count
  }

  override def getAggregated(): Double = {
    avgValue
  }
}
