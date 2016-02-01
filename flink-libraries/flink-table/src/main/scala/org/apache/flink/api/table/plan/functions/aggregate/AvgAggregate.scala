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
package org.apache.flink.api.table.plan.functions.aggregate

abstract class AvgAggregate[T] extends Aggregate[T] {

}

// TinyInt average aggregate return Int as aggregated value.
class TinyIntAvgAggregate extends AvgAggregate[Int] {
  private var avgValue: Double = 0
  private var count: Int = 0

  override def initiateAggregate: Unit = {
    avgValue = 0
    count = 0
  }

  override def aggregate(value: Any): Unit = {
    count += 1
    val current = value.asInstanceOf[Byte]
    avgValue += (current - avgValue) / count
  }

  override def getAggregated(): Int = {
    avgValue.toInt
  }
}

// SmallInt average aggregate return Int as aggregated value.
class SmallIntAvgAggregate extends AvgAggregate[Int] {
  private var avgValue: Double = 0
  private var count: Int = 0

  override def initiateAggregate: Unit = {
    avgValue = 0
    count = 0
  }

  override def aggregate(value: Any): Unit = {
    count += 1
    val current = value.asInstanceOf[Short]
    avgValue += (current - avgValue) / count
  }

  override def getAggregated(): Int = {
    avgValue.toInt
  }
}

// Int average aggregate return Int as aggregated value.
class IntAvgAggregate extends AvgAggregate[Int] {
  private var avgValue: Double = 0
  private var count: Int = 0

  override def initiateAggregate: Unit = {
    avgValue = 0
    count = 0
  }

  override def aggregate(value: Any): Unit = {
    count += 1
    val current = value.asInstanceOf[Int]
    avgValue += (current - avgValue) / count
  }

  override def getAggregated(): Int = {
    avgValue.toInt
  }
}

// Long average aggregate return Long as aggregated value.
class LongAvgAggregate extends AvgAggregate[Long] {
  private var avgValue: Double = 0
  private var count: Int = 0

  override def initiateAggregate: Unit = {
    avgValue = 0
    count = 0
  }

  override def aggregate(value: Any): Unit = {
    count += 1
    val current = value.asInstanceOf[Long]
    avgValue += (current - avgValue) / count
  }

  override def getAggregated(): Long = {
    avgValue.toLong
  }
}

// Float average aggregate return Float as aggregated value.
class FloatAvgAggregate extends AvgAggregate[Float] {
  private var avgValue: Double = 0
  private var count: Int = 0

  override def initiateAggregate: Unit = {
    avgValue = 0
    count = 0
  }

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
class DoubleAvgAggregate extends AvgAggregate[Double] {
  private var avgValue: Double = 0
  private var count: Int = 0

  override def initiateAggregate: Unit = {
    avgValue = 0
    count = 0
  }

  override def aggregate(value: Any): Unit = {
    count += 1
    val current = value.asInstanceOf[Double]
    avgValue += (current - avgValue) / count
  }

  override def getAggregated(): Double = {
    avgValue
  }
}
