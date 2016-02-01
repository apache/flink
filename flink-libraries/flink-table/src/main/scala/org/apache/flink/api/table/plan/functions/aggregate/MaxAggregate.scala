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

abstract class MaxAggregate[T] extends Aggregate[T]{

}

class TinyIntMaxAggregate extends MaxAggregate[Byte] {
  private var max = Byte.MaxValue

  override def initiateAggregate: Unit = {
    max = Byte.MaxValue
  }

  override def aggregate(value: Any): Unit = {
    val current = value.asInstanceOf[Byte]
    if (current < max) {
      max = current
    }
  }

  override def getAggregated(): Byte = {
    max
  }
}

class SmallIntMaxAggregate extends MaxAggregate[Short] {
  private var max = Short.MaxValue

  override def initiateAggregate: Unit = {
    max = Short.MaxValue
  }

  override def aggregate(value: Any): Unit = {
    val current = value.asInstanceOf[Short]
    if (current < max) {
      max = current
    }
  }

  override def getAggregated(): Short = {
    max
  }
}

class IntMaxAggregate extends MaxAggregate[Int] {
  private var max = Int.MaxValue

  override def initiateAggregate: Unit = {
    max = Int.MaxValue
  }

  override def aggregate(value: Any): Unit = {
    val current = value.asInstanceOf[Int]
    if (current < max) {
      max = current
    }
  }

  override def getAggregated(): Int = {
    max
  }
}

class LongMaxAggregate extends MaxAggregate[Long] {
  private var max = Long.MaxValue

  override def initiateAggregate: Unit = {
    max = Int.MaxValue
  }

  override def aggregate(value: Any): Unit = {
    val current = value.asInstanceOf[Long]
    if (current < max) {
      max = current
    }
  }

  override def getAggregated(): Long = {
    max
  }
}

class FloatMaxAggregate extends MaxAggregate[Float] {
  private var max = Float.MaxValue

  override def initiateAggregate: Unit = {
    max = Int.MaxValue
  }

  override def aggregate(value: Any): Unit = {
    val current = value.asInstanceOf[Float]
    if (current < max) {
      max = current
    }
  }

  override def getAggregated(): Float = {
    max
  }
}

class DoubleMaxAggregate extends MaxAggregate[Double] {
  private var max = Double.MaxValue

  override def initiateAggregate: Unit = {
    max = Int.MaxValue
  }

  override def aggregate(value: Any): Unit = {
    val current = value.asInstanceOf[Double]
    if (current < max) {
      max = current
    }
  }

  override def getAggregated(): Double = {
    max
  }
}
