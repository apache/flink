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

abstract class MinAggregate[T] extends Aggregate[T]{

}

class TinyIntMinAggregate extends MinAggregate[Byte] {
  private var min = Byte.MaxValue

  override def initiateAggregate: Unit = {
    min = Byte.MaxValue
  }

  override def aggregate(value: Any): Unit = {
    val current = value.asInstanceOf[Byte]
    if (current < min) {
      min = current
    }
  }

  override def getAggregated(): Byte = {
    min
  }
}

class SmallIntMinAggregate extends MinAggregate[Short] {
  private var min = Short.MaxValue

  override def initiateAggregate: Unit = {
    min = Short.MaxValue
  }

  override def aggregate(value: Any): Unit = {
    val current = value.asInstanceOf[Short]
    if (current < min) {
      min = current
    }
  }

  override def getAggregated(): Short = {
    min
  }
}

class IntMinAggregate extends MinAggregate[Int] {
  private var min = Int.MaxValue

  override def initiateAggregate: Unit = {
    min = Int.MaxValue
  }

  override def aggregate(value: Any): Unit = {
    val current = value.asInstanceOf[Int]
    if (current < min) {
      min = current
    }
  }

  override def getAggregated(): Int = {
    min
  }
}

class LongMinAggregate extends MinAggregate[Long] {
  private var min = Long.MaxValue

  override def initiateAggregate: Unit = {
    min = Int.MaxValue
  }

  override def aggregate(value: Any): Unit = {
    val current = value.asInstanceOf[Long]
    if (current < min) {
      min = current
    }
  }

  override def getAggregated(): Long = {
    min
  }
}

class FloatMinAggregate extends MinAggregate[Float] {
  private var min = Float.MaxValue

  override def initiateAggregate: Unit = {
    min = Int.MaxValue
  }

  override def aggregate(value: Any): Unit = {
    val current = value.asInstanceOf[Float]
    if (current < min) {
      min = current
    }
  }

  override def getAggregated(): Float = {
    min
  }
}

class DoubleMinAggregate extends MinAggregate[Double] {
  private var min = Double.MaxValue

  override def initiateAggregate: Unit = {
    min = Int.MaxValue
  }

  override def aggregate(value: Any): Unit = {
    val current = value.asInstanceOf[Double]
    if (current < min) {
      min = current
    }
  }

  override def getAggregated(): Double = {
    min
  }
}
