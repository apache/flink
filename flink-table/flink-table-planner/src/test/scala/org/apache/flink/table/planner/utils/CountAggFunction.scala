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
package org.apache.flink.table.planner.utils

import org.apache.flink.api.java.tuple.{Tuple1 => JTuple1}
import org.apache.flink.table.annotation.DataTypeHint
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.catalog.DataTypeFactory
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.table.types.inference.{InputTypeStrategies, TypeInference, TypeStrategies}

import java.lang.{Iterable => JIterable, Long => JLong}

/** The initial accumulator for count aggregate function */
class CountAccumulator extends JTuple1[JLong] {
  f0 = 0L // count
}

/** built-in count aggregate function */
class CountAggFunction extends AggregateFunction[JLong, CountAccumulator] {

  def accumulate(acc: CountAccumulator, @DataTypeHint("INT") value: Any): Unit = {
    if (value != null) acc.f0 += 1L
  }

  def accumulate(acc: CountAccumulator): Unit = {
    acc.f0 += 1L
  }

  def retract(acc: CountAccumulator, @DataTypeHint("INT") value: Any): Unit = {
    if (value != null) {
      acc.f0 -= 1L
    }
  }

  def retract(acc: CountAccumulator): Unit = {
    acc.f0 -= 1L
  }

  override def getValue(acc: CountAccumulator): JLong = {
    acc.f0
  }

  def merge(acc: CountAccumulator, its: JIterable[CountAccumulator]): Unit = {
    val iter = its.iterator()
    while (iter.hasNext) {
      acc.f0 += iter.next().f0
    }
  }

  override def createAccumulator(): CountAccumulator = {
    new CountAccumulator
  }

  override def getTypeInference(typeFactory: DataTypeFactory): TypeInference = {
    TypeInference.newBuilder
      .inputTypeStrategy(InputTypeStrategies.sequence(InputTypeStrategies.or(
        InputTypeStrategies.explicit(DataTypes.INT),
        InputTypeStrategies.explicit(DataTypes.BIGINT()),
        InputTypeStrategies.explicit(DataTypes.STRING()),
        InputTypeStrategies.explicit(DataTypes.TIMESTAMP(3))
      )))
      .accumulatorTypeStrategy(TypeStrategies.explicit(
        DataTypes.STRUCTURED(classOf[CountAccumulator], DataTypes.FIELD("f0", DataTypes.BIGINT()))))
      .outputTypeStrategy(TypeStrategies.explicit(DataTypes.BIGINT()))
      .build
  }
}
