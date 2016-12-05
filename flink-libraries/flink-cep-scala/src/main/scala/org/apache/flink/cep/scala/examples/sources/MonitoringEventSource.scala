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

package org.apache.flink.cep.scala.examples.sources

import org.apache.flink.cep.scala.examples.events.{MonitoringEvent, PowerEvent, TemperatureEvent}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

import java.util.Random

class MonitoringEventSource(maxRackId: Int,
                            pause: Long,
                            temperatureRatio: Double,
                            powerStd: Double,
                            powerMean: Double,
                            temperatureStd: Double,
                            temperatureMean: Double)
  extends RichParallelSourceFunction[MonitoringEvent] {

  private var random: Random = _

  private var running = true
  private var shard: Int = 0
  private var offset: Int = 0

  override def open(configuration: Configuration) {
    val numberTasks: Int = getRuntimeContext.getNumberOfParallelSubtasks
    val index: Int = getRuntimeContext.getIndexOfThisSubtask

    offset = (maxRackId.toDouble / numberTasks * index).toInt
    shard = (maxRackId.toDouble / numberTasks * (index + 1)).toInt - offset

    random = new Random()
  }

  override def run(ctx: SourceContext[MonitoringEvent]) {
    while (running) {
      val rackId = random.nextInt(shard) + offset

      val monitoringEvent =
        if (random.nextDouble >= temperatureRatio) {
          val power = random.nextGaussian * powerStd + powerMean
          new PowerEvent(rackId, power)
        } else {
          val temperature = random.nextGaussian * temperatureStd + temperatureMean
          new TemperatureEvent(rackId, temperature)
        }

      ctx.collect(monitoringEvent)
      Thread.sleep(pause)
    }
  }

  override def cancel() {
    running = false
  }
}
