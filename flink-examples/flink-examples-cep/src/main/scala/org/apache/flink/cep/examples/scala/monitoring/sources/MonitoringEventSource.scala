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

package org.apache.flink.cep.examples.scala.monitoring.sources

import java.util.Random

import org.apache.flink.cep.examples.scala.monitoring.events.{MonitoringEvent, PowerEvent, TemperatureEvent}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

/**
  * Event source that randomly produces [[TemperatureEvent]] and [[PowerEvent]].
  * The ratio of temperature events is configured by [[tempEventRatio]].
  * The [[TemperatureEvent#temperature]] is a Gaussian distributed random number
  * with mean [[TEMP_MEAN]] and standard deviation [[TEMP_STD]].
  * [[PowerEvent#voltage]] is generated in a similar way.
  */
class MonitoringEventSource(val numRacks: Int = 10, val tempEventRatio: Double = 0.7)
    extends RichParallelSourceFunction[MonitoringEvent] {

  private val TEMP_STD = 20
  private val TEMP_MEAN = 80
  private val POWER_STD = 10
  private val POWER_MEAN = 100

  private var random: Random = _

  private var running = true

  // the number of racks for which to emit monitoring events from this sub source task
  private var shards = 0
  // rack id of the first shard
  private var offset = 0

  override def open(configuration: Configuration) {
    val numberTasks = getRuntimeContext.getNumberOfParallelSubtasks
    val index = getRuntimeContext.getIndexOfThisSubtask

    offset = (numRacks.toDouble / numberTasks * index).toInt
    shards = (numRacks.toDouble / numberTasks * (index + 1)).toInt - offset

    random = new Random()
  }

  override def run(ctx: SourceContext[MonitoringEvent]) {
    while (running) {
      val rackId = offset + random.nextInt(shards)

      val monitoringEvent =
        if (random.nextDouble >= tempEventRatio) {
          val power = random.nextGaussian * POWER_STD + POWER_MEAN
          new PowerEvent(rackId, power)
        } else {
          val temperature = random.nextGaussian * TEMP_STD + TEMP_MEAN
          new TemperatureEvent(rackId, temperature)
        }

      ctx.collect(monitoringEvent)

      Thread.sleep(100)
    }
  }

  override def cancel() {
    running = false
  }

}
