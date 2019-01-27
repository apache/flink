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
package org.apache.flink.table.runtime.sort

import org.apache.flink.runtime.state.{VoidNamespace, VoidNamespaceSerializer}
import org.apache.flink.streaming.api.operators._
import org.apache.flink.streaming.api.{SimpleTimerService, TimerService}
import org.apache.flink.table.codegen.{CodeGenUtils, GeneratedSorter}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.util.Logging

abstract class SortBaseOperator
  extends AbstractStreamOperator[BaseRow]
  with OneInputStreamOperator[BaseRow, BaseRow]
  with Triggerable[BaseRow, VoidNamespace]
  with Logging {

  @transient protected var timerService: TimerService = _
  @transient protected var collector: TimestampedCollector[BaseRow] = _

  override def open() {
    val internalTimerService = getInternalTimerService(
      "user-timers", VoidNamespaceSerializer.INSTANCE,
      this)

    collector = new TimestampedCollector[BaseRow](output)
    timerService = new SimpleTimerService(internalTimerService)
  }

  protected def getComparator(gSorter: GeneratedSorter): RecordComparator = {
    val name = gSorter.comparator.name
    val code = gSorter.comparator.code
    LOG.debug(s"Compiling Comparator: $name \n\n Code:\n$code")
    val clazz = CodeGenUtils.compile(
      getRuntimeContext.getUserCodeClassLoader, name, code
    ).asInstanceOf[Class[RecordComparator]]
    val comparator = clazz.newInstance()
    comparator.init(gSorter.serializers, gSorter.comparators)
    comparator
  }

  protected def getComputer(gSorter: GeneratedSorter): NormalizedKeyComputer = {
    val name = gSorter.computer.name
    val code = gSorter.computer.code
    LOG.debug(s"Compiling Computer: $name \n\n Code:\n$code")
    val clazz = CodeGenUtils.compile(
      getRuntimeContext.getUserCodeClassLoader, name, code
    ).asInstanceOf[Class[NormalizedKeyComputer]]
    val computor = clazz.newInstance()
    computor.init(gSorter.serializers, gSorter.comparators)
    computor
  }

  /**
    * Invoked when an event-time timer fires.
    */
  override def onEventTime(timer: InternalTimer[BaseRow, VoidNamespace]): Unit = {
    throw new UnsupportedOperationException(
      "Now Sort only is supported based processing time here!"
    )
  }

  /**
    * Invoked when a processing-time timer fires.
    */
  override def onProcessingTime(timer: InternalTimer[BaseRow, VoidNamespace]): Unit = {
    throw new UnsupportedOperationException(
      "Now Sort only is supported based event time here!"
    )
  }
}
