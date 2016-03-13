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

package org.apache.flink.streaming.api.scala

import org.apache.flink.annotation.PublicEvolving
import org.apache.flink.streaming.api.datastream.{DataStreamSink => JavaStreamSink}

class DataStreamSink[T](stream : JavaStreamSink[T]) {

  /**
    * Gets the underlying java DataStream object.
    */
  def javaStream: JavaStreamSink[T] = stream

  /**
    * Sets the name of the current data stream. This name is
    * used by the visualization and logging during runtime.
    *
    * @return The named operator
    */
  def name(name: String) : DataStreamSink[T] = {
    stream.name(name)
    this
  }

  /**
    * Sets an ID for this operator.
    *
    * The specified ID is used to assign the same operator ID across job
    * submissions (for example when starting a job from a savepoint).
    *
    * <strong>Important</strong>: this ID needs to be unique per
    * transformation and job. Otherwise, job submission will fail.
    *
    * @param uid The unique user-specified ID of this transformation.
    * @return The operator with the specified ID.
    */
  @PublicEvolving
  def uid(uid: String) : DataStreamSink[T] =  {
    stream.uid(uid)
    this
  }

  /**
    * Sets the parallelism of this operation. This must be at least 1.
    */
  def setParallelism(parallelism: Int): DataStreamSink[T] = {
    stream.setParallelism(parallelism)
    this
  }

  /**
    * Turns off chaining for this operator so thread co-location will not be
    * used as an optimization. </p> Chaining can be turned off for the whole
    * job by [[StreamExecutionEnvironment.disableOperatorChaining()]]
    * however it is not advised for performance considerations.
    *
    */
  @PublicEvolving
  def disableChaining(): DataStreamSink[T] = {
    stream.disableChaining()
    this
  }

  /**
    * Sets the slot sharing group of this operation. Parallel instances of
    * operations that are in the same slot sharing group will be co-located in the same
    * TaskManager slot, if possible.
    *
    * Operations inherit the slot sharing group of input operations if all input operations
    * are in the same slot sharing group and no slot sharing group was explicitly specified.
    *
    * Initially an operation is in the default slot sharing group. An operation can be put into
    * the default group explicitly by setting the slot sharing group to `"default"`.
    *
    * @param slotSharingGroup The slot sharing group name.
    */
  @PublicEvolving
  def slotSharingGroup(slotSharingGroup: String): DataStreamSink[T] = {
    stream.slotSharingGroup(slotSharingGroup)
    this
  }

}
