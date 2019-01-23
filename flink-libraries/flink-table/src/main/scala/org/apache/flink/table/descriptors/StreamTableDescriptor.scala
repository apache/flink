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

package org.apache.flink.table.descriptors

import java.util

import org.apache.flink.table.api.StreamTableEnvironment
import org.apache.flink.table.descriptors.StreamTableDescriptorValidator._

/**
  * Descriptor for specifying a table source and/or sink in a streaming environment.
  */
class StreamTableDescriptor(
    tableEnv: StreamTableEnvironment,
    connectorDescriptor: ConnectorDescriptor)
  extends ConnectTableDescriptor[StreamTableDescriptor](
    tableEnv,
    connectorDescriptor)
  with StreamableDescriptor[StreamTableDescriptor] {

  private var updateMode: Option[String] = None

  /**
    * Declares how to perform the conversion between a dynamic table and an external connector.
    *
    * In append mode, a dynamic table and an external connector only exchange INSERT messages.
    *
    * @see See also [[inRetractMode()]] and [[inUpsertMode()]].
    */
  override def inAppendMode(): StreamTableDescriptor = {
    updateMode = Some(UPDATE_MODE_VALUE_APPEND)
    this
  }

  /**
    * Declares how to perform the conversion between a dynamic table and an external connector.
    *
    * In retract mode, a dynamic table and an external connector exchange ADD and RETRACT messages.
    *
    * An INSERT change is encoded as an ADD message, a DELETE change as a RETRACT message, and an
    * UPDATE change as a RETRACT message for the updated (previous) row and an ADD message for
    * the updating (new) row.
    *
    * In this mode, a key must not be defined as opposed to upsert mode. However, every update
    * consists of two messages which is less efficient.
    *
    * @see See also [[inAppendMode()]] and [[inUpsertMode()]].
    */
  override def inRetractMode(): StreamTableDescriptor = {
    updateMode = Some(UPDATE_MODE_VALUE_RETRACT)
    this
  }

  /**
    * Declares how to perform the conversion between a dynamic table and an external connector.
    *
    * In upsert mode, a dynamic table and an external connector exchange UPSERT and DELETE messages.
    *
    * This mode requires a (possibly composite) unique key by which updates can be propagated. The
    * external connector needs to be aware of the unique key attribute in order to apply messages
    * correctly. INSERT and UPDATE changes are encoded as UPSERT messages. DELETE changes as
    * DELETE messages.
    *
    * The main difference to a retract stream is that UPDATE changes are encoded with a single
    * message and are therefore more efficient.
    *
    * @see See also [[inAppendMode()]] and [[inRetractMode()]].
    */
  override def inUpsertMode(): StreamTableDescriptor = {
    updateMode = Some(UPDATE_MODE_VALUE_UPSERT)
    this
  }

  // ----------------------------------------------------------------------------------------------

  override def toProperties: util.Map[String, String] = {
    val properties = new DescriptorProperties()

    properties.putProperties(super.toProperties)
    updateMode.foreach(mode => properties.putString(UPDATE_MODE, mode))

    properties.asMap()
  }
}
