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

import org.apache.flink.table.descriptors.StreamTableDescriptorValidator.{UPDATE_MODE, UPDATE_MODE_VALUE_APPEND, UPDATE_MODE_VALUE_RETRACT, UPDATE_MODE_VALUE_UPSERT}

class TestTableDescriptor(connector: ConnectorDescriptor)
  extends TableDescriptor
  with SchematicDescriptor[TestTableDescriptor]
  with StreamableDescriptor[TestTableDescriptor] {

  private var formatDescriptor: Option[FormatDescriptor] = None
  private var schemaDescriptor: Option[Schema] = None
  private var updateMode: Option[String] = None

  override def toProperties: util.Map[String, String] = {
    val properties = new DescriptorProperties()

    properties.putProperties(connector.toProperties)
    formatDescriptor.foreach(d => properties.putProperties(d.toProperties))
    schemaDescriptor.foreach(d => properties.putProperties(d.toProperties))
    updateMode.foreach(mode => properties.putString(UPDATE_MODE, mode))

    properties.asMap()
  }

  override def withFormat(format: FormatDescriptor): TestTableDescriptor = {
    this.formatDescriptor = Some(format)
    this
  }

  override def withSchema(schema: Schema): TestTableDescriptor = {
    this.schemaDescriptor = Some(schema)
    this
  }

  override def inAppendMode(): TestTableDescriptor = {
    updateMode = Some(UPDATE_MODE_VALUE_APPEND)
    this
  }

  override def inRetractMode(): TestTableDescriptor = {
    updateMode = Some(UPDATE_MODE_VALUE_RETRACT)
    this
  }

  override def inUpsertMode(): TestTableDescriptor = {
    updateMode = Some(UPDATE_MODE_VALUE_UPSERT)
    this
  }
}
