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

/**
  * Common class for all descriptors describing table sources and sinks.
  */
abstract class TableDescriptor extends Descriptor {

  protected var connectorDescriptor: Option[ConnectorDescriptor] = None
  protected var formatDescriptor: Option[FormatDescriptor] = None
  protected var schemaDescriptor: Option[Schema] = None
  protected var metaDescriptor: Option[Metadata] = None

  /**
    * Internal method for properties conversion.
    */
  override private[flink] def addProperties(properties: DescriptorProperties): Unit = {
    connectorDescriptor.foreach(_.addProperties(properties))
    formatDescriptor.foreach(_.addProperties(properties))
    schemaDescriptor.foreach(_.addProperties(properties))
    metaDescriptor.foreach(_.addProperties(properties))
  }
}
