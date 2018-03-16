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

import org.apache.flink.table.descriptors.ConnectorDescriptorValidator.{CONNECTOR_TYPE, CONNECTOR_PROPERTY_VERSION}

/**
  * Describes a connector to an other system.
  *
  * @param tpe string identifier for the connector
  */
abstract class ConnectorDescriptor(
    private val tpe: String,
    private val version: Int,
    private val formatNeeded: Boolean)
  extends Descriptor {

  override def toString: String = this.getClass.getSimpleName

  /**
    * Internal method for properties conversion.
    */
  final private[flink] def addProperties(properties: DescriptorProperties): Unit = {
    properties.putString(CONNECTOR_TYPE, tpe)
    properties.putLong(CONNECTOR_PROPERTY_VERSION, version)
    addConnectorProperties(properties)
  }

  /**
    * Internal method for connector properties conversion.
    */
  protected def addConnectorProperties(properties: DescriptorProperties): Unit

  /**
    * Internal method that defines if this connector requires a format descriptor.
    */
  private[flink] def needsFormat(): Boolean = formatNeeded

}
